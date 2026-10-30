package ahkc

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// 复用确定性测试环境，基准只计 SDK 及手动执行器的工作，不包含真实 I/O 延迟。
func newFeatureEnv(b *testing.B, tweak func(*Config)) *testEnv {
	b.Helper()
	e := newTestEnv(b, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) {
		c.Shards = 1
		if tweak != nil {
			tweak(c)
		}
	})
	b.Cleanup(func() { closeManual(b, e.c, e.ex) })
	return e
}

func BenchmarkFeatureReads(b *testing.B) {
	for _, mode := range []string{"borrowed_hit", "borrowed_miss", "stats", "heat"} {
		b.Run(mode, func(b *testing.B) {
			e := newFeatureEnv(b, nil)
			if _, err := e.fetch("key", okLoader("v")); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "borrowed_hit":
					h, ok, err := e.ns.GetBorrowed("key")
					if err != nil || !ok {
						b.Fatal(err)
					}
					h.Release()
				case "borrowed_miss":
					if _, ok, err := e.ns.GetBorrowed("absent"); err != nil || ok {
						b.Fatal(err)
					}
				case "stats":
					_ = e.c.Stats()
				case "heat":
					if _, ok := e.ns.Heat("key"); !ok {
						b.Fatal("unknown heat")
					}
				}
			}
		})
	}
}

func BenchmarkFeatureLoad(b *testing.B) {
	for _, mode := range []string{"async", "merge8", "cancel"} {
		b.Run(mode, func(b *testing.B) {
			e := newFeatureEnv(b, func(c *Config) { c.Mode = ModeObserve })
			loader := okLoader("v")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, err := e.ns.FetchAsync("key", loader)
				if err != nil {
					b.Fatal(err)
				}
				var joined [7]*Future[string]
				if mode == "merge8" {
					for j := range joined {
						joined[j], err = e.ns.FetchAsync("key", loader)
						if err != nil {
							b.Fatal(err)
						}
					}
				}
				if mode == "cancel" {
					f.Cancel()
				}
				e.ex.Drain(1000)
				if mode != "cancel" {
					if _, err := f.Wait(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
				for _, f := range joined {
					if f != nil {
						if _, err := f.Wait(context.Background()); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}

func BenchmarkFeatureControl(b *testing.B) {
	for _, mode := range []string{"key", "namespace", "idle_maintenance"} {
		b.Run(mode, func(b *testing.B) {
			e := newFeatureEnv(b, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "idle_maintenance" {
					e.c.MaintenanceStep()
					e.ex.Drain(1000)
					continue
				}
				var h *ControlHandle
				var err error
				if mode == "key" {
					h, err = e.ns.InvalidateAsync("key")
				} else {
					h, err = e.c.InvalidateNamespaceAsync("t")
				}
				if err != nil {
					b.Fatal(err)
				}
				e.ex.Drain(1000)
				if err := h.Wait(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFeatureHotExport(b *testing.B) {
	for _, mode := range []string{"current20", "history", "cursor_caught_up", "label_history"} {
		b.Run(mode, func(b *testing.B) {
			e := newFeatureEnv(b, func(c *Config) {
				c.Mode, c.Hot.LoadSlots, c.Hot.RequestSlots = ModeObserve, 64, 64
				c.Hot.Windows, c.Hot.TopN, c.Hot.RequestSampleP = 4, 16, 1
				if mode == "label_history" {
					c.Hot.Label = func(_, key string) (string, bool) { return key, true }
				}
			})
			for i := 0; i < 64; i++ {
				if _, err := e.fetch(fmt.Sprintf("key%d", i), okLoader("v")); err != nil {
					b.Fatal(err)
				}
			}
			e.clk.Advance(time.Second)
			e.drive()
			history := e.c.HotKeyHistory(1)
			if len(history) != 1 {
				b.Fatal("missing window")
			}
			seq := history[0].Seq
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "current20":
					_ = e.c.HotKeys(20)
				case "history", "label_history":
					_ = e.c.HotKeyHistory(1)
				case "cursor_caught_up":
					_, _ = e.c.HotWindowsSince(seq)
				}
			}
		})
	}
}

func BenchmarkFeatureSyncMiss(b *testing.B) {
	c, err := NewBytes[string](Config{
		Shards:     1,
		MaxEntries: 64,
		MaxBytes:   1 << 20,
		Mode:       ModeObserve,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			b.Fatal(err)
		}
	})
	ns, err := c.Namespace("t")
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	loader := okLoader("v")
	retries := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r Request[string]
		if err := ns.BeginRequestOn(&r, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
		for attempt := 0; ; attempt++ {
			v, err := ns.FetchOn(context.Background(), &r, loader)
			if err == nil {
				if v != "v" {
					b.Fatal("unexpected value")
				}
				break
			}
			if err != ErrBusy || attempt == 1_000_000 {
				b.Fatal(err)
			}
			retries++
			runtime.Gosched()
		}
	}
	b.ReportMetric(float64(retries)/float64(b.N), "busy/op")
}

func BenchmarkFeaturePrimitives(b *testing.B) {
	for _, mode := range []string{"heat_update", "heat_saturated", "gate", "epoch"} {
		b.Run(mode, func(b *testing.B) {
			var heat, saturated heatCell
			for i := 0; i < 4; i++ {
				saturated.add(0, 4, 2)
			}
			gate := newHeatGate(make([]heatCell, 1<<14))
			epoch := newEpochDomain(128)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "heat_update":
					if heat.add(uint64(i/16), 64, 2) != heatUpdated {
						b.Fatal("heat update skipped")
					}
				case "heat_saturated":
					if saturated.add(0, 4, 2) != heatSaturated {
						b.Fatal("heat not saturated")
					}
				case "gate":
					gate.add(uint64(i), uint64(i/4096))
					if _, ok := gate.read(uint64(i), uint64(i/4096), 0.5); !ok {
						b.Fatal("unknown heat")
					}
				case "epoch":
					g, ok := epoch.pin()
					if !ok {
						b.Fatal("no reader slot")
					}
					g.unpin()
				}
			}
		})
	}
}
