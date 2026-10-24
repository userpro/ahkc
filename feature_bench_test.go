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
	e := newTestEnv(b, NamespaceConfig{TTL: time.Hour, Tin: 1, Prior: testPrior}, func(c *Config) {
		c.ShardCount, c.SampleP = 1, 1e-12
		if tweak != nil {
			tweak(c)
		}
	})
	b.Cleanup(func() { closeManual(b, e.c, e.ex) })
	return e
}

func BenchmarkFeatureReads(b *testing.B) {
	for _, mode := range []string{"borrowed_hit", "borrowed_miss", "stats", "heat", "credits"} {
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
				case "credits":
					if _, ok := e.c.Credits(); !ok {
						b.Fatal("busy credits")
					}
				}
			}
		})
	}
}

func BenchmarkFeatureLoad(b *testing.B) {
	for _, mode := range []string{"async", "merge8", "cancel"} {
		b.Run(mode, func(b *testing.B) {
			e := newFeatureEnv(b, func(c *Config) { c.ObserveOnly, c.ObserveHotKeys = true, -1 })
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
				c.ObserveOnly, c.ObserveHotKeys, c.ObserveRequestKeys = true, 64, 64
				c.HotHistoryWindows, c.HotHistoryTopN, c.ObserveRequestSampleP = 4, 16, 1
				if mode == "label_history" {
					c.HotKeyLabel = func(_, key string) (string, bool) { return key, true }
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

func BenchmarkFeaturePolicy(b *testing.B) {
	for _, mode := range []string{"lookup", "observe", "snapshot", "solve8"} {
		b.Run(mode, func(b *testing.B) {
			m := newCostTable(b, CostTableConfig{Clock: NewManualClock(1), Classes: 4, MergeFactor: 1, FallbackCost: 1})
			var sums [256]subsetAgg
			s := baseSnapshot([]victimInfo{{id: 1, billed: 2, loss: -1}, {id: 2, billed: 3, loss: 1}, {id: 3, billed: 4, loss: 2}, {id: 4, billed: 5, loss: 3}, {id: 5, billed: 6, loss: 4}, {id: 6, billed: 7, loss: 5}, {id: 7, billed: 8, loss: 6}, {id: 8, billed: 9, loss: 7}})
			s.sums = &sums
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "lookup":
					if _, _, _, _, _, ok := m.Lookup(10, 0); !ok {
						b.Fatal("unavailable model")
					}
				case "observe":
					m.ObserveLoad(uint32(i%4), 0.01, time.Millisecond)
				case "snapshot":
					if m.SnapshotAt(1) == nil {
						b.Fatal("unavailable snapshot")
					}
				case "solve8":
					s.checks = 0
					if s.exactSelect() < 0 {
						b.Fatal("infeasible")
					}
				}
			}
		})
	}
}

func BenchmarkFeatureSyncMiss(b *testing.B) {
	c, err := New[string](Config{ShardCount: 1, MaxEntries: 64, MaxResidentBytes: 1 << 20, ObserveOnly: true, SampleP: 1e-12, Advanced: Advanced{ObserveHotKeys: -1}, Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute}}})
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

func BenchmarkFeatureFillFreeze(b *testing.B) {
	e := newFeatureEnv(b, func(c *Config) {
		model := newCostTable(b, CostTableConfig{Clock: c.Clock, MergeFactor: 1, FallbackCost: 1, DisableAutoObserve: true})
		c.Namespaces = map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 1, Prior: testPrior, Model: model}}
	})
	s := e.c.c.shards[0]
	s.fillTokens = 0 // 保留候选，只测冻结和估值，不把后台发布混入测量。
	f, err := e.ns.FetchAsync("key", okLoader("v"))
	if err != nil {
		b.Fatal(err)
	}
	e.ex.Drain(1000)
	if _, ready, err := f.Take(); !ready || err != nil {
		b.Fatal("source not ready", err)
	}
	if len(s.fillQueue) != 1 {
		b.Fatal("missing candidate")
	}
	a := <-s.fillQueue
	b.Cleanup(func() { e.c.c.rejectCandidate(a, fillRejectedNoCache); e.ex.Drain(1000) })
	if snap, _, _, reason := e.c.c.freezeInput(s, a, nil, nil); snap == nil {
		b.Fatal(reason)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if snap, _, _, reason := e.c.c.freezeInput(s, a, nil, nil); snap == nil {
			b.Fatal(reason)
		}
	}
}

func BenchmarkFeaturePrimitives(b *testing.B) {
	for _, mode := range []string{"heat_update", "heat_saturated", "gate", "sketch", "sample_queue", "epoch", "wheel", "retire_pool"} {
		b.Run(mode, func(b *testing.B) {
			var heat, saturated heatCell
			for i := 0; i < 4; i++ {
				saturated.add(0, 4, 2)
			}
			gate, stats := newHeatGate(4, 4096), newValueStats(4, 512, 0)
			queue, epoch := newSampleStripe(32), newEpochDomain(128)
			for i := 0; i < 16; i++ {
				queue.tryPush(sample{eventTime: int64(16 - i)})
			}
			wheel, pool := newTimerWheel[string](1, 4, 64, 0), newRetiredPool[string](16)
			e := &entry[string]{hardDeadline: 100}
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
					gate.add(uint64(i), uint64(i/4096), 64, 2)
					if _, ok := gate.read(uint64(i), uint64(i/4096), 0.5); !ok {
						b.Fatal("unknown heat")
					}
				case "sketch":
					if !stats.addSample(sample{policyHash: uint64(i), eventTime: 1, invP: 1024}, 1, 1e9) {
						b.Fatal("sample rejected")
					}
					if _, n := stats.query(uint64(i), 1, 1e9); n < 1 {
						b.Fatal("sample lost")
					}
				case "sample_queue":
					if !queue.tryPush(sample{eventTime: int64(i%17 + 1)}) {
						b.Fatal("queue full")
					}
					if _, ok := queue.pop(); !ok {
						b.Fatal("queue empty")
					}
				case "epoch":
					g, ok := epoch.pin()
					if !ok {
						b.Fatal("no reader slot")
					}
					g.unpin()
				case "wheel":
					wheel.add(e)
					wheel.remove(e)
				case "retire_pool":
					r, ok := pool.tryGet()
					if !ok {
						b.Fatal("retirement pool empty")
					}
					pool.put(r)
				}
			}
		})
	}
}
