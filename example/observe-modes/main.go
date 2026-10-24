// Command observe-modes 对同一份请求比较仅缓存、仅统计与组合模式。
// 手动时钟和执行器消除等待窗口与后台填充的 sleep。
package main

import (
	"context"
	"fmt"
	"time"

	"ahkc"
)

func main() {
	for _, mode := range []struct {
		name  string
		only  bool
		track bool
	}{
		{"仅缓存", false, false},
		{"仅统计", true, true},
		{"组合", false, true},
	} {
		clock := ahkc.NewManualClock(int64(time.Second))
		executor := ahkc.NewManualExecutor()
		notifications := 0
		cfg := ahkc.Config{
			ShardCount: 1, MaxEntries: 64, MaxResidentBytes: 4 << 20,
			ObserveOnly: mode.only, Clock: clock, Executor: executor, DisableTicker: true,
			Namespaces: map[string]ahkc.NamespaceConfig{
				"demo": {TTL: time.Minute, Tin: 1, Prior: &ahkc.Prior{Lambda: 10}},
			},
		}
		if mode.track {
			cfg.ObserveHotKeys, cfg.ObserveRequestKeys = 8, 8
			cfg.ObserveRequestSampleP, cfg.HotHistoryWindows = 1, 2
			cfg.OnHotWindow = func(ahkc.HotSnapshot) { notifications++ }
		}
		c, err := ahkc.New[string](cfg)
		must(err)
		ns, err := c.Namespace("demo")
		must(err)
		drive := func() {
			for i := 0; i < 32; i++ {
				c.MaintenanceStep()
				executor.Drain(1000)
			}
		}
		loader := func(context.Context, string) (string, error) { return "value", nil }
		for i := 0; i < 3; i++ {
			f, err := ns.FetchAsync("hot", loader)
			must(err)
			drive()
			v, err := f.Wait(context.Background())
			must(err)
			if v != "value" {
				panic("unexpected value")
			}
		}
		for i := 0; i < 2; i++ {
			_, hit, err := ns.Get("hot")
			must(err)
			if hit == mode.only {
				panic("unexpected cache behavior")
			}
		}
		clock.Advance(time.Second)
		drive()
		st, history := c.Stats(), c.HotKeyHistory(1)
		requests := float64(0)
		if len(history) > 0 {
			requests = history[0].Totals.Requests
		}
		fmt.Printf("%s: hit=%d miss=%d loads=%d resident=%d requests=%.0f windows=%d notify=%d\n",
			mode.name, st.Hits, st.Misses, st.Loads, st.ResidentEntries, requests, len(history), notifications)
		if mode.track && requests != 5 {
			panic("request tracking changed the workload")
		}
		c.Shutdown()
		for i := 0; i < 32 && !c.CloseProgress().Reclaimed; i++ {
			drive()
		}
		if !c.CloseProgress().Reclaimed {
			panic("cleanup did not finish")
		}
		must(c.WaitReclaimed(context.Background()))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
