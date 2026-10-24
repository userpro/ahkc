// Command advanced 展示引用值计费、结果元数据、动态命名空间与模型、手动维护及分阶段关闭。
package main

import (
	"context"
	"fmt"
	"time"

	"ahkc"
)

type document struct{ Body []byte }

func main() {
	ctx := context.Background()
	clock, executor := ahkc.NewManualClock(int64(time.Second)), ahkc.NewManualExecutor()
	c, err := ahkc.New[document](ahkc.Config{
		ShardCount: 1, MaxEntries: 64, MaxResidentBytes: 4 << 20,
		Clock: clock, Executor: executor, DisableTicker: true,
	}, ahkc.WithWeigher(func(key string, v document) int64 {
		return ahkc.EntryOverhead[document]() + int64(len(key)+cap(v.Body))
	}), ahkc.WithResultMetadata(func(key string, _ document) ahkc.ResultMetadata {
		return ahkc.ResultMetadata{
			NoCache:      key == "private",
			HardDeadline: clock.Now() + int64(time.Second), // 只能缩短命名空间 TTL
		}
	}))
	must(err)
	ns, err := c.RegisterNamespace("docs", ahkc.NamespaceConfig{TTL: time.Minute, Tin: 1})
	must(err)
	must(ns.SetPrior(&ahkc.Prior{Lambda: 10}))
	// MergeFactor=1 仅代表本例无并发合并的已知负载；生产应实测校准。
	model, err := ahkc.NewCostTableModel(ahkc.CostTableConfig{
		Clock: clock, Classes: 1, MergeFactor: 1,
		DisableAutoObserve: true, FallbackCost: 0.01,
	})
	must(err)
	model.ObserveLoad(0, 0.01, 10*time.Millisecond)
	must(ns.SetModel(model))
	drive := func() {
		for i := 0; i < 32; i++ {
			c.MaintenanceStep()
			executor.Drain(1000)
		}
	}
	loader := func(context.Context, string) (document, error) { return document{[]byte("body")}, nil }
	for _, key := range []string{"public", "private"} {
		var r ahkc.Request[document]
		must(ns.BeginRequestOn(&r, key))
		_, hit, err := ns.GetOn(&r)
		must(err)
		if hit {
			panic("unexpected hit before load")
		}
		f, err := ns.FetchAsyncOn(&r, loader) // 同一个 Request，不重复计数
		must(err)
		drive()
		v, ready, err := f.Take()
		must(err)
		if !ready || string(v.Body) != "body" {
			panic("result not delivered")
		}
		_, cached, err := ns.Get(key)
		must(err)
		if cached != (key == "public") {
			panic("metadata cache policy failed")
		}
		fmt.Printf("%s cached=%v\n", key, cached)
	}
	clock.Advance(time.Second)
	if _, hit, err := ns.Get("public"); err != nil || hit {
		panic("hard deadline was extended")
	}

	keyControl, err := ns.InvalidateAsync("public")
	must(err)
	drive()
	must(keyControl.Wait(ctx))
	nsControl, err := c.InvalidateNamespaceAsync("docs")
	must(err)
	drive()
	must(nsControl.Wait(ctx))
	credits, ok := c.Credits()
	if !ok || credits.CountGranted+credits.CountFree+credits.CountInTransit != credits.CountLimit {
		panic("capacity ledger is inconsistent")
	}
	fmt.Printf("resident=%d managed_bound=%d\n", c.Stats().ResidentEntries, c.MemoryLimits().TotalBytes)

	c.Shutdown()
	for i := 0; i < 32 && !c.CloseProgress().Reclaimed; i++ {
		drive()
	}
	if !c.CloseProgress().Reclaimed {
		panic("cleanup did not finish")
	}
	must(c.WaitDrained(ctx))
	must(c.WaitReclaimed(ctx))
	must(c.Close(ctx))
	fmt.Printf("close: %+v\n", c.CloseProgress())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
