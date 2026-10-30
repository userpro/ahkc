// Command async-load 展示 singleflight 与 Future 的 Take / Cancel / Wait。
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"ahkc"
)

func main() {
	ctx := context.Background()
	c, err := ahkc.NewBytes[string](ahkc.Config{
		MaxEntries: 64,
		MaxBytes:   4 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"doc": {TTL: time.Minute, Tin: 1},
		},
	})
	must(err)
	defer c.Close(ctx)
	ns, err := c.Namespace("doc")
	must(err)

	var realLoads atomic.Int64
	release := make(chan struct{})
	loader := func(context.Context, string) (string, error) {
		realLoads.Add(1)
		<-release // 等所有 Future 注册后再完成加载，不依赖 sleep
		return "body", nil
	}
	futures := make([]*ahkc.Future[string], 8)
	for i := range futures {
		futures[i], err = ns.FetchAsync("doc/a", loader)
		must(err)
	}
	if _, ready, _ := futures[0].Take(); ready {
		panic("source is still blocked")
	}
	futures[0].Cancel() // 只取消自己的等待，不影响其他请求和 loader
	abort, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := futures[1].Wait(abort); !errors.Is(err, context.Canceled) {
		panic("Wait must respect caller cancellation")
	}
	close(release)
	for _, f := range futures[2:] {
		v, err := f.Wait(ctx)
		must(err)
		if v != "body" {
			panic("unexpected value")
		}
	}
	if realLoads.Load() != 1 {
		panic("singleflight failed")
	}
	fmt.Printf("8 futures -> %d real load; 2 cancelled, 6 delivered\n", realLoads.Load())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
