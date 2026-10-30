// Package bench 提供可用公开 API 复现的微基准，与 validation/microbench.txt
// 中的根包基准对应（根包基准额外包含内部保护索引基线 BenchmarkIndexHitProtected，
// 需要访问内部结构，只能通过 `go test -bench . 在仓库根` 运行）。
//
// 运行：
//
//	go test -run '^$' -bench . -benchmem ./bench
//	go test -run '^$' -bench BenchmarkGetHit -benchmem -benchtime=1s -count=2 ./bench
//
// 口径说明：ns/op 是平均值，不是单请求 P50/P99；并行基准的 ns/op 是吞吐倒数。
// 与 validation 相同：请求热点追踪默认关闭，微基准不含追踪成本。
package bench

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"ahkc"
)

// newBenchCache 构造基准环境并等待工作集全部驻留（真实时钟、默认执行器、
// 周期维护后台发布；预热轮询整个工作集，避免发布竞争造成假 miss）。
func newBenchCache(b *testing.B, entries int64, keys int) (*ahkc.Cache[string], *ahkc.Namespace[string]) {
	b.Helper()
	c, err := ahkc.NewBytes[string](ahkc.Config{
		Shards:     64,
		MaxEntries: entries,
		MaxBytes:   1 << 30,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"bench": {TTL: time.Hour, Tin: 1},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	ns, err := c.Namespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	loader := func(ctx context.Context, key string) (string, error) { return "v", nil }
	workingSet := make([]string, keys)
	for i := range workingSet {
		workingSet[i] = fmt.Sprintf("key%d", i)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		ready := true
		for _, key := range workingSet {
			_, hit, err := ns.Get(key)
			if err != nil && !errors.Is(err, ahkc.ErrBusy) {
				b.Fatal(err)
			}
			if err == nil && hit {
				continue
			}
			ready = false
			if _, err := ns.Fetch(context.Background(), key, loader); err != nil &&
				!errors.Is(err, ahkc.ErrBusy) && !errors.Is(err, ahkc.ErrOverloaded) {
				b.Fatal(err)
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("warmup did not publish working set: %+v", c.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Close(ctx)
	})
	return c, ns
}

// BenchmarkGetHit 单线程普通值读取命中（对应 microbench：约 105–109 ns/op，0 alloc）。
func BenchmarkGetHit(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := ns.Get("key255"); err != nil || !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkBorrowHit 单线程借用命中：栈上复用 Request 与 Borrowed，0 堆分配
// （对应 microbench：约 111–120 ns/op，0 alloc）。
func BenchmarkBorrowHit(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	var request ahkc.Request[string]
	var borrowed ahkc.Borrowed[string]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.BeginRequestOn(&request, "key255"); err != nil {
			b.Fatal(err)
		}
		if ok, err := ns.BorrowOn(&request, &borrowed); err != nil || !ok {
			b.Fatal("miss", err)
		}
		runtime.KeepAlive(borrowed.Value())
		borrowed.Release()
	}
}

// BenchmarkGetHitParallel 16 线程并行命中（对应 microbench：约 16–18 ns/op，吞吐倒数）。
func BenchmarkGetHitParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(ctr.Add(1)) % len(keys)
		for pb.Next() {
			if _, ok, err := ns.Get(keys[i]); err != nil || !ok {
				b.Fatal("miss")
			}
			i = (i + 1) % len(keys)
		}
	})
}

// BenchmarkGetMissParallel 16 线程并行确定 miss（对应 microbench：约 20 ns/op）。
func BenchmarkGetMissParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 0)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("absent%d", i)
	}
	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(ctr.Add(1)) % len(keys)
		for pb.Next() {
			if _, ok, err := ns.Get(keys[i]); err != nil || ok {
				b.Fatal("unexpected hit")
			}
			i = (i + 1) % len(keys)
		}
	})
}

// BenchmarkGetOwnedHotKeyParallel 同一热 key 上并发拥有型命中：
// MaxOwnedPinsPerEntry 写入不应使只读字段所在缓存行失效。
func BenchmarkGetOwnedHotKeyParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h, ok, err := ns.GetOwned("key7")
			if err != nil || !ok {
				b.Fatal("miss")
			}
			runtime.KeepAlive(h.Value())
			h.Release()
		}
	})
}

// BenchmarkFetchMissLoad 完整 miss → singleflight → 加载 → 交付路径
// （Tin 极高且无先验，填充不可估，因此每次都是真实回源）。
func BenchmarkFetchMissLoad(b *testing.B) {
	c, err := ahkc.NewBytes[string](ahkc.Config{
		Shards:     64,
		MaxEntries: 16384,
		MaxBytes:   1 << 30,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"bench": {TTL: time.Hour, Tin: 1000},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Close(ctx)
	}()
	ns, _ := c.Namespace("bench")
	var ctr atomic.Uint64
	loader := func(ctx context.Context, key string) (string, error) { return "v", nil }
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := ctr.Add(1)
			for {
				_, err := ns.Fetch(context.Background(), fmt.Sprintf("load%d", n), loader)
				if err == nil {
					break
				}
				if !errors.Is(err, ahkc.ErrBusy) && !errors.Is(err, ahkc.ErrOverloaded) {
					b.Fatal(err)
				}
				runtime.Gosched()
			}
		}
	})
}
