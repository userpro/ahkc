package ahkc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ahkc"
)

// fetchRetry / fetchAsyncRetry 演示契约要求的暂时性错误处理：
// ErrBusy（暂时竞争）与 ErrOverloaded（预算耗尽）都不产生效果、可稍后重试。
// 示例在真实时钟与后台维护并发下运行，偶发 BUSY 属正常路径。
func fetchRetry[V any](ns *ahkc.Namespace[V], key string, loader ahkc.Loader[V]) (V, error) {
	for i := 0; i < 2000; i++ {
		v, err := ns.Fetch(context.Background(), key, loader)
		if errors.Is(err, ahkc.ErrBusy) || errors.Is(err, ahkc.ErrOverloaded) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		return v, err
	}
	var zero V
	return zero, ahkc.ErrBusy
}

func fetchAsyncRetry[V any](ns *ahkc.Namespace[V], key string, loader ahkc.Loader[V]) (*ahkc.Future[V], error) {
	for i := 0; i < 2000; i++ {
		fut, err := ns.FetchAsync(key, loader)
		if errors.Is(err, ahkc.ErrBusy) || errors.Is(err, ahkc.ErrOverloaded) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		return fut, err
	}
	return nil, ahkc.ErrBusy
}

// Example 演示最小用法：创建实例、注册命名空间、查询并加载、只读命中、失效。
func Example() {
	c, err := ahkc.NewBytes[string](ahkc.Config{
		MaxEntries: 1024,
		MaxBytes:   64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"user": {TTL: time.Minute, Tin: 1},
		},
		Runtime: ahkc.RuntimeConfig{
			Clock: ahkc.NewManualClock(int64(time.Second)), // 固定示例时间；生产默认真实单调时钟
		},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close(context.Background())

	ns, _ := c.Namespace("user")
	loader := func(ctx context.Context, key string) (string, error) {
		return "value-of-" + key, nil // 真实场景：读库 / RPC
	}

	// 查询并加载：miss 时回源（singleflight 合并），响应成功不代表已缓存。
	// ErrBusy/ErrOverloaded 是暂时性错误，稍后重试（fetchRetry 演示该模式）。
	v, err := fetchRetry(ns, "u-1", loader)
	if err != nil {
		panic(err)
	}
	fmt.Println("fetch:", v)

	// 填充由后台维护发布（响应成功不代表已缓存），等待发布后只读命中。
	waitPublished(c)

	// 只读查询：命中返回值，不发起加载。
	if v, ok, _ := ns.Get("u-1"); ok {
		fmt.Println("get:", v)
	}

	// 失效：确认后新查询不得取得旧版本。
	if err := ns.Invalidate(context.Background(), "u-1"); err != nil {
		panic(err)
	}
	if _, ok, _ := ns.Get("u-1"); !ok {
		fmt.Println("after invalidate: miss")
	}

	// Output:
	// fetch: value-of-u-1
	// get: value-of-u-1
	// after invalidate: miss
}

// waitPublished 等待后台维护发布填充（示例确定性用）。
func waitPublished(c *ahkc.Cache[string]) {
	for i := 0; i < 400; i++ {
		if c.Stats().FillPublished > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ExampleNamespace_FetchAsync 演示异步加载与 singleflight 合并。
func ExampleNamespace_FetchAsync() {
	c, _ := ahkc.NewBytes[string](ahkc.Config{
		MaxEntries: 1024,
		MaxBytes:   64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"doc": {TTL: time.Minute, Tin: 1},
		},
	})
	defer c.Close(context.Background())
	ns, _ := c.Namespace("doc")

	var calls int
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		calls++
		<-release
		return "doc-body", nil
	}
	// 并发 miss 合并为一次真实回源。
	f1, _ := fetchAsyncRetry(ns, "d-1", loader)
	f2, _ := fetchAsyncRetry(ns, "d-1", loader)
	close(release)
	v1, _ := f1.Wait(context.Background())
	v2, _ := f2.Wait(context.Background())
	fmt.Println(v1, v2, "loads:", calls)

	// Output:
	// doc-body doc-body loads: 1
}

// ExampleCache_Stats 演示指标快照。
func ExampleCache_Stats() {
	c, _ := ahkc.NewBytes[string](ahkc.Config{
		MaxEntries: 1024,
		MaxBytes:   64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"k": {TTL: time.Minute, Tin: 1},
		},
		Runtime: ahkc.RuntimeConfig{
			Clock: ahkc.NewManualClock(int64(time.Second)),
		},
	})
	defer c.Close(context.Background())
	ns, _ := c.Namespace("k")

	fetchRetry(ns, "a", func(ctx context.Context, key string) (string, error) {
		return "1", nil
	})
	waitPublished(c)
	ns.Get("a") // 命中
	ns.Get("b") // 未命中

	st := c.Stats()
	fmt.Println("loads:", st.Loads, "hits:", st.Hits, "misses:", st.Misses, "published:", st.FillPublished)

	// Output:
	// loads: 1 hits: 1 misses: 2 published: 1
}

// ExampleCache_HotKeys 演示仅统计不缓存模式：观测高度重复的数据。
func ExampleCache_HotKeys() {
	c, _ := ahkc.NewBytes[string](ahkc.Config{
		Mode:       ahkc.ModeObserve,
		MaxEntries: 1024,
		MaxBytes:   64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"user": {TTL: time.Minute, Tin: 3},
		},
		Hot: ahkc.HotConfig{LoadSlots: 128},
	})
	defer c.Close(context.Background())
	ns, _ := c.Namespace("user")
	release := make(chan struct{})
	started := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		if key == "u-2" {
			close(started) // the optional load observation has finished
			<-release
		}
		return "v", nil
	}

	// 顺序重复：每次都真实回源（观察模式不缓存）。
	for i := 0; i < 3; i++ {
		fetchRetry(ns, "u-1", loader)
	}
	// 并发重复：合并为一次真实回源。
	f1, _ := fetchAsyncRetry(ns, "u-2", loader)
	<-started
	f2, _ := fetchAsyncRetry(ns, "u-2", loader)
	close(release)
	f1.Wait(context.Background())
	f2.Wait(context.Background())

	// 永不缓存：Get 是确定 miss。
	if _, ok, _ := ns.Get("u-1"); !ok {
		fmt.Println("get u-1: miss (observe only)")
	}
	// 榜单字段确定（不依赖跨桶的窗口排序，按键取值）。
	byKey := map[string]ahkc.HotKeyStat{}
	for _, hk := range c.HotKeys(0) {
		byKey[hk.Key] = hk
	}
	fmt.Printf("u-1: loads=%d merged=%d\n", byKey["u-1"].Loads, byKey["u-1"].Merged)
	fmt.Printf("u-2: loads=%d merged=%d\n", byKey["u-2"].Loads, byKey["u-2"].Merged)

	// Output:
	// get u-1: miss (observe only)
	// u-1: loads=3 merged=0
	// u-2: loads=1 merged=1
}

// ExampleCache_HotKeyHistory 演示窗口摘要历史：每个热度窗口结束时
// 保留一份榜单快照（默认窗口宽 Heat.Delta=1s，由内置周期维护滚动）。
func ExampleCache_HotKeyHistory() {
	c, _ := ahkc.NewBytes[string](ahkc.Config{
		Mode:       ahkc.ModeObserve,
		MaxEntries: 1024,
		MaxBytes:   64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"user": {TTL: time.Minute, Tin: 3},
		},
		Hot: ahkc.HotConfig{
			LoadSlots: 16,
			Windows:   2,
		},
	})
	defer c.Close(context.Background())
	ns, _ := c.Namespace("user")
	loader := func(ctx context.Context, key string) (string, error) {
		return "v", nil
	}

	// 顺序重复回源：u-1 加载 3 次（观察模式不缓存，全部真实回源）。
	for i := 0; i < 3; i++ {
		fetchRetry(ns, "u-1", loader)
	}
	// 等待首个窗口滚动（内置周期维护驱动，至多一个窗口周期）。
	for i := 0; i < 1000 && len(c.HotKeyHistory(0)) == 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	// 窗口快照：只收录该窗口仍有加载活动的 key；发布后不再变更，可安全持有。
	for _, s := range c.HotKeyHistory(0) {
		for _, hk := range s.Top {
			fmt.Printf("%s: loads=%d\n", hk.Key, hk.Loads)
		}
	}

	// Output:
	// u-1: loads=3
}
