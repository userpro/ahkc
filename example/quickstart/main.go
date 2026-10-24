// Command quickstart 演示 AHKC 的最小完整流程：
// 创建实例 → 查询并加载（Fetch）→ 等待后台发布 → 只读命中（Get）→
// 失效（Invalidate）→ 指标（Stats）→ 关闭（Close）。
//
// 运行：go run ./example/quickstart
package main

import (
	"context"
	"fmt"
	"time"

	"ahkc"
)

func main() {
	ctx := context.Background()

	// 创建实例：MaxEntries / MaxResidentBytes 必填，其余走默认值。
	// Prior 提供请求率先验：冷启动阶段（收益 sketch 热身的 2×ValueDelta 内）
	// 也能给出估值，否则新填充会因 NO_ESTIMATE 一律拒绝。
	c, err := ahkc.New[string](ahkc.Config{
		MaxEntries:       1024,
		MaxResidentBytes: 64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"user": {TTL: time.Minute, Tin: 1, Prior: &ahkc.Prior{Lambda: 10}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close(ctx)

	ns, err := c.Namespace("user")
	if err != nil {
		panic(err)
	}

	loader := func(ctx context.Context, key string) (string, error) {
		return "value-of-" + key, nil // 真实场景：读库 / RPC / 远端文件
	}

	// Fetch = 查询并加载：miss 时回源（singleflight 合并并发请求）。
	// 注意契约：响应成功不代表已缓存，填充由后台维护发布。
	v, err := ns.Fetch(ctx, "u-1", loader)
	if err != nil {
		panic(err)
	}
	fmt.Println("fetch:", v)

	// 填充是异步的：轮询 Stats().FillPublished 等待发布（生产中通常不等待，
	// 下次访问 miss 会再次回源并最终驻留）。
	waitPublished(c, 1)

	// Get = 只读查询：命中返回值，不发起加载。
	v, ok, err := ns.Get("u-1")
	if err != nil || !ok {
		panic("expected cached value")
	}
	fmt.Println("get:", v)

	// Invalidate = 单 key 失效：确认后新查询不得取得旧版本。
	if err := ns.Invalidate(ctx, "u-1"); err != nil {
		panic(err)
	}
	if _, ok, err := ns.Get("u-1"); err != nil || ok {
		panic("expected miss after invalidation")
	}
	fmt.Println("after invalidate: miss")

	st := c.Stats()
	fmt.Printf("stats: loads=%d hits=%d misses=%d published=%d\n",
		st.Loads, st.Hits, st.Misses, st.FillPublished)
}

func waitPublished[V any](c *ahkc.Cache[V], want uint64) {
	for i := 0; i < 400; i++ {
		if c.Stats().FillPublished >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("fill was not published before the demo deadline")
}
