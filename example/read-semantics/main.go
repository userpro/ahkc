// Command read-semantics 对比三种读取语义，按持有方式选型：
//
//   - Get         值拷贝：返回值属于调用方，最简单，稳态 0 堆分配。
//   - GetBorrowed 借用作用域：受保护的只读视图，限单 goroutine，
//     离开作用域前必须 Release；复用 Request + Borrowed 可全程 0 分配。
//   - GetOwned    拥有型句柄：可跨 goroutine / 异步持有，Release 前该条目
//     不会被回收（I9）；Clone 可再派生独立句柄。
//
// 运行：go run ./example/read-semantics
package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"ahkc"
)

func main() {
	ctx := context.Background()

	c, err := ahkc.New[string](ahkc.Config{
		MaxEntries:       1024,
		MaxResidentBytes: 64 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"doc": {TTL: time.Hour, Tin: 1, Prior: &ahkc.Prior{Lambda: 10}},
		},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close(ctx)

	ns, err := c.Namespace("doc")
	if err != nil {
		panic(err)
	}
	loader := func(ctx context.Context, key string) (string, error) {
		return "body-of-" + key, nil
	}

	// 预热：Fetch 一次并等待后台发布，之后的读取全部命中。
	if _, err := ns.Fetch(ctx, "doc/42", loader); err != nil {
		panic(err)
	}
	waitPublished(c, 1)

	// 1) 值拷贝读取：v 按 Go 规则浅拷贝；引用类型仍共享底层存储。
	v, ok, err := ns.Get("doc/42")
	if err != nil || !ok {
		panic("expected cached value")
	}
	fmt.Println("get:", v)

	// 2) 借用作用域：零分配热路径。栈上复用 Request 与 Borrowed，
	//    读完必须 Release（允许复用），且不得跨 goroutine 转让。
	var request ahkc.Request[string]
	var borrowed ahkc.Borrowed[string]
	if err := ns.BeginRequestOn(&request, "doc/42"); err != nil {
		panic(err)
	}
	ok, err = ns.BorrowOn(&request, &borrowed)
	if err != nil || !ok {
		panic("borrow miss")
	}
	runtime.KeepAlive(borrowed.Value())
	borrowed.Release()                        // 归还保护；Release 后可再次 BorrowOn 复用
	view, ok, err := ns.GetBorrowed("doc/42") // 便捷形式，自动创建 Request 和视图
	if err != nil || !ok {
		panic("borrow miss")
	}
	fmt.Println("borrowed:", view.Value())
	view.Release()

	// 3) 拥有型句柄：跨 goroutine / 异步安全持有。
	//    Release 之前条目保证不被回收，即使后台已判定淘汰或失效。
	handle, ok, err := ns.GetOwned("doc/42")
	if err != nil || !ok {
		panic("owned miss")
	}
	//    Clone 派生独立句柄（失效后仍可读出旧值），各自独立 Release。
	clone, err := handle.Clone()
	if err != nil {
		panic(err)
	}
	// 逻辑失效后，既有句柄仍持有旧值。
	if err := ns.Invalidate(ctx, "doc/42"); err != nil {
		panic(err)
	}
	done := make(chan string, 1)
	go func() { // 句柄可以在其他 goroutine 中使用
		done <- clone.Value()
		clone.Release()
	}()
	fmt.Println("owned in goroutine:", <-done)
	handle.Release()

	// 失效后：Get 立即 miss；已持有的句柄仍可读出旧值直到 Release（安全回收）。
	if _, ok, err := ns.Get("doc/42"); err != nil || ok {
		panic("expected miss after invalidation")
	}
	fmt.Println("after invalidate: miss")

	st := c.Stats()
	fmt.Printf("stats: hits=%d misses=%d\n", st.Hits, st.Misses)
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
