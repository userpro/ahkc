// Command hot-observe 直接统计已有业务请求，不加载或缓存值。
package main

import (
	"context"
	"fmt"
	"time"

	"ahkc"
)

func main() {
	c, err := ahkc.New[string](ahkc.Config{
		ObserveOnly: true,
		Namespaces:  map[string]ahkc.NamespaceConfig{"user": {}},
		Advanced: ahkc.Advanced{
			ObserveHotKeys:     -1,                          // 本例没有回源，无需回源表
			ObserveRequestKeys: 8, ObserveRequestSampleP: 1, // 小流量演示逐请求追踪
			HotHistoryWindows: 2,
		},
	})
	if err != nil {
		panic(err)
	}
	defer c.Close(context.Background())
	ns, err := c.Namespace("user")
	if err != nil {
		panic(err)
	}

	for _, key := range []string{"hot", "hot", "cold", "hot", "hot"} {
		_, hit, err := ns.Get(key) // 记录一次请求；原有业务逻辑可继续自行执行
		if err != nil {
			panic(err)
		}
		if hit {
			panic("ObserveOnly must not cache")
		}
	}
	heat, ok := ns.Heat("hot")
	fmt.Printf("hot heat=%.1f known=%v\n", heat, ok)

	// 仅演示时等待一个窗口；生产由消费者定期拉取或接收信号。
	deadline := time.Now().Add(3 * time.Second)
	for len(c.HotKeyHistory(1)) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	history := c.HotKeyHistory(1)
	if len(history) == 0 {
		panic("history did not roll")
	}
	for _, k := range history[0].Requests {
		fmt.Printf("%s: requests≈%.0f error=%.0f\n", k.Key, k.WindowRequests, k.WindowError)
	}
	st := c.Stats()
	if st.Loads != 0 || st.ResidentEntries != 0 {
		panic("unexpected loading or residency")
	}
	fmt.Printf("loads=%d resident=%d\n", st.Loads, st.ResidentEntries)
}
