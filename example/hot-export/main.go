// Command hot-export 用信号与游标导出 JSON 窗口，并输出固定维度的汇总指标。
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"ahkc"
)

func main() {
	signal, stop := make(chan struct{}, 1), make(chan struct{})
	c, err := ahkc.NewBytes[string](ahkc.Config{
		Mode:       ahkc.ModeObserve,
		Namespaces: map[string]ahkc.NamespaceConfig{"user": {}},
		Hot: ahkc.HotConfig{
			LoadSlots:      16,
			RequestSlots:   16,
			RequestSampleP: 1,
			Windows:        8,
			Interval:       500 * time.Millisecond,
			TopN:           5,
			Label: func(namespace, key string) (string, bool) {
				sum := sha256.Sum256([]byte(namespace + "/" + key))
				return hex.EncodeToString(sum[:8]), true // 标签映射不改变内部追踪
			},
			OnWindow: func(ahkc.HotSnapshot) {
				select {
				case signal <- struct{}{}:
				default:
				}
			},
		},
	})
	must(err)
	defer c.Close(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); consume(c, signal, stop) }()
	ns, err := c.Namespace("user")
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	loader := func(context.Context, string) (string, error) { return "v", nil }
	for i := 0; ctx.Err() == nil; i++ {
		key := "hot"
		if i%3 == 0 {
			key = fmt.Sprintf("cold-%d", i%20)
		}
		var r ahkc.Request[string]
		must(ns.BeginRequestOn(&r, key))
		for attempt := 0; attempt < 3; attempt++ {
			_, err := ns.FetchOn(ctx, &r, loader)
			if !errors.Is(err, ahkc.ErrBusy) {
				if ctx.Err() == nil {
					must(err)
				}
				break
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond) // 等包含最后一批请求的窗口结束
	close(stop)
	wg.Wait()
}

// 通知可能丢失：定时兜底，拉取后推进游标，检查覆盖与采集缺口。
func consume(c *ahkc.Cache[string], signal, stop <-chan struct{}) {
	enc := json.NewEncoder(os.Stdout)
	var cursor uint64
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	drain := func() {
		snaps, lost := c.HotWindowsSince(cursor)
		if lost > 0 {
			fmt.Printf("# overwritten windows: %d\n", lost)
		}
		for _, s := range snaps {
			if err := enc.Encode(s); err != nil {
				panic(err)
			}
			cursor = s.Seq // 仅在成功导出后推进
		}
	}
	for {
		select {
		case <-signal:
			drain()
		case <-tick.C:
			drain()
		case <-stop:
			drain()
			st := c.Stats()
			// 动态 key 放 JSON 日志；指标只使用固定维度，避免高基数。
			fmt.Printf("ahkc_loads_total %d\nahkc_hot_window_dropped_total %d\n", st.Loads, st.HotWindowDropped)
			return
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
