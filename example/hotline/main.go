// Command hotline 演示真实时钟下的并发缓存、逻辑请求、失效与窗口报表。
// go run ./example/hotline -duration=12s -workers=16 -keys=3000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"ahkc"
)

func main() {
	duration := flag.Duration("duration", 12*time.Second, "运行时长")
	workers := flag.Int("workers", 16, "并发协程数")
	numKeys := flag.Int("keys", 3000, "key 数量")
	skew := flag.Float64("skew", 1.2, "Zipf 倾斜度，须大于 1")
	report := flag.Duration("report", 500*time.Millisecond, "报表周期")
	flag.Parse()
	if *duration <= 0 || *workers <= 0 || *numKeys <= 0 || *report <= 0 || !(*skew > 1) || math.IsInf(*skew, 0) {
		panic("duration/workers/keys/report 须为正，skew 须有限且大于 1")
	}
	c, err := ahkc.NewBytes[string](ahkc.Config{
		MaxEntries: int64(max(16, *numKeys)),
		MaxBytes:   256 << 20,
		Namespaces: map[string]ahkc.NamespaceConfig{
			"order": {TTL: 30 * time.Second, Tin: 3},
		},
		Hot: ahkc.HotConfig{
			LoadSlots: 64,
		},
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			fmt.Println("close:", err)
		}
	}()
	ns, err := c.Namespace("order")
	if err != nil {
		panic(err)
	}
	loader := func(context.Context, string) (string, error) {
		time.Sleep(2 * time.Millisecond) // 实际业务应自行设置数据库/RPC 期限
		return "value", nil
	}
	keys := make([]string, *numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("order-%d", i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			zipf := rand.NewZipf(rng, *skew, 1, uint64(len(keys)-1))
			for ctx.Err() == nil {
				_, err := getOrLoad(ctx, ns, keys[zipf.Uint64()], loader)
				if err != nil && !errors.Is(err, ahkc.ErrBusy) && !errors.Is(err, ahkc.ErrOverloaded) && ctx.Err() == nil {
					fmt.Println("fetch:", err)
				}
				time.Sleep(300 * time.Microsecond)
			}
		}(int64(w + 1))
	}
	tick, invalidate := time.NewTicker(*report), time.NewTicker(4*time.Second)
	defer tick.Stop()
	defer invalidate.Stop()
	previous := c.Stats()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			st := c.Stats()
			fmt.Printf("total: hit=%d miss=%d busy=%d overload=%d loads=%d merged=%d\n",
				st.Hits, st.Misses, st.Busy, st.Overloaded, st.Loads, st.LoadMerged)
			return
		case <-invalidate.C:
			// Zipf 排名第一的 key 更新后失效，下次访问重新回源。
			if err := ns.Invalidate(ctx, keys[0]); err != nil {
				fmt.Println("invalidate:", err)
			}
		case <-tick.C:
			st := c.Stats()
			hits, misses := st.Hits-previous.Hits, st.Misses-previous.Misses
			rate := float64(0)
			if hits+misses > 0 {
				rate = 100 * float64(hits) / float64(hits+misses)
			}
			fmt.Printf("window: hit=%.1f%% busy=%d loads=%d merged=%d resident=%d\n",
				rate, st.Busy-previous.Busy, st.Loads-previous.Loads, st.LoadMerged-previous.LoadMerged, st.ResidentEntries)
			previous = st
			for _, k := range c.HotKeys(3) {
				heat, known := ns.Heat(k.Key)
				fmt.Printf("  %s: load_heat=%.1f loads=%d merged=%d request_heat=%.1f known=%v\n",
					k.Key, k.LoadHeat, k.Loads, k.Merged, heat, known)
			}
		}
	}
}

// 同一逻辑请求复用于查询、加载与 BUSY 重试；过载交给业务退避。
func getOrLoad(ctx context.Context, ns *ahkc.Namespace[string], key string, loader ahkc.Loader[string]) (string, error) {
	var r ahkc.Request[string]
	if err := ns.BeginRequestOn(&r, key); err != nil {
		return "", err
	}
	for i := 0; i < 3; i++ {
		v, hit, err := ns.GetOn(&r)
		if errors.Is(err, ahkc.ErrBusy) {
			continue
		}
		if err != nil || hit {
			return v, err
		}
		v, err = ns.FetchOn(ctx, &r, loader)
		if !errors.Is(err, ahkc.ErrBusy) {
			return v, err
		}
	}
	return "", ahkc.ErrBusy
}
