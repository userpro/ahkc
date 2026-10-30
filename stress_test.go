package ahkc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentMixedStress 并发混合压测（真实时钟 + goroutine 执行器 + 内置 ticker）：
// 在 Get/Fetch/Invalidate 混合负载下验证无数据竞争（配合 -race）、
// 容量不变量（I1）与预算守恒（I2）。
func TestConcurrentMixedStress(t *testing.T) {
	cfg := Config{
		Shards:     16,
		MaxEntries: 512,
		MaxBytes:   4 << 20,
		Namespaces: map[string]NamespaceConfig{
			"a": {TTL: 2 * time.Second, Tin: 2},
			"b": {TTL: 3 * time.Second, Tin: 3},
		},
		Limits: LimitsConfig{
			MaxConcurrentLoads: 32,
			LoadTimeout:        500 * time.Millisecond,
			HandoffTimeout:     200 * time.Millisecond,
		},
		Runtime: RuntimeConfig{
			PeriodicInterval: 5 * time.Millisecond,
		},
	}
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	namespaces := []*Namespace[string]{}
	for _, name := range []string{"a", "b"} {
		ns, err := c.Namespace(name)
		if err != nil {
			t.Fatal(err)
		}
		namespaces = append(namespaces, ns)
	}
	errLoad := errors.New("load failed")
	loader := func(ctx context.Context, key string) (string, error) {
		if rand.Uint64N(50) == 0 { // 2% 错误率
			return "", errLoad
		}
		time.Sleep(time.Duration(rand.Uint64N(1000)) * time.Microsecond) // 0-1ms 慢源
		return "v-" + key, nil
	}

	var unexpected atomic.Int64
	var wg sync.WaitGroup
	const workers = 32
	const opsPerWorker = 300
	for wkr := 0; wkr < workers; wkr++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				ns := namespaces[rand.Uint64N(uint64(len(namespaces)))]
				key := fmt.Sprintf("k%d", rand.Uint64N(256))
				switch rand.Uint64N(100) {
				case 0, 1, 2, 3: // 4% 失效
					h, err := ns.InvalidateAsync(key)
					if err == nil {
						h.Wait(context.Background())
					} else if !errors.Is(err, ErrBusy) && !errors.Is(err, ErrClosed) {
						unexpected.Add(1)
					}
				default:
					if rand.Uint64N(100) < 55 { // 只读
						_, _, err := ns.Get(key)
						if err != nil && !errors.Is(err, ErrBusy) && !errors.Is(err, ErrClosed) {
							unexpected.Add(1)
						}
					} else { // 加载
						_, err := ns.Fetch(context.Background(), key, loader)
						switch {
						case err == nil, errors.Is(err, errLoad), errors.Is(err, ErrBusy),
							errors.Is(err, ErrOverloaded), errors.Is(err, ErrTimeout),
							errors.Is(err, ErrClosed):
						default:
							unexpected.Add(1)
						}
					}
				}
			}
		}(wkr)
	}
	wg.Wait()
	if unexpected.Load() != 0 {
		t.Fatalf("unexpected errors: %d", unexpected.Load())
	}
	// 运行期不变量：容量不超限，预算余量在有界区间内。
	time.Sleep(200 * time.Millisecond) // ticker 推进收尾
	st := c.Stats()
	if st.ResidentEntries > cfg.MaxEntries {
		t.Fatalf("I1 violated: resident=%d > %d", st.ResidentEntries, cfg.MaxEntries)
	}
	for _, s := range c.c.shards {
		if s.residentCount.Load() > s.countQuota.Load() {
			t.Fatalf("I1 violated on shard %d: resident=%d quota=%d", s.id, s.residentCount.Load(), s.countQuota.Load())
		}
	}
	checkAvail := func(name string, av, limit int64) {
		t.Helper()
		if av < 0 || av > limit {
			t.Fatalf("budget %s out of bounds: avail=%d limit=%d", name, av, limit)
		}
	}
	checkAvail("io", c.c.ioBudget.available(), c.c.ioBudget.limit)
	checkAvail("waiter", c.c.waiterBudget.available(), c.c.waiterBudget.limit)
	checkAvail("pendingBytes", c.c.pendingBytes.available(), c.c.pendingBytes.limit)
	checkAvail("responseBytes", c.c.responseBytes.available(), c.c.responseBytes.limit)

	// 关闭后预算守恒（I2）：所有额度归还。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if av := c.c.ioBudget.available(); av != c.c.ioBudget.limit {
		t.Fatalf("I2: io budget not conserved: %d/%d", av, c.c.ioBudget.limit)
	}
	if av := c.c.waiterBudget.available(); av != c.c.waiterBudget.limit {
		t.Fatalf("I2: waiter budget not conserved: %d/%d", av, c.c.waiterBudget.limit)
	}
	if av := c.c.pendingBytes.available(); av != c.c.pendingBytes.limit {
		t.Fatalf("I2: pendingBytes not conserved: %d/%d", av, c.c.pendingBytes.limit)
	}
	if av := c.c.responseBytes.available(); av != c.c.responseBytes.limit {
		t.Fatalf("I2: responseBytes not conserved: %d/%d", av, c.c.responseBytes.limit)
	}
	if st.FillPublished == 0 || st.Loads == 0 {
		t.Fatalf("stress did no work: %+v", st)
	}
}

// TestCloseReclaimsUnderLoad 关闭与在途加载交错：Close 返回 nil 即 RECLAIMED。
func TestCloseReclaimsUnderLoad(t *testing.T) {
	cfg := Config{
		Shards:     4,
		MaxEntries: 64,
		MaxBytes:   1 << 20,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: 2 * time.Second, Tin: 2}},
		Runtime: RuntimeConfig{
			PeriodicInterval: 2 * time.Millisecond,
		},
	}
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	var wg sync.WaitGroup
	stop := atomic.Bool{}
	for wkr := 0; wkr < 8; wkr++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stop.Load() {
				ns.Fetch(context.Background(), fmt.Sprintf("k%d", id), okLoader("v"))
			}
		}(wkr)
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil { // Close 停止受理；工作者随后退出
		t.Fatalf("close under load: %v", err)
	}
	stop.Store(true)
	wg.Wait()
	progress := c.CloseProgress()
	stopped, drained, reclaimed := progress.Stopped, progress.Drained, progress.Reclaimed
	if !stopped || !drained || !reclaimed {
		t.Fatalf("CloseState = %v %v %v", stopped, drained, reclaimed)
	}
}
