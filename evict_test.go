package ahkc

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// fullCacheEnv 返回一个单分片、容量 2 条的缓存，并先填满两个冷 key（热度 1，成本 1）。
func fullCacheEnv(t *testing.T) *testEnv {
	t.Helper()
	env := newTestEnv(t, NamespaceConfig{TTL: 10 * time.Minute, Tin: 1}, func(c *Config) {
		c.Shards, c.MaxEntries = 1, 2
	})
	for _, k := range []string{"k1", "k2"} {
		if _, err := env.fetch(k, okLoader("v-"+k)); err != nil {
			t.Fatal(err)
		}
		env.drive()
	}
	if n := env.c.Stats().ResidentEntries; n != 2 {
		t.Fatalf("setup: resident=%d want 2", n)
	}
	return env
}

// 收益 = 热度 × 成本 × 剩余有效期：满载后，热度更高的候选替换冷条目；
// 热度相同（净收益为 0）时不替换。
func TestHotCandidateReplacesColdEntryWhenFull(t *testing.T) {
	env := fullCacheEnv(t)
	defer closeManual(t, env.c, env.ex)

	if _, err := env.fetch("k3", okLoader("v-k3")); err != nil { // 热度 1，与受害者相同
		t.Fatal(err)
	}
	env.drive()
	if st := env.c.Stats(); st.FillRejected["NON_POSITIVE_GAIN"] == 0 {
		t.Fatalf("equal heat must not replace: %v", st.FillRejected)
	}
	if _, ok, _ := env.ns.Get("k3"); ok {
		t.Fatal("k3 was admitted with zero net gain")
	}
	for i := 0; i < 3; i++ { // 热度升到 4
		if _, err := env.fetch("k3", okLoader("v-k3")); err != nil {
			t.Fatal(err)
		}
		env.drive()
	}
	if _, ok, _ := env.ns.Get("k3"); !ok {
		t.Fatalf("hotter candidate must replace a cold entry: %v", env.c.Stats().FillRejected)
	}
	if n := env.c.Stats().ResidentEntries; n != 2 {
		t.Fatalf("capacity must stay at 2: %d", n)
	}
}

// 单次回源成本缩放收益：同热度下，高成本结果替换低成本条目。
func TestCostScalesReplacementValue(t *testing.T) {
	env := fullCacheEnv(t)
	defer closeManual(t, env.c, env.ex)
	env.c.c.metadata = func(key string, _ string) ResultMetadata {
		if key == "k3" {
			return ResultMetadata{Cost: 10}
		}
		return ResultMetadata{}
	}
	if _, err := env.fetch("k3", okLoader("v-k3")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("k3"); !ok {
		t.Fatalf("a 10x costlier result must replace a cold entry: %v", env.c.Stats().FillRejected)
	}
}

// 非法成本（负数、NaN、Inf）的结果不入缓存，业务结果照常交付。
func TestInvalidCostIsNotCached(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	env.c.c.metadata = func(string, string) ResultMetadata { return ResultMetadata{Cost: -1} }
	v, err := env.fetch("k", okLoader("v"))
	if err != nil || v != "v" {
		t.Fatalf("business result must deliver: %q %v", v, err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("k"); ok {
		t.Fatal("invalid cost result was cached")
	}
}

// 字节额度满载后用变长 value 持续替换：分配器的碎片与未回收的退休块不得让填充大面积失败。
// 15% 余量下实测为 0（余量 5%、0 也是 0）；断言 1% 是为了在回归时报警而不是追求零。
func TestChurnAtFullByteQuotaKeepsAllocationFailuresRare(t *testing.T) {
	const byteBudget, keys, fetches = 6 << 20, 120000, 400000
	cfg := Config{
		Shards:        4,
		MaxEntries:    200000,
		MaxBytes:      byteBudget,
		MaxEntryBytes: 1024,
		Namespaces:    map[string]NamespaceConfig{"t": {TTL: time.Hour, Tin: 1}},
		Heat: HeatConfig{
			Delta: time.Second,
		},
	}
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	ns, _ := c.Namespace("t")
	loader := func(_ context.Context, k string) (string, error) {
		return strings.Repeat("v", 16+len(k)*7%225), nil // 16~240 字节
	}
	zipf := rand.NewZipf(rand.New(rand.NewSource(1)), 1.05, 1, keys-1)
	for i := 0; i < fetches; i++ {
		k := fmt.Sprintf("key-%d", zipf.Uint64())
		for {
			if _, err := ns.Fetch(context.Background(), k, loader); err == nil {
				break
			}
		}
	}
	for c.Stats().PendingFills > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	st := c.Stats()
	fills := st.FillPublished
	for _, n := range st.FillRejected {
		fills += n
	}
	if got := st.FillRejected["ALLOCATION_FAILED"]; got*100 > fills {
		t.Fatalf("ALLOCATION_FAILED %d of %d fills (>1%%): %v", got, fills, st.FillRejected)
	}
	if st.ResidentBytes < byteBudget*9/10 {
		t.Fatalf("resident bytes %d of %d: the byte quota never filled, the test is not exercising the allocator", st.ResidentBytes, byteBudget)
	}
}
