package ahkc

import (
	"context"
	"testing"
	"time"
)

// runShadow 对同一热 key 连续 Fetch n 次，返回环境与 loader 被调用次数。
func runShadow(t *testing.T, shadow bool, n int) (*testEnv, int) {
	t.Helper()
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) {
			if shadow {
				c.Mode = ModeShadow
			}
		})
	loads := 0
	loader := func(ctx context.Context, key string) (string, error) {
		loads++
		return "v-" + key, nil
	}
	for i := 0; i < n; i++ {
		if v, err := env.fetch("k1", loader); err != nil || v != "v-k1" {
			t.Fatalf("fetch %d: %q %v", i, v, err)
		}
	}
	return env, loads
}

// 影子条目与真实条目同样按 TTL 过期：越过原 TTL 后不再命中。
func TestShadowFollowsTTL(t *testing.T) {
	var hits [2]uint64
	for i, shadow := range []bool{false, true} {
		env, _ := runShadow(t, shadow, 6)
		defer closeManual(t, env.c, env.ex)
		for _, d := range []time.Duration{30 * time.Second, 35 * time.Second} { // 第二次已越过原 TTL（1m）
			env.clk.Advance(d)
			env.drive()
			if _, err := env.fetch("k1", okLoader("v-k1")); err != nil {
				t.Fatal(err)
			}
		}
		hits[i] = env.c.Stats().Hits
	}
	if hits[0] != hits[1] {
		t.Fatalf("hits: cache %d, shadow %d", hits[0], hits[1])
	}
}

// 影子缓存的命中率与真实缓存同口径，但每个请求都照旧回源且只发布一次。
func TestShadowMatchesCacheHitRate(t *testing.T) {
	const n = 8
	real, realLoads := runShadow(t, false, n)
	defer closeManual(t, real.c, real.ex)
	shadow, shadowLoads := runShadow(t, true, n)
	defer closeManual(t, shadow.c, shadow.ex)

	rs, ss := real.c.Stats(), shadow.c.Stats()
	if rs.Hits == 0 || realLoads >= n {
		t.Fatalf("cache mode never hit, test is vacuous: %+v loads=%d", rs, realLoads)
	}
	if ss.Hits != rs.Hits || ss.Misses != rs.Misses {
		t.Fatalf("hit rate differs: shadow %d/%d, cache %d/%d", ss.Hits, ss.Misses, rs.Hits, rs.Misses)
	}
	if shadowLoads != n || ss.Loads != n {
		t.Fatalf("shadow must load from source every time: loads=%d stats=%d", shadowLoads, ss.Loads)
	}
	if ss.Mode != ModeShadow || rs.Mode != ModeCache {
		t.Fatalf("Stats.Mode: shadow=%v cache=%v", ss.Mode, rs.Mode)
	}
	if ss.FillPublished != 1 || ss.ResidentEntries != 1 {
		t.Fatalf("shadow must publish once: %+v", ss)
	}
	if got := ss.FillRejected["SHADOW_HIT"]; got != ss.Hits {
		t.Fatalf("SHADOW_HIT = %d, want %d", got, ss.Hits)
	}
}

// 读接口在影子命中时一律返回 miss，驻留条目不保留 value。
func TestShadowNeverServesNorRetainsValue(t *testing.T) {
	env, _ := runShadow(t, true, 6)
	defer closeManual(t, env.c, env.ex)

	if v, ok, err := env.ns.Get("k1"); err != nil || ok || v != "" {
		t.Fatalf("Get: %q %v %v", v, ok, err)
	}
	if h, ok, err := env.ns.GetOwned("k1"); err != nil || ok || h != nil {
		t.Fatalf("GetOwned: %v %v %v", h, ok, err)
	}
	if b, ok, err := env.ns.GetBorrowed("k1"); err != nil || ok || b != nil {
		t.Fatalf("GetBorrowed: %v %v %v", b, ok, err)
	}
	kh := env.ns.ns.hashKey("k1")
	s := env.c.c.shardOf(kh)
	g, ok := s.epoch.pin()
	if !ok {
		t.Fatal("pin")
	}
	defer g.unpin()
	if fr := s.idx.find("k1", kh, env.ns.ns.id); !fr.found || fr.e.valLen != 0 {
		t.Fatalf("resident entry: found=%v valLen=%d", fr.found, fr.e.valLen)
	}
}

// 影子命中路径不新增堆分配（Request 在栈上，计数复用分片条纹）。
func TestShadowHitGetNoAlloc(t *testing.T) {
	env, _ := runShadow(t, true, 6)
	defer closeManual(t, env.c, env.ex)

	before := env.c.Stats().Hits
	if allocs := testing.AllocsPerRun(100, func() { _, _, _ = env.ns.Get("k1") }); allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
	if got := env.c.Stats().Hits - before; got != 101 {
		t.Fatalf("shadow hits = %d, want 101", got)
	}
}
