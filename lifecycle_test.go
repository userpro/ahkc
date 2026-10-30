package ahkc

import (
	"fmt"
	"testing"
	"time"
)

// 惰性退场的补充用例：提示复核、慢扫描覆盖周期与软预算、命名空间代际失效（访问提示与基本退场见 cache_test.go）。

func publishedEntries(t *testing.T, env *testEnv, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := env.fetch(fmt.Sprintf("k%d", i), okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	if got := env.c.Stats().ResidentEntries; got != int64(n) {
		t.Fatalf("resident = %d, want %d", got, n)
	}
}

// 提示只存索引槽号，维护者按语义复核：槽内条目仍有效（例如旧条目已被新条目取代）时不得退休。
func TestStaleHintDoesNotRetireLiveEntry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) { c.Shards = 1 })
	publishedEntries(t, env, 1)
	c, s := env.c.c, env.c.c.shards[0]
	var slot int
	for i := range s.idx.words {
		if s.idx.slotEntry(i) != nil {
			slot = i
		}
	}
	s.hints[3].Store(uint32(slot) + 1) // 读者曾经在这个槽里见过失效条目，但现在槽内是有效条目
	s.hints[5].Store(1)                // 指向组控制字：没有条目，直接忽略
	if c.consumeHints(s) != 2 {
		t.Fatal("both hints must be consumed")
	}
	if got := env.c.Stats().ResidentEntries; got != 1 {
		t.Fatalf("a live entry was retired by a stale hint: resident=%d", got)
	}
	if _, ok, _ := env.ns.Get("k0"); !ok {
		t.Fatal("live entry lost")
	}
}

// 慢扫描按流逝时间折算扫描量：一个 SweepPeriod 内累计扫描量覆盖整张索引。
func TestSweepCoversIndexWithinSweepPeriod(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
		c.Shards, c.MaxEntries, c.Runtime.SweepPeriod = 1, 2000, 10*time.Second
	})
	c, s := env.c.c, env.c.c.shards[0]
	c.sweepStep(s, 0, false) // 对齐 sweepAt
	total := 0
	for i := 0; i < 10; i++ {
		env.clk.Advance(time.Second)
		total += c.sweepStep(s, 0, false)
	}
	if n := len(s.idx.words); total < n {
		t.Fatalf("swept %d slots in one SweepPeriod, index has %d", total, n)
	}
}

// 慢扫描只在软时间预算内超出配额继续，且每 256 个槽才检查一次时钟。
func TestSweepStopsAtSoftBudgetBeyondAllowance(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
		c.Shards, c.MaxEntries = 1, 2000
	})
	c, s := env.c.c, env.c.c.shards[0]
	s.roundActive, s.roundDeadline = true, c.now()-1 // 预算已耗尽
	if work := c.sweepStep(s, 3, true); work != 256 {
		t.Fatalf("closing sweep over budget did %d slots, want 256 (first clock check)", work)
	}
	s.roundActive = false
	if work := c.sweepStep(s, 3, true); work != len(s.idx.words) {
		t.Fatalf("closing sweep without a budget did %d, want the whole index %d", work, len(s.idx.words))
	}
}

// 命名空间代际失效后，从未被访问的条目也由慢扫描退场。
func TestSweepRetiresInvalidatedNamespaceEntries(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 1 })
	publishedEntries(t, env, 5)
	c, s := env.c.c, env.c.c.shards[0]
	env.ns.ns.gen.Add(1) // 绕过控制队列，只验证慢扫描的代际复核
	c.sweepStep(s, 0, true)
	if got := env.c.Stats().ResidentEntries; got != 0 {
		t.Fatalf("stale generation entries still resident: %d", got)
	}
	if _, ok, _ := env.ns.Get("k0"); ok {
		t.Fatal("stale generation entry delivered")
	}
}

// 退休链很长时，回收每轮只做一个配额，但有进展就立即续行：手动驱动下，维护步数与链长成正比，
// 不依赖周期节流。
func TestReclaimContinuesWhileMakingProgress(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) {
		c.Shards, c.MaxEntries, c.Runtime.RoundWorkLimit = 1, 64, 16
	})
	publishedEntries(t, env, 40)
	s := env.c.c.shards[0]
	env.c.c.sweepStep(s, 0, true) // 全部退休，链长 40
	if s.retiredCount.Load() != 40 {
		t.Fatalf("retired = %d, want 40", s.retiredCount.Load())
	}
	env.drive()
	if n := s.retiredCount.Load(); n != 0 {
		t.Fatalf("retire chain not drained by continuous rounds: %d left", n)
	}
}

// 被拥有型句柄挡住的退休条目没有进展：维护者不得因此空转重排自己。
func TestReclaimBlockedByHandleDoesNotSpin(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 1 })
	publishedEntries(t, env, 1)
	h, ok, err := env.ns.GetOwned("k0")
	if err != nil || !ok {
		t.Fatalf("GetOwned: %v %v", ok, err)
	}
	if _, err := env.ns.InvalidateAsync("k0"); err != nil {
		t.Fatal(err)
	}
	env.drive()
	s := env.c.c.shards[0]
	if s.retiredCount.Load() != 1 {
		t.Fatalf("pinned entry must stay retired, got %d", s.retiredCount.Load())
	}
	if s.reclaimMore {
		t.Fatal("a round blocked by a handle must not request an immediate rerun")
	}
	h.Release()
	env.drive()
	if s.retiredCount.Load() != 0 {
		t.Fatal("released entry was not reclaimed")
	}
	closeManual(t, env.c, env.ex)
}

// 释放内存区前必须先封住所有分片：任一分片忙则全部回滚；封住后调度不再被接受。
func TestSealShardsRollsBackWhenBusyAndBlocksScheduling(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 4 })
	c := env.c.c
	c.shards[3].ownerState.Store(ownerRunning)
	if c.sealShards() {
		t.Fatal("sealed while a shard was running")
	}
	for i := 0; i < 3; i++ {
		if got := c.shards[i].ownerState.Load(); got != ownerIdle {
			t.Fatalf("shard %d not rolled back: state %d", i, got)
		}
	}
	c.shards[3].ownerState.Store(ownerIdle)
	if !c.sealShards() {
		t.Fatal("idle shards must seal")
	}
	c.ensureMaintenanceTask(c.shards[0])
	if got := c.shards[0].ownerState.Load(); got != ownerDead || env.ex.Drain(10) != 0 {
		t.Fatalf("a sealed shard accepted maintenance: state %d", got)
	}
	for _, s := range c.shards {
		s.ownerState.Store(ownerIdle)
	}
	closeManual(t, env.c, env.ex)
}

// 关闭完成后，分片里不得残留指向已释放内存区的指针。
func TestReleaseClearsPointersIntoRegions(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 2 })
	publishedEntries(t, env, 3)
	closeManual(t, env.c, env.ex)
	for _, s := range env.c.c.shards {
		if s.reg.base != nil || s.idxReg.base != nil || s.arena.base != nil || s.idx.words != nil || s.gate.cells != nil {
			t.Fatalf("shard %d keeps pointers into released regions", s.id)
		}
	}
}

// Stats 暴露条目区的已分配/空闲字节与退休链长度：退休块在回收前仍占用条目区。
func TestStatsReportArenaAndRetiredBlocks(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 1 })
	publishedEntries(t, env, 5)
	st := env.c.Stats()
	total := st.ArenaUsedBytes + st.ArenaFreeBytes
	if st.ArenaUsedBytes < st.ResidentBytes || st.ResidentBytes == 0 || st.RetiredPending != 0 {
		t.Fatalf("resident: used=%d resident=%d retired=%d", st.ArenaUsedBytes, st.ResidentBytes, st.RetiredPending)
	}
	env.c.c.sweepStep(env.c.c.shards[0], 0, true) // 全部摘除，进入退休链
	st = env.c.Stats()
	if st.RetiredPending != 5 || st.ResidentBytes != 0 || st.ArenaUsedBytes == 0 {
		t.Fatalf("retired but not reclaimed: pending=%d resident=%d used=%d", st.RetiredPending, st.ResidentBytes, st.ArenaUsedBytes)
	}
	env.drive()
	st = env.c.Stats()
	if st.RetiredPending != 0 || st.ArenaUsedBytes != 0 || st.ArenaUsedBytes+st.ArenaFreeBytes != total {
		t.Fatalf("after reclaim: pending=%d used=%d free=%d total=%d", st.RetiredPending, st.ArenaUsedBytes, st.ArenaFreeBytes, total)
	}
	if st.ArenaLargestFree != st.ArenaFreeBytes {
		t.Fatalf("empty arena must be one free block: largest=%d free=%d", st.ArenaLargestFree, st.ArenaFreeBytes)
	}
}

// 空闲维护零分配：周期触发、慢扫描、提示消费与回收推进都不产生堆分配。
func TestIdleMaintenanceZeroAlloc(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) { c.Shards = 2 })
	publishedEntries(t, env, 20)
	step := func() {
		env.clk.Advance(50 * time.Millisecond)
		env.c.MaintenanceStep()
		env.ex.Drain(100)
	}
	for i := 0; i < 20; i++ { // 预热：执行器队列、任务池走到稳态
		step()
	}
	if a := testing.AllocsPerRun(200, step); a != 0 {
		t.Fatalf("idle maintenance allocs = %v, want 0", a)
	}
}

// 维护被频繁唤醒（填充、失效等）时，慢扫描只按流逝时间扫，不会每次唤醒都扫一个固定下限；
// 折算不足一个槽的时间累计起来，最终仍扫满一个周期。
func TestSweepRateIndependentOfWakeups(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) {
		c.Shards, c.MaxEntries, c.Runtime.SweepPeriod = 1, 2000, 10*time.Second
	})
	c, s := env.c.c, env.c.c.shards[0]
	n := len(s.idx.words)
	total := 0
	for i := 0; i < 100_000; i++ { // 同一时刻被唤醒十万次：没有时间流逝就没有扫描
		total += c.sweepStep(s, 128, false)
	}
	if total != 0 {
		t.Fatalf("100k wakeups with no time elapsed scanned %d slots", total)
	}
	step := c.cfg.Runtime.SweepPeriod / time.Duration(4*n) // 每步只折算 1/4 个槽
	for i := 0; i < 8*n; i++ {
		env.clk.Advance(step)
		total += c.sweepStep(s, 128, false)
	}
	if total < 2*n-2 || total > 2*n+2 { // 8n 步 × 1/4 槽 = 2 个槽圈
		t.Fatalf("fractional owed time was lost or inflated: scanned %d, want about %d", total, 2*n)
	}
}
