package ahkc

import (
	"testing"
	"time"
)

func TestWheelEmptyTicksConsumeQuota(t *testing.T) {
	w, _, _ := wheelEnv(t)
	work := w.advance(6000, 1, 64, nil, nil)
	if work != 1 || w.cursorTick > 1 {
		t.Fatalf("empty wheel exceeded one unit of work: work=%d cursor=%d", work, w.cursorTick)
	}
}

// wheelEnv 构造测试环境：tick=100ms，K=8，L=3（覆盖 51.2s）。
func wheelEnv(t *testing.T) (*timerWheel[string], *index[string], *entryPool[string]) {
	t.Helper()
	w := newTimerWheel[string](100, 3, 8, 0) // tick=100ns（测试尺度）
	ix := newIndex[string](64, 8)
	pool := newEntryPool[string](8)
	return w, ix, pool
}

func mkWheelEntry(t *testing.T, pool *entryPool[string], ix *index[string], key string, kh uint64, deadline int64) *entry[string] {
	t.Helper()
	e, ok := pool.tryGet()
	if !ok {
		t.Fatal("pool exhausted")
	}
	e.key, e.keyHash, e.hardDeadline = key, kh, deadline
	e.readable.Store(true)
	slot, ok := ix.installSlot(kh, nil)
	if !ok {
		t.Fatal("install failed")
	}
	ix.publish(e, slot)
	return e
}

func TestWheelNoEarlyExpiry(t *testing.T) {
	w, ix, pool := wheelEnv(t)
	e := mkWheelEntry(t, pool, ix, "a", 1, 1000) // 10 tick 后到期
	valid := func(e *entry[string], now int64) bool { return now < e.hardDeadline }
	var retired []*entry[string]
	retire := func(e *entry[string]) bool { retired = append(retired, e); return true }
	w.add(e)
	// 推进到 tick 9（900ns）：不得过期。
	w.advance(900, 100, 64, valid, retire)
	if len(retired) != 0 || !e.timerMember {
		t.Fatalf("early expiry or lost membership: retired=%d member=%v", len(retired), e.timerMember)
	}
	// 推进到 tick 10：到期。
	w.advance(1000, 100, 64, valid, retire)
	if len(retired) != 1 || retired[0] != e {
		t.Fatalf("expiry failed: %d", len(retired))
	}
	if e.timerMember {
		t.Fatal("retired entry still has timer membership")
	}
}

func TestWheelCascadeLevels(t *testing.T) {
	w, ix, pool := wheelEnv(t)
	// 截止 tick 60（远超第一层 K=8 跨度），应挂高层。
	e := mkWheelEntry(t, pool, ix, "far", 2, 6000)
	valid := func(e *entry[string], now int64) bool { return now < e.hardDeadline }
	var retired []*entry[string]
	retire := func(e *entry[string]) bool { retired = append(retired, e); return true }
	w.add(e)
	if e.timerLevel == 0 {
		t.Fatal("far entry must start at higher level")
	}
	// 推进到 5900：未到期，经级联降层。
	w.advance(5900, 10000, 64, valid, retire)
	if len(retired) != 0 {
		t.Fatal("cascade early expiry")
	}
	if !e.timerMember {
		t.Fatal("lost membership during cascade")
	}
	w.advance(6000, 10000, 64, valid, retire)
	if len(retired) != 1 {
		t.Fatalf("cascade expiry failed: %d", len(retired))
	}
}

func TestWheelRebase(t *testing.T) {
	w, ix, pool := wheelEnv(t)
	e1 := mkWheelEntry(t, pool, ix, "x", 3, 30000)
	e2 := mkWheelEntry(t, pool, ix, "y", 4, 31000)
	valid := func(e *entry[string], now int64) bool { return now < e.hardDeadline }
	var retired []*entry[string]
	retire := func(e *entry[string]) bool { retired = append(retired, e); return true }
	w.add(e1)
	w.add(e2)
	// 长时间暂停：滞后超 maxLag → advance 要求 rebase。
	if r := w.advance(29000, 100, 10, valid, retire); r != -1 {
		t.Fatalf("expected rebase signal, got %d", r)
	}
	w.startRebase(29000 / 100)
	if done := w.rebaseStep(ix, 100, 29000, valid, retire); !done {
		t.Fatal("rebase incomplete")
	}
	if !e1.timerMember || !e2.timerMember {
		t.Fatal("rebase lost memberships")
	}
	if e1.timerEpoch != w.epoch || e2.timerEpoch != w.epoch {
		t.Fatal("rebase must refresh timer epoch")
	}
	// 推进到到期点后正常过期。
	w.advance(30000, 10000, 10, valid, retire)
	if len(retired) != 1 || retired[0] != e1 {
		t.Fatalf("post-rebase expiry: %d", len(retired))
	}
	w.advance(31000, 10000, 10, valid, retire)
	if len(retired) != 2 {
		t.Fatalf("post-rebase expiry 2: %d", len(retired))
	}
}

func TestWheelRemoveMember(t *testing.T) {
	w, ix, pool := wheelEnv(t)
	e := mkWheelEntry(t, pool, ix, "z", 5, 2000)
	w.add(e)
	w.remove(e)
	if e.timerMember {
		t.Fatal("remove failed")
	}
	// 重复 remove 安全。
	w.remove(e)
	// 过期扫描不应再看到它。
	valid := func(e *entry[string], now int64) bool { return now < e.hardDeadline }
	var retired []*entry[string]
	retire := func(e *entry[string]) bool { retired = append(retired, e); return true }
	w.advance(5000, 1000, 64, valid, retire)
	if len(retired) != 0 {
		t.Fatal("removed entry processed")
	}
}

// TestWheelCursorStartsAtConstructionTime 覆盖时间轮游标初值：时钟原点之后很久才创建的实例，
// 游标必须从构造时刻所在 tick 开始，否则首轮维护就会因“滞后”触发受限重建，
// 重建完成前所有填充被拒为 RESOURCE_LIMIT。
func TestWheelCursorStartsAtConstructionTime(t *testing.T) {
	const now = 3600 * 1_000_000_000 // 构造时钟已读到 1 小时
	w := newTimerWheel[string](100_000_000, 3, 8, now)
	if got, want := w.cursorTick, int64(now/100_000_000); got != want {
		t.Fatalf("cursorTick=%d, want %d (construction tick)", got, want)
	}
	if work := w.advance(now, 64, 64, nil, nil); work < 0 {
		t.Fatal("a freshly constructed wheel must not request a rebase on its first round")
	}
}

// TestLateCreatedInstancePublishesWithoutWheelRebase 是端到端形式：时钟读数远大于
// WheelTick×MaxWheelLag 时创建的实例，首次填充不得因时间轮重建被拒。
func TestLateCreatedInstancePublishesWithoutWheelRebase(t *testing.T) {
	late := NewManualClock(3600 * 1_000_000_000)
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) { c.Clock = late })
	for i := 0; i < 2; i++ {
		if _, err := env.fetch("k", okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	env.drive()
	if n := env.c.Stats().FillRejected["RESOURCE_LIMIT"]; n != 0 {
		t.Fatalf("first round must not rebase the wheel: RESOURCE_LIMIT=%d", n)
	}
	if v, ok, _ := env.ns.Get("k"); !ok || v != "v" {
		t.Fatalf("entry must be published: %v %v", v, ok)
	}
}
