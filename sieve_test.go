package ahkc

import "testing"

// SIEVE 策略队列（sieve.go）纯数据结构单元测试。
// 正交说明：cache_test.go 的替换测试覆盖队列在真实填充中的行为；
// 本文件聚焦链接结构与 hand 修正的每个分支（白盒、确定性）。

// newTestPolicy 返回一个建立在小 arena 上的空策略队列与三个未入队条目。
func newTestPolicy(t *testing.T) (l *policyList, a *arena, e1, e2, e3 *entry) {
	t.Helper()
	a, r := newTestArena(t, 1<<14)
	t.Cleanup(r.release)
	mk := func() *entry {
		ref, ok := a.alloc(int(EntryHeaderBytes))
		if !ok {
			t.Fatal("alloc")
		}
		e := a.entry(ref)
		*e = entry{boundary: e.boundary} // 保留 arena 的边界字
		return e
	}
	return &policyList{a: a}, a, mk(), mk(), mk()
}

func TestPolicyListPushHead(t *testing.T) {
	l, a, e1, e2, _ := newTestPolicy(t)
	r1, r2 := a.refOf(e1), a.refOf(e2)
	l.pushHead(e1)
	if l.head != r1 || l.tail != r1 || l.count != 1 {
		t.Fatalf("single push: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}
	l.pushHead(e2)
	// e2 <-> e1：e2 是头，e1 是尾。
	if l.head != r2 || l.tail != r1 || l.count != 2 {
		t.Fatalf("second push: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}
	if e2.policyNext != r1 || e1.policyPrev != r2 || e2.policyPrev != 0 || e1.policyNext != 0 {
		t.Fatal("links must point toward head via policyPrev")
	}
	if !e1.inPolicy || !e2.inPolicy {
		t.Fatal("members must be marked inPolicy")
	}
}

func TestPolicyListNextTowardHeadOrWrap(t *testing.T) {
	l, a, e1, e2, e3 := newTestPolicy(t)
	l.pushHead(e1) // [e1]
	l.pushHead(e2) // [e2, e1]
	l.pushHead(e3) // [e3, e2, e1]（e3=头，e1=尾）
	if n := l.nextTowardHeadOrWrap(e1); n != a.refOf(e2) {
		t.Fatalf("tail -> %v, want e2", n)
	}
	if n := l.nextTowardHeadOrWrap(e2); n != a.refOf(e3) {
		t.Fatalf("middle -> %v, want e3", n)
	}
	if n := l.nextTowardHeadOrWrap(e3); n != a.refOf(e1) {
		t.Fatalf("head must wrap to tail, got %v", n)
	}
}

func TestPolicyListRemoveHand(t *testing.T) {
	// hand 删除修正（sieve.go remove 的核心分支）。
	l, a, e1, e2, e3 := newTestPolicy(t)
	r1, r2, r3 := a.refOf(e1), a.refOf(e2), a.refOf(e3)
	l.pushHead(e1)
	l.pushHead(e2)
	l.pushHead(e3) // [e3, e2, e1]

	// 删除 hand=中间：hand 移向头的方向（e2 → e3）。
	l.hand = r2
	l.remove(e2)
	if l.hand != r3 {
		t.Fatalf("hand after middle removal: %v, want e3", l.hand)
	}
	if l.count != 2 || l.head != r3 || l.tail != r1 {
		t.Fatalf("structure after middle removal: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}

	// 删除 hand=头：回绕到尾。
	l.hand = r3
	l.remove(e3)
	if l.hand != r1 {
		t.Fatalf("hand after head removal: %v, want e1 (wrap)", l.hand)
	}

	// 单元素删除 hand 自身：hand 置空。
	if l.count != 1 || l.head != r1 {
		t.Fatalf("single element expected: head=%v count=%d", l.head, l.count)
	}
	l.hand = r1
	l.remove(e1)
	if l.hand != 0 || l.head != 0 || l.tail != 0 || l.count != 0 {
		t.Fatalf("empty list: hand=%v head=%v tail=%v count=%d", l.hand, l.head, l.tail, l.count)
	}
}

func TestPolicyListRemoveNonMemberIsNoop(t *testing.T) {
	l, a, e1, e2, _ := newTestPolicy(t)
	l.pushHead(e1)
	l.remove(e2) // 非成员：不得影响队列
	if l.count != 1 || l.head != a.refOf(e1) {
		t.Fatalf("non-member removal must be noop: count=%d", l.count)
	}
	l.remove(e1) // 已摘除的条目再删一次
	if l.count != 0 {
		t.Fatalf("double removal must be noop: count=%d", l.count)
	}
}
