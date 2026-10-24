package ahkc

import "testing"

// SIEVE 策略队列（sieve.go）纯数据结构单元测试。
// 正交说明：cache_test.go 的替换测试覆盖队列在真实填充中的行为；
// 本文件聚焦链接结构与 hand 修正的每个分支（白盒、确定性）。

func mkPolicyEntries() (e1, e2, e3 *entry[string]) {
	return &entry[string]{key: "e1"}, &entry[string]{key: "e2"}, &entry[string]{key: "e3"}
}

func TestPolicyListPushHead(t *testing.T) {
	var l policyList[string]
	e1, e2, _ := mkPolicyEntries()
	l.pushHead(e1)
	if l.head != e1 || l.tail != e1 || l.count != 1 {
		t.Fatalf("single push: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}
	l.pushHead(e2)
	// e2 <-> e1：e2 是头，e1 是尾。
	if l.head != e2 || l.tail != e1 || l.count != 2 {
		t.Fatalf("second push: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}
	if e2.policyNext != e1 || e1.policyPrev != e2 || e2.policyPrev != nil || e1.policyNext != nil {
		t.Fatal("links must point toward head via policyPrev")
	}
	if !e1.inPolicy || !e2.inPolicy {
		t.Fatal("members must be marked inPolicy")
	}
}

func TestPolicyListNextTowardHeadOrWrap(t *testing.T) {
	var l policyList[string]
	e1, e2, e3 := mkPolicyEntries()
	l.pushHead(e1) // [e1]
	l.pushHead(e2) // [e2, e1]
	l.pushHead(e3) // [e3, e2, e1]（e3=头，e1=尾）
	if n := l.nextTowardHeadOrWrap(e1); n != e2 {
		t.Fatalf("tail -> %v, want e2", n)
	}
	if n := l.nextTowardHeadOrWrap(e2); n != e3 {
		t.Fatalf("middle -> %v, want e3", n)
	}
	if n := l.nextTowardHeadOrWrap(e3); n != e1 {
		t.Fatalf("head must wrap to tail, got %v", n)
	}
}

func TestPolicyListRemoveHand(t *testing.T) {
	// hand 删除修正（sieve.go remove 的核心分支）。
	var l policyList[string]
	e1, e2, e3 := mkPolicyEntries()
	l.pushHead(e1)
	l.pushHead(e2)
	l.pushHead(e3) // [e3, e2, e1]

	// 删除 hand=中间：hand 移向头的方向（e2 → e3）。
	l.hand = e2
	l.remove(e2)
	if l.hand != e3 {
		t.Fatalf("hand after middle removal: %v, want e3", l.hand)
	}
	if l.count != 2 || l.head != e3 || l.tail != e1 {
		t.Fatalf("structure after middle removal: head=%v tail=%v count=%d", l.head, l.tail, l.count)
	}

	// 删除 hand=头：回绕到尾。
	l.hand = e3
	l.remove(e3)
	if l.hand != e1 {
		t.Fatalf("hand after head removal: %v, want e1 (wrap)", l.hand)
	}

	// 单元素删除 hand 自身：hand 置空。
	if l.count != 1 || l.head != e1 {
		t.Fatalf("single element expected: head=%v count=%d", l.head, l.count)
	}
	l.hand = e1
	l.remove(e1)
	if l.hand != nil || l.head != nil || l.tail != nil || l.count != 0 {
		t.Fatalf("empty list: hand=%v head=%v tail=%v count=%d", l.hand, l.head, l.tail, l.count)
	}
}

func TestPolicyListRemoveNonMemberIsNoop(t *testing.T) {
	var l policyList[string]
	e1, e2, _ := mkPolicyEntries()
	l.pushHead(e1)
	l.remove(e2) // 非成员：不得影响队列
	if l.count != 1 || l.head != e1 {
		t.Fatalf("non-member removal must be noop: count=%d", l.count)
	}
	l.remove(e1) // 已摘除的条目再删一次
	if l.count != 0 {
		t.Fatalf("double removal must be noop: count=%d", l.count)
	}
}
