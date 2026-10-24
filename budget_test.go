package ahkc

import (
	"sync"
	"testing"
)

func TestBudgetReserveReturnOnce(t *testing.T) {
	b := newBudget(100)
	r, fr := b.tryReserve(40)
	if fr != failOK || r == nil {
		t.Fatal("reserve failed")
	}
	if b.available() != 60 {
		t.Fatalf("avail=%d", b.available())
	}
	r.returnOnce()
	r.returnOnce() // 幂等：只归还一次
	if b.available() != 100 {
		t.Fatalf("avail after double return=%d", b.available())
	}
}

func TestBudgetFull(t *testing.T) {
	b := newBudget(10)
	if _, fr := b.tryReserve(11); fr != failFull {
		t.Fatalf("want failFull, got %v", fr)
	}
	r, _ := b.tryReserve(10)
	if _, fr := b.tryReserve(1); fr != failFull {
		t.Fatalf("want failFull, got %v", fr)
	}
	r.returnOnce()
	if _, fr := b.tryReserve(10); fr != failOK {
		t.Fatalf("want ok after return, got %v", fr)
	}
}

func TestBudgetNeverNegativeConcurrent(t *testing.T) {
	b := newBudget(64)
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if r, fr := b.tryReserve(1); fr == failOK {
					r.returnOnce()
				}
			}
		}()
	}
	wg.Wait()
	if b.available() != 64 {
		t.Fatalf("conservation broken: avail=%d", b.available())
	}
}

// TestReserveIntoReusesReceiptStorage：原位预留可反复复用同一块凭证存储（池化退休记录依赖这一点）：
// 每次预留重置结算标记，归还各至多一次，额度守恒。
func TestReserveIntoReusesReceiptStorage(t *testing.T) {
	b := newBudget(10)
	var r receipt
	for i := 0; i < 3; i++ {
		if fr := b.tryReserveInto(&r, 7); fr != failOK {
			t.Fatalf("round %d: reserve failed: %v", i, fr)
		}
		if b.available() != 3 {
			t.Fatalf("round %d: avail=%d want 3", i, b.available())
		}
		r.returnOnce()
		r.returnOnce() // 重复归还无效
		if b.available() != 10 {
			t.Fatalf("round %d: avail=%d want 10 after settle", i, b.available())
		}
	}
	if fr := b.tryReserveInto(&r, 11); fr != failFull || b.available() != 10 {
		t.Fatalf("over-limit reserve: %v avail=%d", fr, b.available())
	}
	if !b.reserveUnitInto(&r) || b.available() != 9 {
		t.Fatal("unit reserve")
	}
	r.returnOnce()
	for i := 0; i < 10; i++ {
		var u receipt
		if !b.reserveUnitInto(&u) {
			t.Fatalf("unit %d should fit", i)
		}
	}
	var over receipt
	if b.reserveUnitInto(&over) || b.available() != 0 {
		t.Fatalf("unit over-reserve must fail without leaking: avail=%d", b.available())
	}
}

func newBudget(limit int64) *budget {
	b := &budget{}
	b.init(limit)
	return b
}

// tryReserve 是测试用的堆凭证形式，便于逐项断言。
func (b *budget) tryReserve(amount int64) (*receipt, failReason) {
	r := &receipt{}
	if fr := b.tryReserveInto(r, amount); fr != failOK {
		return nil, fr
	}
	return r, failOK
}
