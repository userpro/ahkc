package ahkc

import (
	"sync"
	"testing"
	"unsafe"
)

func TestEpochPinUnpin(t *testing.T) {
	d := newEpochDomain(4)
	g, ok := d.pin()
	if !ok {
		t.Fatal("pin failed")
	}
	g.unpin()
	// 全部槽占用后 BUSY。
	var gs []guard
	for i := 0; i < 4; i++ {
		g, ok := d.pin()
		if !ok {
			t.Fatalf("pin %d failed", i)
		}
		gs = append(gs, g)
	}
	if _, ok := d.pin(); ok {
		t.Fatal("pin must be BUSY when slots exhausted")
	}
	for _, g := range gs {
		g.unpin()
	}
	if _, ok := d.pin(); !ok {
		t.Fatal("pin after unpin failed")
	}
}

func TestEpochAdvanceBlockedByReader(t *testing.T) {
	d := newEpochDomain(2)
	g, ok := d.pin()
	if !ok {
		t.Fatal("pin failed")
	}
	defer g.unpin()
	// 活跃读者在 scanEpoch：不阻塞（v-1 < scanEpoch 才阻塞）。
	e0 := d.epoch.Load()
	for i := 0; i < 4; i++ {
		d.advanceStep(16)
	}
	if d.epoch.Load() <= e0 {
		t.Fatal("epoch must advance when reader is current")
	}
	// 读者仍持有旧 epoch（pin 在更早代际）：阻塞推进。
	d2 := newEpochDomain(2)
	gg, _ := d2.pin()
	d2.epoch.Store(d2.epoch.Load() + 1) // 模拟推进，使读者变旧
	before := d2.epoch.Load()
	for i := 0; i < 4; i++ {
		d2.advanceStep(16)
	}
	if d2.epoch.Load() != before {
		t.Fatal("epoch advanced despite old reader")
	}
	gg.unpin()
	d2.advanceStep(16) // 一次完整扫描推进一个 epoch
	if d2.epoch.Load() != before+1 {
		t.Fatal("epoch must advance once after reader exits")
	}
}

func TestEpochSafeForReclaim(t *testing.T) {
	d := newEpochDomain(1)
	if d.safeForReclaim(0) {
		t.Fatal("retire at 0 must wait two epochs")
	}
	d.advanceStep(1)
	d.advanceStep(1)
	if !d.safeForReclaim(0) {
		t.Fatal("two advances required")
	}
}

func TestEpochConcurrentPinUnpin(t *testing.T) {
	d := newEpochDomain(16)
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if gr, ok := d.pin(); ok {
					gr.unpin()
				}
			}
		}()
	}
	wg.Wait()
	// 所有槽必须回到 INACTIVE。
	for i := range d.slots {
		if d.slots[i].v.Load() != epochSlotInactive {
			t.Fatalf("slot %d leaked", i)
		}
	}
}

// TestEpochSlotsOwnCacheLine：每个保护槽独占一条缓存行，且 epoch 与槽表头不共享缓存行，
// 防止相邻读者 pin/unpin 的写入互相使对方失效（命中路径的伪共享回归）。
func TestEpochSlotsOwnCacheLine(t *testing.T) {
	if got := unsafe.Sizeof(epochSlot{}); got != cacheLineSize {
		t.Fatalf("epochSlot size=%d, want %d", got, cacheLineSize)
	}
	if got := unsafe.Offsetof(epochDomain{}.slots); got < cacheLineSize {
		t.Fatalf("slots header offset=%d shares epoch's cache line", got)
	}
}
