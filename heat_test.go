package ahkc

import (
	"math"
	"testing"
)

func TestHeatPackUnpack(t *testing.T) {
	s := heatSnap{epoch: 12345, cur: 7, prev: 9}
	got := unpackHeat(packHeat(s))
	if got != s {
		t.Fatalf("roundtrip: want %+v got %+v", s, got)
	}
}

func TestNormalizeHeat(t *testing.T) {
	s := heatSnap{epoch: 10, cur: 5, prev: 3}
	// 同桶
	n, ok := normalizeHeat(s, 10)
	if !ok || n != s {
		t.Fatalf("same bucket: %+v ok=%v", n, ok)
	}
	// 跨一桶：current 移入 previous
	n, ok = normalizeHeat(s, 11)
	if !ok || n.epoch != 11 || n.cur != 0 || n.prev != 5 {
		t.Fatalf("next bucket: %+v ok=%v", n, ok)
	}
	// 跨两桶以上：清零
	n, ok = normalizeHeat(s, 13)
	if !ok || n.epoch != 13 || n.cur != 0 || n.prev != 0 {
		t.Fatalf("skip buckets: %+v ok=%v", n, ok)
	}
	// 时间落后：UNKNOWN，禁止回退
	if _, ok = normalizeHeat(s, 9); ok {
		t.Fatal("stale time must be UNKNOWN")
	}
}

func TestHeatAddSaturation(t *testing.T) {
	var c heatCell
	const hcap = 4
	for i := 0; i < 4; i++ {
		if r := c.add(100, hcap, 2); r != heatUpdated {
			t.Fatalf("add %d: %v", i, r)
		}
	}
	if r := c.add(100, hcap, 2); r != heatSaturated {
		t.Fatalf("expect saturated, got %v", r)
	}
	// 换桶后可重新计数
	if r := c.add(101, hcap, 2); r != heatUpdated {
		t.Fatalf("after bucket shift: %v", r)
	}
	s := unpackHeat(c.v.Load())
	if s.epoch != 101 || s.cur != 1 || s.prev != 4 {
		t.Fatalf("shift state: %+v", s)
	}
}

func TestHeatReadWeighted(t *testing.T) {
	var c heatCell
	// 桶 10 计 8 次，桶 11 计 2 次。
	for i := 0; i < 8; i++ {
		c.add(10, 64, 2)
	}
	for i := 0; i < 2; i++ {
		c.add(11, 64, 2)
	}
	// phi=0.5：H = 2 + 0.5*8 = 6
	h, ok := c.read(11, 0.5)
	if !ok || math.Abs(h-6) > 1e-9 {
		t.Fatalf("weighted read: %v ok=%v", h, ok)
	}
	// phi=0：H = 2 + 8 = 10
	h, _ = c.read(11, 0)
	if math.Abs(h-10) > 1e-9 {
		t.Fatalf("phi=0: %v", h)
	}
}

func TestHeatGateMinAndRows(t *testing.T) {
	g := newHeatGate(4, 256)
	key := uint64(0xdeadbeef)
	for i := 0; i < 5; i++ {
		g.add(key, 7, 64, 2)
	}
	h, ok := g.read(key, 7, 0)
	if !ok || h < 5 {
		t.Fatalf("gate read: %v ok=%v", h, ok)
	}
	// 未访问 key 热度为 0。
	h, ok = g.read(0x1234, 7, 0)
	if !ok || h != 0 {
		t.Fatalf("cold key: %v ok=%v", h, ok)
	}
}

func TestHeatAddSkipsStaleTime(t *testing.T) {
	var c heatCell
	c.add(100, 64, 2)
	// 更旧时间的请求不放弃已有状态，也不回退 epoch。
	if r := c.add(99, 64, 2); r != heatSkipped {
		t.Fatalf("stale add: %v", r)
	}
	s := unpackHeat(c.v.Load())
	if s.epoch != 100 || s.cur != 1 {
		t.Fatalf("state rolled back: %+v", s)
	}
}
