package ahkc

import (
	"fmt"
	"testing"
)

func TestIndexNamespaceHashCollision(t *testing.T) {
	ix := newIndex[string](8, 4)
	for ns := uint32(0); ns < 2; ns++ {
		e := &entry[string]{key: "same", keyHash: 42, nsID: ns}
		slot, ok := ix.installSlot(e.keyHash, nil)
		if !ok {
			t.Fatal("install")
		}
		ix.publish(e, slot)
	}
	for ns := uint32(0); ns < 2; ns++ {
		fr := ix.find("same", 42, ns)
		if !fr.found || fr.e.nsID != ns {
			t.Fatal("hash collision crossed namespace identity")
		}
	}
	if ix.find("same", 42, 2).found {
		t.Fatal("unknown namespace matched a colliding key")
	}
}

func TestIndexFindAbsentAfterRemove(t *testing.T) {
	ix := newIndex[string](64, 8)
	pool := newEntryPool[string](4)
	e, _ := pool.tryGet()
	e.key, e.keyHash = "k1", 1234
	slot, ok := ix.installSlot(e.keyHash, nil)
	if !ok {
		t.Fatal("installSlot failed")
	}
	ix.publish(e, slot)
	fr := ix.find("k1", 1234, 0)
	if !fr.found || fr.e != e {
		t.Fatal("find after publish failed")
	}
	ix.remove(e)
	if ix.find("k1", 1234, 0).found {
		t.Fatal("found after remove")
	}
	// TOMBSTONE 不能被清成 EMPTY：槽位仍是 tomb。
	if ix.slots[e.indexSlot].Load() == nil {
		t.Fatal("tombstone cleared to EMPTY")
	}
	// tomb 槽位可以再安装。
	slot2, ok := ix.installSlot(e.keyHash, nil)
	if !ok {
		t.Fatal("reinstall over tombstone failed")
	}
	_ = slot2
}

func TestIndexProbeLimitFull(t *testing.T) {
	ix := newIndex[string](8, 4)
	pool := newEntryPool[string](8)
	// 插入 4 个同 probe 起点的条目后，第 5 个同起点 key 无安装位。
	// 构造 keyHash 相同的不同 key（完整 key 比较区分）。
	for i := 0; i < 4; i++ {
		e, _ := pool.tryGet()
		e.key, e.keyHash = fmt.Sprintf("k%d", i), 100 // 同一探测域
		slot, ok := ix.installSlot(100, nil)
		if !ok {
			t.Fatalf("install %d failed", i)
		}
		ix.publish(e, slot)
	}
	if _, ok := ix.installSlot(100, nil); ok {
		t.Fatal("install beyond probeLimit must fail")
	}
	// 找到全部已发布条目。
	for i := 0; i < 4; i++ {
		if !ix.find(fmt.Sprintf("k%d", i), 100, 0).found {
			t.Fatalf("find k%d failed", i)
		}
	}
	// ABSENT 是完整探测后的确定结论。
	if ix.find("absent", 100, 0).found {
		t.Fatal("false positive")
	}
}

func TestIndexRemovalSetInstall(t *testing.T) {
	ix := newIndex[string](8, 4)
	pool := newEntryPool[string](4)
	// 用同探测域的 4 个条目占满 probeLimit。
	var es []*entry[string]
	for i := 0; i < 4; i++ {
		e, _ := pool.tryGet()
		e.key, e.keyHash = fmt.Sprintf("r%d", i), 55
		slot, ok := ix.installSlot(55, nil)
		if !ok {
			t.Fatalf("install %d failed", i)
		}
		ix.publish(e, slot)
		es = append(es, e)
	}
	// 探测域已满：无移除计划时装不下。
	if _, ok := ix.installSlot(55, nil); ok {
		t.Fatal("must fail without removal")
	}
	// 移除集合内的条目可让出槽位。
	victim := es[2]
	if _, ok := ix.installSlot(55, func(x *entry[string]) bool { return x == victim }); !ok {
		t.Fatal("removal-set install failed")
	}
}

func TestEntryPoolExhaustion(t *testing.T) {
	p := newEntryPool[string](2)
	a, ok1 := p.tryGet()
	b, ok2 := p.tryGet()
	if !ok1 || !ok2 {
		t.Fatal("pool get failed")
	}
	if _, ok := p.tryGet(); ok {
		t.Fatal("pool must report exhaustion")
	}
	p.put(a)
	if _, ok := p.tryGet(); !ok {
		t.Fatal("pool reuse failed")
	}
	p.put(b)
}
