package ahkc

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// indexFixture 是建立在小 arena 与索引区上的测试夹具。
type indexFixture struct {
	t  *testing.T
	a  *arena
	ix *index
}

func newIndexFixture(t *testing.T, groups, arenaBytes int) *indexFixture {
	t.Helper()
	a, ar := newTestArena(t, uintptr(arenaBytes))
	ir, _ := newRegion(indexRegionBytes(groups), true, false)
	t.Cleanup(func() { ar.release(); ir.release() })
	return &indexFixture{t: t, a: a, ix: newIndex(ir, a, groups)}
}

// entry 在 arena 中分配一个带 key 的条目头（未发布）。
func (f *indexFixture) entry(key string, keyHash uint64, nsID uint32) *entry {
	f.t.Helper()
	ref, ok := f.a.alloc(int(EntryHeaderBytes) + len(key) + 8)
	if !ok {
		f.t.Fatal("alloc")
	}
	e := f.a.entry(ref)
	*e = entry{boundary: e.boundary, keyHash: keyHash, nsID: nsID, keyLen: uint16(len(key))}
	copy(unsafe.Slice((*byte)(e.keyPtr()), len(key)), key)
	return e
}

func (f *indexFixture) publish(e *entry) {
	f.t.Helper()
	slot, ok := f.ix.installSlot(e.keyHash, nil)
	if !ok {
		f.t.Fatal("installSlot failed")
	}
	f.ix.publish(e, slot)
}

func (f *indexFixture) overflow(g uint64) uint32 {
	return uint32(f.ix.words[int(g)*groupWords].Load())
}

// checkOverflow 按全部在册条目重算每组应有的 overflow 并与实际值比对。
func (f *indexFixture) checkOverflow() {
	f.t.Helper()
	want := make([]uint32, f.ix.groups)
	for slot := range f.ix.words {
		e := f.ix.slotEntry(slot)
		if e == nil {
			continue
		}
		if int(e.indexSlot) != slot {
			f.t.Fatalf("entry at slot %d records indexSlot %d", slot, e.indexSlot)
		}
		for g := f.ix.home(e.keyHash); g != uint64(slot/groupWords); g = f.ix.next(g) {
			want[g]++
		}
	}
	for g, w := range want {
		if got := f.overflow(uint64(g)); got != w {
			f.t.Fatalf("group %d overflow = %d, want %d", g, got, w)
		}
	}
}

func TestIndexNamespaceHashCollision(t *testing.T) {
	f := newIndexFixture(t, 4, 1<<16)
	for ns := uint32(0); ns < 2; ns++ {
		f.publish(f.entry("same", 42, ns))
	}
	for ns := uint32(0); ns < 2; ns++ {
		fr := f.ix.find("same", 42, ns)
		if !fr.found || fr.e.nsID != ns {
			t.Fatal("hash collision crossed namespace identity")
		}
	}
	if f.ix.find("same", 42, 2).found {
		t.Fatal("unknown namespace matched a colliding key")
	}
}

// 删除没有墓碑：槽直接清零，途经组的 overflow 减回，空槽可立即复用。
func TestIndexRemoveClearsSlotAndOverflow(t *testing.T) {
	f := newIndexFixture(t, 8, 1<<16)
	for i := 0; i < groupSlots+1; i++ { // 同起点 8 个 key：第 8 个溢出到下一组
		f.publish(f.entry(fmt.Sprintf("k%d", i), 100, 0))
	}
	home := f.ix.home(100)
	if got := f.overflow(home); got != 1 {
		t.Fatalf("home overflow = %d, want 1", got)
	}
	f.checkOverflow()
	last := f.ix.find("k7", 100, 0)
	if !last.found || uint64(last.slot/groupWords) != f.ix.next(home) {
		t.Fatalf("k7 should live in the next group: %+v", last)
	}
	f.ix.remove(last.e)
	if f.ix.find("k7", 100, 0).found || f.ix.words[last.slot].Load() != 0 {
		t.Fatal("removed entry is still visible or its slot is not empty")
	}
	if got := f.overflow(home); got != 0 {
		t.Fatalf("overflow after remove = %d, want 0", got)
	}
	// 腾出的槽直接复用，且未命中时本组 overflow 为 0 即可判定不存在。
	first := f.ix.find("k0", 100, 0)
	f.ix.remove(first.e)
	f.publish(f.entry("again", 100, 0))
	if !f.ix.find("again", 100, 0).found || f.overflow(home) != 0 {
		t.Fatal("freed slot was not reused in the home group")
	}
	f.checkOverflow()
}

// 探测上限：同起点的 key 最多占满 maxGroups 个组，再多就没有安装位。
func TestIndexProbeLimitFull(t *testing.T) {
	f := newIndexFixture(t, 16, 1<<20)
	n := maxGroups * groupSlots
	for i := 0; i < n; i++ {
		f.publish(f.entry(fmt.Sprintf("k%d", i), 100, 0))
	}
	if _, ok := f.ix.installSlot(100, nil); ok {
		t.Fatal("install beyond the probe sequence must fail")
	}
	for i := 0; i < n; i++ {
		if !f.ix.find(fmt.Sprintf("k%d", i), 100, 0).found {
			t.Fatalf("find k%d failed", i)
		}
	}
	if f.ix.find("absent", 100, 0).found {
		t.Fatal("false positive")
	}
	f.checkOverflow()
}

func TestIndexRemovalSetInstall(t *testing.T) {
	f := newIndexFixture(t, 16, 1<<20)
	var es []*entry
	for i := 0; i < maxGroups*groupSlots; i++ {
		e := f.entry(fmt.Sprintf("r%d", i), 55, 0)
		f.publish(e)
		es = append(es, e)
	}
	if _, ok := f.ix.installSlot(55, nil); ok {
		t.Fatal("must fail without removal")
	}
	victim := es[20]
	slot, ok := f.ix.installSlot(55, func(x *entry) bool { return x == victim })
	if !ok || slot != int(victim.indexSlot) {
		t.Fatalf("removal-set install = %d,%v want slot %d", slot, ok, victim.indexSlot)
	}
	if !f.ix.inProbeDomain(55, int(victim.indexSlot)) {
		t.Fatal("victim slot must be inside the probe domain")
	}
	// 移除受害者后用其槽位发布，overflow 保持自洽。
	f.ix.remove(victim)
	n := f.entry("replacement", 55, 0)
	f.ix.publish(n, slot)
	f.checkOverflow()
	if !f.ix.find("replacement", 55, 0).found {
		t.Fatal("replacement not found")
	}
}

func TestIndexProbeDomain(t *testing.T) {
	f := newIndexFixture(t, 32, 1<<12)
	home := f.ix.home(7)
	for d := uint64(0); d < 32; d++ {
		slot := int((home+d)%32)*groupWords + 3
		if got, want := f.ix.inProbeDomain(7, slot), d < maxGroups; got != want {
			t.Fatalf("distance %d: inProbeDomain = %v, want %v", d, got, want)
		}
	}
	small := newIndexFixture(t, 3, 1<<16) // 组数少于 maxGroups：序列内的组互不重复
	if small.ix.probes != 3 {
		t.Fatalf("probes = %d, want 3", small.ix.probes)
	}
	for i := 0; i < 3*groupSlots; i++ {
		small.publish(small.entry(fmt.Sprintf("s%d", i), 9, 0))
	}
	small.checkOverflow()
	if _, ok := small.ix.installSlot(9, nil); ok {
		t.Fatal("full small index must reject")
	}
}

// 长期 churn（装载率钉在 70%，每次先删后插）后：overflow 始终与在册条目一致，全部在册 key 可查，
// 未命中的探测组数进入稳态（实测约 3.0±0.4，与轮换次数无关）。若删除留下墓碑式残留，
// 该值会随轮换涨向探测上限 maxGroups，所以用绝对上界而不是与早期比较（小表上噪声大）。
func TestIndexChurnKeepsOverflowExactAndMissCheap(t *testing.T) {
	const groups = 256
	f := newIndexFixture(t, groups, 4<<20)
	rng := rand.New(rand.NewSource(1))
	live := map[string]*entry{}
	target := groups * groupSlots * 7 / 10
	missGroups := func() float64 {
		visited, probes := 0, 20000
		for i := 0; i < probes; i++ {
			g := f.ix.home(mix64(uint64(1)<<40 + uint64(i)))
			visited++
			for j := 1; j < f.ix.probes && f.overflow(g) != 0; j++ {
				g = f.ix.next(g)
				visited++
			}
		}
		return float64(visited) / float64(probes)
	}
	var worst float64
	rejected := 0
	for step := 0; step < 300000; step++ {
		if len(live) >= target { // 稳态：每插入一条先删除一条
			for k, e := range live {
				f.ix.remove(e)
				f.a.free(f.a.refOf(e))
				delete(live, k)
				break
			}
		}
		k := fmt.Sprintf("key-%d", rng.Intn(1<<20))
		if live[k] != nil {
			continue
		}
		kh := mix64(uint64(rng.Intn(1 << 20)))
		if slot, ok := f.ix.installSlot(kh, nil); ok {
			e := f.entry(k, kh, 0)
			f.ix.publish(e, slot)
			live[k] = e
		} else {
			rejected++
		}
		if step%25013 == 0 {
			f.checkOverflow()
		}
		if step >= 60000 && step%60000 == 0 {
			worst = max(worst, missGroups())
		}
	}
	f.checkOverflow()
	for k, e := range live {
		if fr := f.ix.find(k, e.keyHash, 0); !fr.found || fr.e != e {
			t.Fatalf("live key %s not found", k)
		}
	}
	worst = max(worst, missGroups())
	if worst > 4.5 {
		t.Fatalf("miss probes reached %.2f groups after churn, want a steady state <= 4.5 (probe limit %d)", worst, maxGroups)
	}
	if rejected > 300000/100 {
		t.Fatalf("%d inserts rejected by the probe limit at 70%% load", rejected)
	}
}

// 并发读者与唯一写者：不被删除的 key 永远可查；被反复删插的 key 查到则一定是正确条目。
func TestIndexConcurrentReadersSingleWriter(t *testing.T) {
	const groups, stable, churning = 64, 120, 200
	f := newIndexFixture(t, groups, 8<<20)
	type kv struct {
		key string
		kh  uint64
	}
	var stableKeys, churnKeys []kv
	for i := 0; i < stable; i++ {
		k := kv{fmt.Sprintf("stable-%d", i), mix64(uint64(i))}
		stableKeys = append(stableKeys, k)
		f.publish(f.entry(k.key, k.kh, 0))
	}
	for i := 0; i < churning; i++ {
		churnKeys = append(churnKeys, kv{fmt.Sprintf("churn-%d", i), mix64(uint64(1000 + i))})
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				k := stableKeys[rng.Intn(len(stableKeys))]
				if !f.ix.find(k.key, k.kh, 0).found {
					t.Errorf("stable key %s vanished", k.key)
					return
				}
				c := churnKeys[rng.Intn(len(churnKeys))]
				if fr := f.ix.find(c.key, c.kh, 0); fr.found && fr.e.keyString() != c.key {
					t.Errorf("wrong entry for %s", c.key)
					return
				}
			}
		}(int64(r))
	}
	rng := rand.New(rand.NewSource(99))
	live := map[int]*entry{}
	for step := 0; step < 30000 && f.a.freeBytes.Load() > 1<<20; step++ {
		i := rng.Intn(churning)
		if e := live[i]; e != nil {
			f.ix.remove(e) // 不释放块：读者可能仍在读它（真实实现由 epoch 保护）
			delete(live, i)
			continue
		}
		slot, ok := f.ix.installSlot(churnKeys[i].kh, nil)
		if !ok {
			continue
		}
		e := f.entry(churnKeys[i].key, churnKeys[i].kh, 0)
		f.ix.publish(e, slot)
		live[i] = e
	}
	stop.Store(true)
	wg.Wait()
	f.checkOverflow()
}

// 条目头不含 Go 指针，且与 EntryHeaderBytes 一致、8 字节对齐。
func TestEntryHeaderLayout(t *testing.T) {
	if EntryHeaderBytes%8 != 0 {
		t.Fatalf("header size %d not a multiple of 8", EntryHeaderBytes)
	}
	if unsafe.Offsetof(entry{}.heat)%8 != 0 {
		t.Fatal("64-bit atomic field heat must be 8-byte aligned")
	}
	if unsafe.Offsetof(entry{}.ownedPins)%4 != 0 {
		t.Fatal("32-bit atomic field ownedPins must be 4-byte aligned")
	}
	if EntryHeaderBytes != 88 {
		t.Fatalf("header is %d bytes, want 88; update the documentation that quotes it", EntryHeaderBytes)
	}
	assertPointerFree(t, entry{})
}

// 索引组恰为一条缓存行。
func TestIndexGroupIsOneCacheLine(t *testing.T) {
	if groupWords*8 != cacheLineSize {
		t.Fatalf("group is %d bytes, want %d", groupWords*8, cacheLineSize)
	}
}
