package ahkc

import (
	"context"
	"strings"
	"testing"
	"unsafe"
)

// 三种值布局的往返、对齐、借用视图容量与对象侧表（构造入口校验见 cache_test.go）。

func layoutDrive[V any](c *Cache[V], cfg Config) {
	ex := cfg.Runtime.Executor.(*ManualExecutor)
	for i := 0; i < 50; i++ {
		ex.Drain(1000)
		c.MaintenanceStep()
	}
}

func layoutFill[V any](t *testing.T, c *Cache[V], cfg Config, ns *Namespace[V], key string, v V) {
	t.Helper()
	fut, err := ns.FetchAsync(key, func(context.Context, string) (V, error) { return v, nil })
	if err != nil {
		t.Fatal(err)
	}
	layoutDrive(c, cfg)
	if _, err := fut.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// layoutEntry 返回 key 对应的驻留条目头（测试专用，不 pin）。
func layoutEntry[V any](t *testing.T, c *Cache[V], ns *Namespace[V], key string) *entry {
	t.Helper()
	kh := ns.ns.hashKey(key)
	fr := c.c.shardOf(kh).idx.find(key, kh, ns.ns.id)
	if !fr.found {
		t.Fatalf("%q not resident", key)
	}
	return fr.e
}

type namedString string
type namedBytes []byte

// 字节布局：命名的 string 与 []byte 往返；Get 返回的值归调用方独占；借用视图容量受限；空值可驻留。
func TestLayoutBytesRoundTrip(t *testing.T) {
	cfg := smallConfig(2, 64)
	cs, err := NewBytes[namedString](cfg)
	if err != nil {
		t.Fatal(err)
	}
	nss, _ := cs.Namespace("t")
	layoutFill(t, cs, cfg, nss, "key-1", namedString("hello"))
	layoutFill(t, cs, cfg, nss, "empty", namedString(""))
	if v, ok, err := nss.Get("key-1"); err != nil || !ok || v != "hello" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	if v, ok, err := nss.Get("empty"); err != nil || !ok || v != "" {
		t.Fatalf("empty value: %q %v %v", v, ok, err)
	}
	b, ok, err := nss.GetBorrowed("key-1")
	if err != nil || !ok || b.Value() != "hello" {
		t.Fatalf("borrowed = %v %v", ok, err)
	}
	b.Release()

	cfg2 := smallConfig(2, 64)
	cb, err := NewBytes[namedBytes](cfg2)
	if err != nil {
		t.Fatal(err)
	}
	nsb, _ := cb.Namespace("t")
	layoutFill(t, cb, cfg2, nsb, "k", namedBytes("payload"))
	got, ok, err := nsb.Get("k")
	if err != nil || !ok || string(got) != "payload" {
		t.Fatalf("Get = %q %v %v", got, ok, err)
	}
	got[0] = 'X' // 调用方独占：改它不能影响驻留值
	if again, _, _ := nsb.Get("k"); string(again) != "payload" {
		t.Fatalf("Get result aliases the resident block: %q", again)
	}
	bv, ok, err := nsb.GetBorrowed("k")
	if err != nil || !ok {
		t.Fatalf("borrow: %v %v", ok, err)
	}
	if v := bv.Value(); string(v) != "payload" || cap(v) != len(v) {
		t.Fatalf("borrowed view len=%d cap=%d, want capacity limited to length", len(v), cap(v))
	}
	bv.Release()
}

type flatRec struct {
	A int32
	B [3]uint16
	C float64
}

// 定长布局：无指针结构体与数组按内存映像往返，值段 8 字节对齐（与 key 长度无关）。
func TestLayoutFlatRoundTripAndAlignment(t *testing.T) {
	cfg := smallConfig(2, 64)
	c, err := NewFlat[flatRec](cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	want := flatRec{A: -7, B: [3]uint16{1, 2, 3}, C: 2.5}
	for _, key := range []string{"a", "abc", "abcdefgh", "abcdefghi"} {
		layoutFill(t, c, cfg, ns, key, want)
		if p := uintptr(layoutEntry(t, c, ns, key).valuePtr()); p%8 != 0 {
			t.Fatalf("key %q: value at %#x is not 8-byte aligned", key, p)
		}
		if v, ok, err := ns.Get(key); err != nil || !ok || v != want {
			t.Fatalf("key %q: Get = %+v %v %v", key, v, ok, err)
		}
	}
	if e := layoutEntry(t, c, ns, "a"); int(e.valLen) != int(unsafe.Sizeof(want)) {
		t.Fatalf("valLen = %d, want %d", e.valLen, unsafe.Sizeof(want))
	}

	cfg2 := smallConfig(1, 64)
	ca, err := NewFlat[[5]int64](cfg2)
	if err != nil {
		t.Fatal(err)
	}
	nsa, _ := ca.Namespace("t")
	layoutFill(t, ca, cfg2, nsa, "arr", [5]int64{1, -2, 3, -4, 5})
	if v, ok, _ := nsa.Get("arr"); !ok || v != [5]int64{1, -2, 3, -4, 5} {
		t.Fatalf("array = %v %v", v, ok)
	}
}

type objDoc struct{ body *string }

// 对象布局：块内只有头和 key，值在侧表；槽位按需取用，回收后清零并复用。
func TestLayoutObjectSideTableIsLazyAndReused(t *testing.T) {
	cfg := smallConfig(1, 64)
	c, err := NewObject[*objDoc](cfg, func(key string, _ *objDoc) int64 { return EntryHeaderBytes + int64(len(key)) + 16 })
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	s := c.c.shards[0]
	if s.sideNext != 0 || len(s.sideFree) != 0 || cap(s.sideFree) != len(s.side) {
		t.Fatalf("side table touched at creation: next=%d free=%d cap=%d slots=%d", s.sideNext, len(s.sideFree), cap(s.sideFree), len(s.side))
	}
	body := "payload"
	d1 := &objDoc{body: &body}
	layoutFill(t, c, cfg, ns, "k1", d1)
	if s.sideNext != 1 {
		t.Fatalf("sideNext = %d after one fill", s.sideNext)
	}
	if v, ok, _ := ns.Get("k1"); !ok || v != d1 {
		t.Fatal("object identity lost")
	}
	if e := layoutEntry(t, c, ns, "k1"); int(e.valLen) != 0 || s.side[e.valLen] != d1 {
		t.Fatalf("slot %d does not hold the object", e.valLen)
	}
	h, err := ns.InvalidateAsync("k1")
	if err != nil {
		t.Fatal(err)
	}
	layoutDrive(c, cfg)
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && c.Stats().RetiredPending != 0; i++ {
		layoutDrive(c, cfg)
	}
	if len(s.sideFree) != 1 || s.side[0] != nil {
		t.Fatalf("retired slot not recycled and cleared: free=%d slot0=%v", len(s.sideFree), s.side[0])
	}
	layoutFill(t, c, cfg, ns, "k2", &objDoc{body: &body})
	if s.sideNext != 1 || len(s.sideFree) != 0 {
		t.Fatalf("recycled slot not reused: next=%d free=%d", s.sideNext, len(s.sideFree))
	}
}

// 侧表耗尽按分配失败优雅拒绝：不 panic，不移除任何条目，业务结果照常交付。
func TestLayoutObjectSideTableExhaustionRejectsFill(t *testing.T) {
	cfg := smallConfig(1, 64)
	c, err := NewObject[string](cfg, func(key string, v string) int64 { return EntryHeaderBytes + int64(len(key)) + 8 })
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	s := c.c.shards[0]
	s.sideNext = uint32(len(s.side)) // 视为全部槽位已被占用
	fut, err := ns.FetchAsync("k", func(context.Context, string) (string, error) { return strings.Repeat("v", 3), nil })
	if err != nil {
		t.Fatal(err)
	}
	layoutDrive(c, cfg)
	if v, err := fut.Wait(context.Background()); err != nil || v != "vvv" {
		t.Fatalf("result = %q %v, want delivered despite the rejected fill", v, err)
	}
	if st := c.Stats(); st.ResidentEntries != 0 || st.FillRejected["ALLOCATION_FAILED"] != 1 || st.ArenaUsedBytes != 0 {
		t.Fatalf("stats after exhausted side table: %+v", st)
	}
}
