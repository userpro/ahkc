package ahkc

import (
	"math/rand"
	"reflect"
	"testing"
	"unsafe"
)

func newTestArena(t testing.TB, bytes uintptr) (*arena, *region) {
	t.Helper()
	r, _ := newRegion(bytes, true, false)
	return newArena(r.base, r.size), r
}

// 随机 alloc/free 对照模型：已分配块互不重叠、账目守恒、全部释放后合并回单个空闲块。
func TestArenaRandomAgainstModel(t *testing.T) {
	const size = 1 << 20
	a, r := newTestArena(t, size)
	defer r.release()
	total := a.freeBytes.Load()
	rng := rand.New(rand.NewSource(1))
	type blk struct {
		ref   blockRef
		units uint32
	}
	var live []blk
	owner := make(map[uint32]int) // 单元 → 块序号，检查不重叠
	var seq int
	check := func() {
		if got := a.usedBytes.Load() + a.freeBytes.Load(); got != total {
			t.Fatalf("bytes not conserved: used+free=%d want %d", got, total)
		}
		// 最大空闲块不超过任一真实空闲块的上界：遍历空闲链取真实最大值，镜像须在其 1/16 内。
		var truth int64
		for fl := range a.heads {
			for sl := range a.heads[fl] {
				for r := a.heads[fl][sl]; r != 0; r = blockRef(*a.links(r)) {
					truth = max(truth, int64(uint32(*a.word(r)))*arenaUnit)
				}
			}
		}
		if got := a.largestFree.Load(); got > truth || got < truth-truth/16 {
			t.Fatalf("largestFree=%d, true largest free block %d", got, truth)
		}
	}
	for i := 0; i < 20000; i++ {
		if len(live) > 0 && (rng.Intn(3) == 0 || len(live) > 400) {
			j := rng.Intn(len(live))
			b := live[j]
			live[j] = live[len(live)-1]
			live = live[:len(live)-1]
			for u := b.ref; u < b.ref+blockRef(b.units); u++ {
				delete(owner, uint32(u))
			}
			a.free(b.ref)
		} else {
			n := 24 + rng.Intn(2000)
			ref, ok := a.alloc(n)
			if !ok {
				continue
			}
			units := uint32(*a.word(ref))
			if int(units)*arenaUnit < n {
				t.Fatalf("block too small: %d units for %d bytes", units, n)
			}
			seq++
			for u := ref; u < ref+blockRef(units); u++ {
				if _, dup := owner[uint32(u)]; dup {
					t.Fatalf("overlap at unit %d", u)
				}
				owner[uint32(u)] = seq
			}
			// 写满整个块，检查没有越界破坏边界字。
			p := a.at(ref)
			for off := uintptr(8); off < uintptr(units)*arenaUnit; off += 8 {
				*(*uint64)(unsafe.Add(p, off)) = ^uint64(0)
			}
			live = append(live, blk{ref, units})
		}
		check()
	}
	for _, b := range live {
		a.free(b.ref)
	}
	check()
	if a.usedBytes.Load() != 0 {
		t.Fatalf("leak: used=%d", a.usedBytes.Load())
	}
	// 完全合并：区首块是覆盖整个区的单个空闲块。
	if w := *a.word(1); w&blockFree == 0 || int64(uint32(w))*arenaUnit != total {
		t.Fatalf("not fully coalesced: first block header %#x, free %d", w, total)
	}
	if a.largestFree.Load() != total {
		t.Fatalf("largestFree=%d after full coalescing, want %d", a.largestFree.Load(), total)
	}
	// TLSF 向上取整分级：紧贴整块大小的请求可能落在更高分级而失败，四分之三总能满足。
	if ref, ok := a.alloc(int(total) * 3 / 4); !ok {
		t.Fatal("large allocation failed after full coalescing")
	} else {
		a.free(ref)
	}
}

func TestArenaFullThenRecover(t *testing.T) {
	a, r := newTestArena(t, 4096)
	defer r.release()
	var refs []blockRef
	for {
		ref, ok := a.alloc(100)
		if !ok {
			break
		}
		refs = append(refs, ref)
	}
	if len(refs) < 30 {
		t.Fatalf("expected many small blocks, got %d", len(refs))
	}
	if _, ok := a.alloc(100); ok {
		t.Fatal("alloc must fail when full")
	}
	// 交错释放，再合并出大块。
	for i := 0; i < len(refs); i += 2 {
		a.free(refs[i])
	}
	for i := 1; i < len(refs); i += 2 {
		a.free(refs[i])
	}
	if _, ok := a.alloc(3000); !ok {
		t.Fatal("coalescing failed to rebuild a large block")
	}
}

func TestArenaRefsAreNonZeroAndAligned(t *testing.T) {
	a, r := newTestArena(t, 1<<16)
	defer r.release()
	for i := 0; i < 100; i++ {
		ref, ok := a.alloc(64)
		if !ok || ref == 0 {
			t.Fatalf("ref=%d ok=%v", ref, ok)
		}
		if uintptr(a.at(ref))%8 != 0 {
			t.Fatal("block not 8-byte aligned")
		}
	}
}

// region 的两个后端都能读写；prefault 与 release 不崩溃。
func TestRegionBackends(t *testing.T) {
	for _, forceHeap := range []bool{true, false} {
		r, _ := newRegion(1<<20, forceHeap, false)
		*(*uint64)(unsafe.Add(r.base, 1<<19)) = 42
		r.prefault(1 << 20)
		if *(*uint64)(unsafe.Add(r.base, 1<<19)) != 42 {
			t.Fatal("region lost a write")
		}
		r.release()
		if r.base != nil {
			t.Fatal("release must clear the base")
		}
	}
}

// assertPointerFree 断言类型的所有字段（递归）都不含 Go 指针：驻留结构必须对 GC 不可见。
func assertPointerFree(t *testing.T, v any) {
	t.Helper()
	var walk func(rt reflect.Type, path string)
	walk = func(rt reflect.Type, path string) {
		switch rt.Kind() {
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64:
		case reflect.Array:
			walk(rt.Elem(), path+"[]")
		case reflect.Struct:
			for i := 0; i < rt.NumField(); i++ {
				walk(rt.Field(i).Type, path+"."+rt.Field(i).Name)
			}
		default:
			t.Fatalf("%s: %s may hold a Go pointer", path, rt)
		}
	}
	walk(reflect.TypeOf(v), reflect.TypeOf(v).Name())
}
