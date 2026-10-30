package ahkc

import (
	"strings"
	"unsafe"
)

// 值布局：由构造函数在创建时确定（cache.layout），运行期按小枚举 switch 分派，不做反射。
//
//   - layoutBytes：string / []byte 的内容原始字节存放在块内。
//   - layoutFlat：无指针定长类型的内存映像存放在块内（8 字节对齐）。
//   - layoutObject：块内只有头和 key；值留在分片侧表 []V（会被 GC 扫描），头里记槽号。
//   - 影子模式：任何布局都只存头和 key，命中从不交付值。

// carriesValue 报告块内是否带值段。
func (c *cache[V]) carriesValue() bool {
	return !c.cfg.shadow() && c.layout != layoutObject
}

// valueLen 返回候选值写入块内的字节数（对象布局与影子模式为 0）。
func (c *cache[V]) valueLen(v V) int {
	if !c.carriesValue() {
		return 0
	}
	if c.layout == layoutFlat {
		return int(unsafe.Sizeof(v))
	}
	if c.bytesIsString {
		return len(*(*string)(unsafe.Pointer(&v)))
	}
	return len(*(*[]byte)(unsafe.Pointer(&v)))
}

// blockSize 是条目块的物理大小：头 + 8 字节对齐 key + 8 字节对齐 value。
func (c *cache[V]) blockSize(keyLen, valLen int) int {
	return int(blockBytes(keyLen, int64(valLen)))
}

// writeValue 把候选值写入已分配的块（头与 key 已写好）。对象布局占用侧表槽；
// 侧表耗尽返回 false，调用方负责释放块。
func (c *cache[V]) writeValue(s *shard[V], e *entry, v V) bool {
	switch {
	case c.cfg.shadow():
		return true
	case c.layout == layoutObject:
		var slot uint32
		switch n := len(s.sideFree); {
		case n > 0:
			slot = s.sideFree[n-1]
			s.sideFree = s.sideFree[:n-1]
		case int(s.sideNext) < len(s.side):
			slot = s.sideNext
			s.sideNext++
		default:
			return false
		}
		s.side[slot] = v
		e.valLen = slot
	case c.layout == layoutFlat:
		*(*V)(e.valuePtr()) = v
	case c.bytesIsString:
		str := *(*string)(unsafe.Pointer(&v))
		copy(unsafe.Slice((*byte)(e.valuePtr()), len(str)), str)
	default:
		b := *(*[]byte)(unsafe.Pointer(&v))
		copy(unsafe.Slice((*byte)(e.valuePtr()), len(b)), b)
	}
	return true
}

// view 返回条目值。字节布局是块内的零拷贝视图（容量受限），只在条目被 pin 住期间有效；
// 其余布局是值拷贝。
func (c *cache[V]) view(e *entry) V {
	switch c.layout {
	case layoutFlat:
		return *(*V)(e.valuePtr())
	case layoutObject:
		return c.shards[e.entryID>>48].side[e.valLen]
	}
	n := int(e.valLen)
	if c.bytesIsString {
		s := unsafe.String((*byte)(e.valuePtr()), n)
		return *(*V)(unsafe.Pointer(&s))
	}
	b := unsafe.Slice((*byte)(e.valuePtr()), n)[:n:n]
	return *(*V)(unsafe.Pointer(&b))
}

// copyOut 返回调用方独占的值：字节布局拷贝一次，其余布局与 view 相同。
func (c *cache[V]) copyOut(e *entry) V {
	if c.layout != layoutBytes {
		return c.view(e)
	}
	n := int(e.valLen)
	if c.bytesIsString {
		s := strings.Clone(unsafe.String((*byte)(e.valuePtr()), n))
		return *(*V)(unsafe.Pointer(&s))
	}
	b := append(make([]byte, 0, n), unsafe.Slice((*byte)(e.valuePtr()), n)...)
	return *(*V)(unsafe.Pointer(&b))
}

// freeBlock 归还条目块（及对象布局的侧表槽）。调用方保证无读者可达该块。
func (c *cache[V]) freeBlock(s *shard[V], e *entry) {
	if c.layout == layoutObject && !c.cfg.shadow() {
		var zero V
		s.side[e.valLen] = zero
		s.sideFree = append(s.sideFree, e.valLen)
	}
	s.arena.free(s.arena.refOf(e))
}

// initBlock 在已分配的块上写入头、key 与值。
func (c *cache[V]) initBlock(s *shard[V], ref blockRef, a *fillCandidate[V], valLen int) (*entry, bool) {
	e := s.arena.entry(ref)
	k := &a.e
	e.keyHash, e.entryID, e.hardDeadline = k.keyHash, k.entryID, k.hardDeadline
	e.billedBytes, e.nsGen, e.nsID, e.cost = uint32(k.billedBytes), uint32(k.nsGen), k.nsID, k.cost
	e.keyLen = uint16(len(k.key))
	e.valLen = uint32(valLen)
	copy(unsafe.Slice((*byte)(e.keyPtr()), len(k.key)), k.key)
	if !c.writeValue(s, e, k.value) {
		return nil, false
	}
	e.flags.Store(0)
	e.ownedPins.Store(0)
	e.heat.v.Store(packHeat(heatSnap{epoch: bucketIndex(c.now(), int64(c.cfg.Heat.Delta))}))
	e.indexSlot = noSlot
	e.inPolicy = false
	e.policyPrev, e.policyNext = 0, 0
	return e, true
}
