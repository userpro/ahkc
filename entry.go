package ahkc

import (
	"sync/atomic"
	"unsafe"
)

// entry 是缓存条目的头部，叠加在 arena 块的起始位置；块布局为
// [头][key，补齐到 8 字节][value]。头部不含 Go 指针，因此整个条目区对 GC 不可见。
//
// 字段分三区：
//   - 身份区：发布后不可变（keyHash/entryID/nsID/nsGen/hardDeadline/billedBytes/cost/keyLen/valLen）。
//     nsGen、billedBytes、retireEpoch 在头里只存 32 位，换每条目省 16 字节（亿级约 1.6GB）：
//     billedBytes 由 MaxResultBytes <= 4GiB 的配置校验保证放得下；nsGen 与 retireEpoch 的回绕见各自的比较函数。
//   - 请求区：请求线程可更新（flags 里的 readable 只由有效变无效、heat、visited、ownedPins）。
//   - 维护区：仅分片维护者修改（indexSlot/policy 链接/退休链）。
//
// 读者只能在 epoch pin 内、通过索引取得块引用后访问头部；块在退休并经 epoch 确认安全后才被复用。
type entry struct {
	boundary uint64 // arena 边界字，arena 独占，读者不得访问

	keyHash      uint64
	entryID      uint64 // 高 16 位为分片号
	hardDeadline int64

	heat heatCell

	billedBytes uint32
	nsGen       uint32 // 命名空间代际的低 32 位，比较用 genIs
	nsID        uint32
	ownedPins   atomic.Int32
	cost        float32 // 单次回源成本，缩放收益；默认 1

	indexSlot  uint32
	valLen     uint32   // 字节/定长布局为值字节数；对象布局为侧表槽号
	policyPrev blockRef // 更靠近头；退休后 policyNext 复用为退休链 next
	policyNext blockRef

	retireEpoch uint32 // 退休时 epoch 的低 32 位，比较用 safeForReclaim

	keyLen   uint16
	inPolicy bool          // 仅维护者
	flags    atomic.Uint32 // flagReadable、flagVisited
}

const (
	flagReadable uint32 = 1 << iota
	flagVisited
)

func (e *entry) isReadable() bool { return e.flags.Load()&flagReadable != 0 }

func (e *entry) setReadable(on bool) {
	if on {
		e.flags.Or(flagReadable)
	} else {
		e.flags.And(^flagReadable)
	}
}

// markVisited 置访问位；已置位时只读不写，免得热 key 反复写同一缓存行。
func (e *entry) markVisited() {
	if e.flags.Load()&flagVisited == 0 {
		e.flags.Or(flagVisited)
	}
}

// takeVisited 清访问位并返回清除前的值。
func (e *entry) takeVisited() bool { return e.flags.And(^flagVisited)&flagVisited != 0 }

// genIs 报告条目是否属于命名空间当前代际。
//
// shortcut: 头里只存代际低 32 位。条目要被误判有效，需要在它被读到或被慢扫描（默认 5 分钟）
// 退休之前，该命名空间恰好被失效 2^32 的整数倍次，实际不可能；需要更强保证时把 nsGen 改回 64 位。
func (e *entry) genIs(gen uint64) bool { return e.nsGen == uint32(gen) }

// EntryHeaderBytes 是每个条目的固定头部大小。NewObject 的计费函数应把它计入返回值。
const EntryHeaderBytes = int64(unsafe.Sizeof(entry{}))

const noSlot = ^uint32(0)

const pinRetries = 2 // PIN_RETRIES

// tryPin 增加拥有型引用计数，不超过 max。无等待：先加后检，超限即回滚。
// 回滚前的瞬时超额只会让回收判定（ownedPins==0）更保守，不会提前释放；
// 相比有限次 CAS，热 key 并发下不会因 CAS 竞争而产生伪 BUSY。
func (e *entry) tryPin(max int32) bool {
	if e.ownedPins.Add(1) > max {
		e.ownedPins.Add(-1)
		return false
	}
	return true
}

func (e *entry) unpin() { e.ownedPins.Add(-1) }

func (e *entry) keyPtr() unsafe.Pointer {
	return unsafe.Add(unsafe.Pointer(e), uintptr(EntryHeaderBytes))
}

// keyString 返回块内 key 的零拷贝视图：只在条目被 pin 住期间有效。
func (e *entry) keyString() string { return unsafe.String((*byte)(e.keyPtr()), int(e.keyLen)) }

// valuePtr 返回值段起始地址（key 之后按 8 字节对齐）。
func (e *entry) valuePtr() unsafe.Pointer {
	return unsafe.Add(e.keyPtr(), uintptr(roundUp8(int64(e.keyLen))))
}

// entry 返回块引用对应的条目头；0 返回 nil。
func (a *arena) entry(r blockRef) *entry {
	if r == 0 {
		return nil
	}
	return (*entry)(a.at(r))
}

// refOf 返回条目头在本 arena 内的块引用。
func (a *arena) refOf(e *entry) blockRef {
	return blockRef((uintptr(unsafe.Pointer(e)) - uintptr(a.base)) / arenaUnit)
}
