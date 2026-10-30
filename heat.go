package ahkc

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// 双时间桶热度。
//
// 计数状态 {epoch, current, previous} 打包进单个 64 位原子字：
// 桶推进与加一作为一致状态提交（CAS），不会先公开新 epoch 再清零旧计数。
//
//	H(t) = current + (1-phi) * previous，phi 为当前桶已过去比例。
//	跨过 1 个桶：current 移入 previous；跨过 >=2 个桶：全部清零。
//	桶内计数到 Hcap 后跳写（饱和跳写）。
//
// 位宽：epoch 40 位，current/previous 各 12 位（Hcap <= 4095）。
// delta >= 1ms 时 epoch 可用约 34 年；配置校验保证 Hcap 可表示。

const (
	heatEpochBits = 40
	heatCountBits = 12
	heatCountMax  = uint32(1<<heatCountBits - 1)
)

type heatSnap struct {
	epoch uint64
	cur   uint32
	prev  uint32
}

func packHeat(s heatSnap) uint64 {
	return s.epoch<<(2*heatCountBits) | uint64(s.cur)<<heatCountBits | uint64(s.prev)
}

func unpackHeat(v uint64) heatSnap {
	return heatSnap{
		epoch: v >> (2 * heatCountBits),
		cur:   uint32((v >> heatCountBits) & uint64(heatCountMax)),
		prev:  uint32(v & uint64(heatCountMax)),
	}
}

// bucketIndex 计算时间 t 所在的桶号。
func bucketIndex(t, delta int64) uint64 { return uint64(t / delta) }

// bucketPhi 计算当前桶已过去比例 [0,1)。
func bucketPhi(t, delta int64) float64 {
	return float64(t%delta) / float64(delta)
}

// normalizeHeat 将快照归一化到桶 e。ok=false 表示 UNKNOWN：
// 请求被暂停后恢复导致时间落后于快照 epoch，放弃本次统计，禁止回退 epoch。
func normalizeHeat(s heatSnap, e uint64) (heatSnap, bool) {
	switch {
	case s.epoch == e:
		return s, true
	case e == s.epoch+1:
		return heatSnap{epoch: e, prev: s.cur}, true
	case e > s.epoch+1:
		return heatSnap{epoch: e}, true
	default:
		return heatSnap{}, false
	}
}

type heatAddResult uint8

const (
	heatUpdated heatAddResult = iota
	heatSaturated
	heatSkipped
)

// heatCell 是打包的原子热度单元。
type heatCell struct{ v atomic.Uint64 }

// add 最多 retries 次尝试：读取一致快照 -> 归一化 -> min(current+1, Hcap) -> CAS。
func (c *heatCell) add(e uint64, hcap uint32, retries int) heatAddResult {
	for i := 0; i < retries; i++ {
		old := c.v.Load()
		s := unpackHeat(old)
		ns, ok := normalizeHeat(s, e)
		if !ok {
			return heatSkipped
		}
		if ns.epoch == s.epoch && ns.cur >= hcap {
			return heatSaturated
		}
		ns.cur++ // 此处 ns.cur < hcap：换桶后为 0，同桶已在上方按饱和跳写
		if c.v.CompareAndSwap(old, packHeat(ns)) {
			return heatUpdated
		}
	}
	return heatSkipped
}

// read 在快照副本上归一化并返回加权热度，不强制写回。
func (c *heatCell) read(e uint64, phi float64) (float64, bool) {
	s, ok := normalizeHeat(unpackHeat(c.v.Load()), e)
	if !ok {
		return 0, false
	}
	return float64(s.cur) + (1-phi)*float64(s.prev), true
}

// heatRetries 是热度更新 CAS 的最大尝试次数：失败即放弃本次更新（有损，宁可低估也不自旋）。
const heatRetries = 2

// gateLine 是门卫一组的热度单元数：64 字节 = 一条缓存行。
const gateLine = 8

// heatGate 是非驻留门卫：每个 key 映射到一组（一条缓存行，8 个热度单元），
// 4 行各在组内自己的 16 字节段（2 个单元）中二选一；查询取 4 个单元热度的最小值。
// 一次 miss 的读与写都只访问一条缓存行。碰撞可高估，饱和与更新竞争也可低估；
// 这是准入门卫，不能套用未饱和 Count-Min 的频率误差保证。
//
// shortcut: 默认单元数按容量近似（见 applyDefaults 中的 Heat.GateCells）；规模场景应按"每个 Heat.Delta 内
// 去重后的未命中 key 数的 4 倍"设置 Heat.GateCells。
type heatGate struct {
	cells  []heatCell // groups × gateLine
	groups uint64
}

// newHeatGate 把门卫建立在 cells 上（长度须是 gateLine 的正整数倍，且已清零）。
func newHeatGate(cells []heatCell) heatGate {
	return heatGate{cells: cells, groups: uint64(len(cells) / gateLine)}
}

func gateRegionBytes(cells int) uintptr { return uintptr(cells) * unsafe.Sizeof(heatCell{}) }

func (g *heatGate) line(keyHash uint64) (*[gateLine]heatCell, uint64) {
	h := mix64(keyHash ^ rowSeedStep)
	hi, _ := bits.Mul64(h, g.groups)
	return (*[gateLine]heatCell)(g.cells[hi*gateLine:]), h
}

const rowSeedStep = 0x9e3779b97f4a7c15

// add 仅 miss 路径调用：各行独立有限次尝试。
func (g *heatGate) add(keyHash, e uint64) {
	line, h := g.line(keyHash)
	for row := uint(0); row < 4; row++ {
		line[row*2+uint(h>>row)&1].add(e, heatCountMax, heatRetries)
	}
}

// read 返回各行热度的最小值；任一 UNKNOWN 则整体 UNKNOWN。
func (g *heatGate) read(keyHash, e uint64, phi float64) (float64, bool) {
	line, h := g.line(keyHash)
	lowest := float64(0)
	for row := uint(0); row < 4; row++ {
		v, ok := line[row*2+uint(h>>row)&1].read(e, phi)
		if !ok {
			return 0, false
		}
		if row == 0 || v < lowest {
			lowest = v
		}
	}
	return lowest, true
}
