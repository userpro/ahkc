package ahkc

import "sync/atomic"

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

// heatGate 是非驻留门卫：固定 rows x width 单元，完整 key 映射至各行。
// 查询取各行 H 的最小值。碰撞可高估，饱和与更新竞争也可低估；
// 这是准入门卫，不能套用未饱和 Count-Min 的频率误差保证。
type heatGate struct {
	rows  int
	width int
	cells []heatCell // rows*width，行优先
}

func newHeatGate(rows, width int) heatGate {
	return heatGate{rows: rows, width: width, cells: make([]heatCell, rows*width)}
}

const rowSeedStep = 0x9e3779b97f4a7c15

func (g *heatGate) cellAt(row int, keyHash uint64) *heatCell {
	h := mix64(keyHash + uint64(row)*rowSeedStep)
	return &g.cells[row*g.width+int(h%uint64(g.width))]
}

// add 仅 miss 路径调用：各行独立有限次尝试。
func (g *heatGate) add(keyHash, e uint64, hcap uint32, retries int) {
	for row := 0; row < g.rows; row++ {
		g.cellAt(row, keyHash).add(e, hcap, retries)
	}
}

// read 返回各行热度的最小值；任一 UNKNOWN 则整体 UNKNOWN。
func (g *heatGate) read(keyHash, e uint64, phi float64) (float64, bool) {
	lowest := float64(0)
	for row := 0; row < g.rows; row++ {
		h, ok := g.cellAt(row, keyHash).read(e, phi)
		if !ok {
			return 0, false
		}
		if row == 0 || h < lowest {
			lowest = h
		}
	}
	return lowest, true
}
