package ahkc

import (
	"math/bits"
	"slices"
)

// 精确子集求解。
//
// 求解器接收冻结快照上的真实可执行动作集：
// 容量（条数/字节）与索引安装位置
// 都在 FEASIBLE 内同时核算，不能先求容量解再遗漏其他约束。
//
//	S* = argmin[S∈F(Q)] Σ L_v；ΔG* = G_a − K_a − Σ_O L_o − Σ_S* L_v；发布要求 ΔG* > 0。
//	并列解依次优先更少退休字节、更少删除条目，再按稳定身份打破平局。

// victimInfo 是冻结后的受害者视图（身份、大小、损失、索引槽位）。
type victimInfo struct {
	id        uint64
	billed    int64
	loss      float64
	indexSlot int
}

// fillSnapshot 是冻结输入（FREEZE_INPUT）。
type fillSnapshot struct {
	aBytes                int64 // 候选计费大小
	oBytes                int64 // O（同 key 旧条目，至多一个）计费大小
	oCount                int   // |O|
	q                     []victimInfo
	n, bytes              int64 // 冻结的驻留条数/字节
	countQuota, byteQuota int64
	installWithoutQ       bool
	installMask           uint64 // Q 成员槽位落在候选探测域内的位掩码
	maxChecks             int
	checks                int             // 本次求解实际检查的子集数（观测用）
	sums                  *[256]subsetAgg // 通用枚举的子集累计 scratch（分片维护者独占复用；nil 时临时分配）
}

// allocSums 在堆上分配回退用的累计表。必须 noinline：内联后因不逃逸会被放回调用者栈帧，
// 使 exactSelect 的栈帧恒为 6KB（每个新 goroutine 都要为此扩栈）。
//
//go:noinline
func allocSums() *[256]subsetAgg { return new([256]subsetAgg) }

// subsetAgg 是子集（位掩码）的条数 / 字节 / 损失累计。
type subsetAgg struct {
	count int64
	bytes int64
	loss  float64
}

const (
	selectInfeasible = -2
	selectWorkLimit  = -3
)

// feasible 判定整个 O∪S 组合是否可执行（A.5 FEASIBLE）。
func (s *fillSnapshot) feasible(mask uint64, sumCount int64, sumBytes int64) bool {
	rCount := int64(s.oCount) + sumCount
	rBytes := s.oBytes + sumBytes
	postN := s.n + 1 - rCount
	postB := s.bytes + s.aBytes - rBytes
	if postN > s.countQuota || postB > s.byteQuota {
		return false
	}
	if !s.installWithoutQ && mask&s.installMask == 0 {
		return false
	}
	return true
}

// rankLess 比较两个可行解的优劣：(损失, 退休字节, 条目数, 稳定身份)。
func (s *fillSnapshot) rankLess(maskA uint64, lossA float64, bytesA, countA int64,
	maskB uint64, lossB float64, bytesB, countB int64) bool {
	if lossA != lossB {
		return lossA < lossB
	}
	if bytesA != bytesB {
		return bytesA < bytesB
	}
	if countA != countB {
		return countA < countB
	}
	return s.stableIDLess(maskA, maskB)
}

// stableIDLess 按成员稳定身份（entryID 升序序列的字典序）打破平局。
func (s *fillSnapshot) stableIDLess(maskA, maskB uint64) bool {
	var idsA, idsB [8]uint64
	na, nb := 0, 0
	for i := 0; i < len(s.q); i++ {
		if maskA&(1<<uint(i)) != 0 {
			idsA[na] = s.q[i].id
			na++
		}
		if maskB&(1<<uint(i)) != 0 {
			idsB[nb] = s.q[i].id
			nb++
		}
	}
	slices.Sort(idsA[:na])
	slices.Sort(idsB[:nb])
	return slices.Compare(idsA[:na], idsB[:nb]) < 0
}

// exactSelect 精确求解（A.5 EXACT_SELECT）。
// 返回 (mask>=0) 表示选中集合；selectInfeasible 表示 F(Q) 为空；
// selectWorkLimit 表示求解预算耗尽（整次拒绝，不能提交暂定最佳）。
func (s *fillSnapshot) exactSelect() int64 {
	m := len(s.q)
	if m == 0 {
		if s.feasible(0, 0, 0) {
			return 0
		}
		return selectInfeasible
	}
	allNonNeg := true
	for _, v := range s.q {
		if v.loss < 0 {
			allNonNeg = false
			break
		}
	}
	// 快速路径 1：无容量缺口且空集合可行、所有损失非负 → S*=空集。
	if allNonNeg && s.feasible(0, 0, 0) {
		return 0
	}
	// 快速路径 2：等大小、非负损失、索引位置已解决 → 取损失最小的 k 个。
	if allNonNeg && s.installWithoutQ && s.equalSized() {
		b := s.q[0].billed
		// 条数缺口同时受额度 countQuota 与索引物理上限 countMax 约束（见 feasible 的 postN 检查）。
		kN := s.n + 1 - int64(s.oCount) - s.countQuota
		needB := s.bytes + s.aBytes - s.oBytes - s.byteQuota
		var kB int64
		if needB > 0 && b > 0 {
			kB = ceilDiv(needB, b)
		}
		k := kN
		if kB > k {
			k = kB
		}
		if k < 0 {
			k = 0
		}
		if k <= int64(m) {
			mask := s.kSmallestLosses(int(k))
			if s.feasible(mask, k, k*b) {
				return int64(mask)
			}
		}
		// 同大小同数量组合的资源要求相同；无解即可拒绝。
		return selectInfeasible
	}
	// 一般情况：枚举全部子集（m<=8 → 最多 256 个），受工作量预算约束。
	maxMask := uint64(1) << uint(m)
	sums := s.sums
	if sums == nil {
		sums = allocSums()
	}
	sums[0] = subsetAgg{}
	for mask := uint64(1); mask < maxMask; mask++ {
		bit := mask & (-mask)
		prev := mask ^ bit
		idx := bits.TrailingZeros64(bit)
		sums[mask] = subsetAgg{count: sums[prev].count + 1, bytes: sums[prev].bytes + s.q[idx].billed, loss: sums[prev].loss + s.q[idx].loss}
	}
	best := int64(-1)
	var bestLoss float64
	var bestBytes, bestCount int64
	for mask := uint64(0); mask < maxMask; mask++ {
		if s.checks >= s.maxChecks {
			return selectWorkLimit
		}
		s.checks++
		a := sums[mask]
		if !s.feasible(mask, a.count, a.bytes) {
			continue
		}
		if best < 0 || s.rankLess(mask, a.loss, a.bytes, a.count,
			uint64(best), bestLoss, bestBytes, bestCount) {
			best = int64(mask)
			bestLoss, bestBytes, bestCount = a.loss, a.bytes, a.count
		}
	}
	if best < 0 {
		return selectInfeasible
	}
	return best
}

func (s *fillSnapshot) equalSized() bool {
	for i := 1; i < len(s.q); i++ {
		if s.q[i].billed != s.q[0].billed {
			return false
		}
	}
	return true
}

// kSmallestLosses 返回损失最小的 k 个成员的位掩码（k<=m<=8，选择排序足够）。
func (s *fillSnapshot) kSmallestLosses(k int) uint64 {
	if k == 0 {
		return 0
	}
	// 标记已选
	var picked [8]bool
	var mask uint64
	for n := 0; n < k; n++ {
		best := -1
		for i := 0; i < len(s.q); i++ {
			if picked[i] {
				continue
			}
			if best < 0 || s.q[i].loss < s.q[best].loss ||
				(s.q[i].loss == s.q[best].loss && s.q[i].id < s.q[best].id) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		picked[best] = true
		mask |= 1 << uint(best)
	}
	return mask
}

// ceilDiv 返回 a/b 向上取整；a 不大于 0 时为 0。
func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}
