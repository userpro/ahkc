package ahkc

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

// bruteForce 独立穷举：与 EXACT_SELECT 的实现路径（DP 递推/快速路径）无关。
func bruteForce(s *fillSnapshot) int64 {
	m := len(s.q)
	maxMask := uint64(1) << uint(m)
	best := int64(selectInfeasible)
	var bestLoss float64
	var bestBytes, bestCount int64
	var bestIDs []uint64
	for mask := uint64(0); mask < maxMask; mask++ {
		var count, bytes int64
		var loss float64
		var ids []uint64
		for i := 0; i < m; i++ {
			if mask&(1<<uint(i)) != 0 {
				count++
				bytes += s.q[i].billed
				loss += s.q[i].loss
				ids = append(ids, s.q[i].id)
			}
		}
		// Use the specification's deficits and resource inequalities directly;
		// sharing the production feasibility/ranking helpers would hide bugs in them.
		needCount := max(int64(0), s.n+1-int64(s.oCount)-s.countQuota)
		needBytes := max(int64(0), s.bytes+s.aBytes-s.oBytes-s.byteQuota)
		if count < needCount || bytes < needBytes ||
			s.n+1-int64(s.oCount)-count > s.countMax ||
			s.oBytes+bytes > s.retiredBytesAvail ||
			int64(s.oCount)+count > s.retiredRecordSlots ||
			int64(s.oCount)+count > s.removalWorkLeft ||
			(!s.installWithoutQ && mask&s.installMask == 0) || !s.wheelOK {
			continue
		}
		slices.Sort(ids)
		better := loss < bestLoss || (loss == bestLoss &&
			(bytes < bestBytes || (bytes == bestBytes &&
				(count < bestCount || (count == bestCount && slices.Compare(ids, bestIDs) < 0)))))
		if best < 0 || better {
			best = int64(mask)
			bestLoss, bestBytes, bestCount = loss, bytes, count
			bestIDs = ids
		}
	}
	return best
}

func baseSnapshot(q []victimInfo) *fillSnapshot {
	return &fillSnapshot{
		aBytes: 4, q: q,
		n: 10, bytes: 40,
		countQuota: 12, byteQuota: 44, countMax: 16,
		retiredBytesAvail: 1 << 40, retiredRecordSlots: 16, removalWorkLeft: 16,
		installWithoutQ: true, wheelOK: true, maxChecks: 256,
	}
}

func TestExactSelectEmptyQ(t *testing.T) {
	s := baseSnapshot(nil)
	if got := s.exactSelect(); got != 0 {
		t.Fatalf("empty Q feasible: got %d", got)
	}
	// 空集合不可行（容量缺口必须释放）→ INFEASIBLE。
	s.n, s.countQuota = 10, 9
	if got := s.exactSelect(); got != selectInfeasible {
		t.Fatalf("empty Q infeasible: got %d", got)
	}
}

// 规格示例：新对象大小 6、收益 14、安装成本 1；
// Q 有 (6,13)、(3,4)、(3,5)；需释放 6 → 选后两项（净增益 4），只取第一个净增益 0 应拒绝。
func TestExactSelectSpecExample(t *testing.T) {
	q := []victimInfo{
		{id: 1, billed: 6, loss: 13, indexSlot: 0},
		{id: 2, billed: 3, loss: 4, indexSlot: 1},
		{id: 3, billed: 3, loss: 5, indexSlot: 2},
	}
	s := baseSnapshot(q)
	s.aBytes = 6
	s.bytes, s.byteQuota = 40, 40 // r_b = 6
	got := s.exactSelect()
	if got != 0b110 { // Q[1] | Q[2]
		t.Fatalf("want mask 0b110, got %b", got)
	}
	// 净收益检验：G_a=14, K=1 → ΔG = 14-1-(4+5) = 4 > 0；若只移除 (6,13)：ΔG=0 应拒绝。
	gain := 14 - 1 - (4 + 5)
	if gain <= 0 {
		t.Fatal("spec example gain must be positive")
	}
}

// 等大小静态选择 Top-K；变大小不能用密度贪心代替整数最优。
func TestExactSelectKnapsackNotGreedy(t *testing.T) {
	// 容量需释放 10：受害者 (6,L=12),(5,L=9),(5,L=9)。
	// 密度贪心会先挑 12/6=2，再 9/5=1.8 → 还需 4 字节但无更多 → 不可行解 (6,5)=11 损失 21；
	// 整数最优 (5,5)=10 损失 18。
	q := []victimInfo{
		{id: 1, billed: 6, loss: 12},
		{id: 2, billed: 5, loss: 9},
		{id: 3, billed: 5, loss: 9},
	}
	s := baseSnapshot(q)
	s.aBytes = 10
	s.bytes, s.byteQuota = 40, 40 // r_b = 10
	got := s.exactSelect()
	if got != 0b110 {
		t.Fatalf("want mask 0b110 (5+5), got %b", got)
	}
}

func TestExactSelectFastPathConsistency(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 2000; trial++ {
		m := rng.Intn(9)
		q := make([]victimInfo, m)
		size := int64(1 + rng.Intn(8))
		for i := range q {
			q[i] = victimInfo{
				id:     uint64(i + 1),
				billed: size,
				loss:   float64(rng.Intn(20)), // 非负 → 快速路径适用
			}
		}
		s := baseSnapshot(q)
		s.n = int64(rng.Intn(12))
		s.countQuota = 10
		s.bytes = int64(rng.Intn(60))
		s.byteQuota = 44
		got := s.exactSelect()
		want := bruteForce(s)
		if got != want {
			t.Fatalf("trial %d: fast path %b != brute %b (q=%+v n=%d bytes=%d)",
				trial, got, want, q, s.n, s.bytes)
		}
	}
}

func TestExactSelectGeneralConsistency(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 2000; trial++ {
		m := rng.Intn(9)
		q := make([]victimInfo, m)
		for i := range q {
			q[i] = victimInfo{
				id:     uint64(i + 1),
				billed: int64(1 + rng.Intn(12)),
				loss:   float64(rng.Intn(25) - 5), // 允许负损失
			}
		}
		s := baseSnapshot(q)
		s.oCount = rng.Intn(2)
		s.oBytes = int64(rng.Intn(8)) * int64(s.oCount)
		s.n = int64(rng.Intn(12))
		s.bytes = int64(rng.Intn(60))
		s.countQuota = 10
		s.byteQuota = 44
		s.retiredBytesAvail = int64(rng.Intn(30))
		s.retiredRecordSlots = int64(rng.Intn(6))
		s.removalWorkLeft = int64(rng.Intn(6))
		s.installWithoutQ = rng.Intn(2) == 0
		if !s.installWithoutQ {
			s.installMask = uint64(rng.Int63n(1 << uint(m+1)))
		}
		s.wheelOK = rng.Intn(4) != 0
		got := s.exactSelect()
		want := bruteForce(s)
		if got != want {
			t.Fatalf("trial %d: got %b want %b q=%+v", trial, got, want, q)
		}
	}
}

func TestExactSelectWorkLimit(t *testing.T) {
	q := make([]victimInfo, 8)
	for i := range q {
		q[i] = victimInfo{id: uint64(i + 1), billed: 2, loss: float64(i)}
	}
	s := baseSnapshot(q)
	s.installWithoutQ = false // 强制走枚举
	s.installMask = 0xff
	s.aBytes = 16
	s.bytes, s.byteQuota = 40, 40
	s.maxChecks = 4 // 预算不足：整次拒绝，不提交暂定最佳
	if got := s.exactSelect(); got != selectWorkLimit {
		t.Fatalf("want work limit, got %d", got)
	}
}

func TestStableIDTieBreak(t *testing.T) {
	q := []victimInfo{
		{id: 9, billed: 2, loss: 3},
		{id: 4, billed: 2, loss: 3},
	}
	s := baseSnapshot(q)
	s.n, s.countQuota = 10, 10 // 需释放 1 条（10+1-1=10）
	s.bytes, s.byteQuota = 40, 40
	s.aBytes = 2
	got := s.exactSelect()
	// 损失/字节/条数并列 → 稳定身份较小者（id=4, mask=0b10）。
	if got != 0b10 {
		t.Fatalf("tie break: got %b", got)
	}
	if math.IsNaN(s.q[0].loss) {
		t.Fatal("unreachable")
	}
}

// TestFastPathHonorsCountMax：快速路径 2 的条数缺口必须同时受 countMax 约束，
// 与通用枚举 / 独立穷举一致（即便 countMax < countQuota 在现行维护流程中不可达）。
func TestFastPathHonorsCountMax(t *testing.T) {
	mk := func() *fillSnapshot {
		s := baseSnapshot([]victimInfo{
			{id: 1, billed: 4, loss: 3}, {id: 2, billed: 4, loss: 1}, {id: 3, billed: 4, loss: 2},
		})
		s.n, s.bytes, s.countQuota, s.byteQuota, s.countMax = 10, 40, 20, 100, 9
		return s
	}
	s := mk()
	got, want := s.exactSelect(), bruteForce(mk())
	if got != want {
		t.Fatalf("fast path mask=%d, brute force mask=%d", got, want)
	}
	if got < 0 || popcount(uint64(got)) != 2 {
		t.Fatalf("n+1=11 exceeds countMax=9: need 2 victims, got mask=%d", got)
	}
}

func popcount(x uint64) int {
	n := 0
	for ; x != 0; x &= x - 1 {
		n++
	}
	return n
}
