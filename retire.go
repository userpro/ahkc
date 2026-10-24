package ahkc

import (
	"sync"
	"sync/atomic"
)

// 条目退休与回收：摘链登记退休记录（tryRetire）、按配额推进安全释放（reclaimStep）。
// 由单维护者调用；释放在回收执行器上进行，不进入索引 / Flight / 维护临界区。

// tryRetire 退休条目（TRY_RETIRE）：无退休额度返回 false，索引/resident 账目保留。
func (c *cache[V]) tryRetire(s *shard[V], e *entry[V]) bool {
	if !c.prepareRetirement(s, e) {
		return false
	}
	e.readable.Store(false)
	s.idx.remove(e)
	s.policy.remove(e)
	s.wheel.remove(e)
	s.residentCount.Add(-1)
	s.residentBytes.Add(-e.billedBytes)
	c.registerRetired(s, e)
	return true
}

const (
	retiredWaitSafe int32 = iota
	retiredSubmitting
	retiredDisposing
	retiredDone
	retiredAccounted
)

// retiredRecord 记录退休对象，保留至安全释放且账目归还。
type retiredRecord[V any] struct {
	e           *entry[V]
	s           *shard[V]
	entryID     uint64
	retireEpoch uint64
	billed      int64
	lease       [2]receipt // 退休字节 / 退休记录凭证：内嵌于池化记录，无逐次分配
	state       atomic.Int32
}

// releaseLease 归还两项退休凭证（各至多一次）。
func (r *retiredRecord[V]) releaseLease() {
	r.lease[0].returnOnce()
	r.lease[1].returnOnce()
}

type retiredPool[V any] struct {
	mu    sync.Mutex
	items []retiredRecord[V]
	free  []*retiredRecord[V]
}

func newRetiredPool[V any](size int) *retiredPool[V] {
	p := &retiredPool[V]{items: make([]retiredRecord[V], size), free: make([]*retiredRecord[V], size)}
	for i := range p.items {
		p.free[i] = &p.items[i]
	}
	return p
}

func (p *retiredPool[V]) tryGet() (*retiredRecord[V], bool) {
	if !p.mu.TryLock() {
		return nil, false
	}
	if len(p.free) == 0 {
		p.mu.Unlock()
		return nil, false
	}
	r := p.free[len(p.free)-1]
	p.free[len(p.free)-1] = nil
	p.free = p.free[:len(p.free)-1]
	p.mu.Unlock()
	return r, true
}

func (p *retiredPool[V]) put(r *retiredRecord[V]) {
	r.e, r.s = nil, nil
	p.mu.Lock()
	p.free = append(p.free, r)
	p.mu.Unlock()
}

func (c *cache[V]) prepareRetirement(s *shard[V], e *entry[V]) bool {
	if s.roundActive && (s.retireWorkLeft < 1 || s.retireBytesLeft < e.billedBytes) {
		return false
	}
	r, ok := c.retiredPool.tryGet()
	if !ok {
		return false
	}
	if c.retiredBytes.tryReserveInto(&r.lease[0], e.billedBytes) != failOK {
		c.retiredPool.put(r)
		return false
	}
	if c.retiredRecords.tryReserveInto(&r.lease[1], 1) != failOK {
		r.lease[0].returnOnce()
		c.retiredPool.put(r)
		return false
	}
	r.e, r.s, r.entryID = e, s, e.entryID
	r.billed = e.billedBytes
	r.state.Store(retiredWaitSafe)
	e.retirement = r
	return true
}

func (c *cache[V]) registerRetired(s *shard[V], e *entry[V]) {
	if s.roundActive {
		s.retireWorkLeft--
		s.retireBytesLeft -= e.billedBytes
	}
	r := e.retirement
	r.retireEpoch = s.epoch.epoch.Load()
	e.retirement = nil
	r.state.Store(retiredWaitSafe)
	s.retired = append(s.retired, r)
	s.retiredCount.Add(1)
}

// reclaimStep 推进 epoch 并安全回收退休对象（RECLAIM_STEP，I9）。
func (c *cache[V]) reclaimStep(s *shard[V], quota int) int {
	work := s.epoch.advanceStep((quota + 1) / 2)
	for work < quota && len(s.retired) > 0 {
		i := s.retiredScan % len(s.retired)
		r := s.retired[i]
		work++
		switch r.state.Load() {
		case retiredWaitSafe:
			if s.epoch.safeForReclaim(r.retireEpoch) && r.e.ownedPins.Load() == 0 {
				bytes := r.e.billedBytes
				if s.roundActive && bytes > s.reclaimBytesLeft {
					s.retiredScan = (i + 1) % len(s.retired)
					continue
				}
				if r.state.CompareAndSwap(retiredWaitSafe, retiredSubmitting) {
					if !c.trySubmit(func() { c.disposeRetired(r) }) {
						r.state.Store(retiredWaitSafe)
					} else if s.roundActive {
						s.reclaimBytesLeft -= bytes
					}
				}
			}
		case retiredDone:
			if r.state.CompareAndSwap(retiredDone, retiredAccounted) {
				r.releaseLease()
				c.metrics.retiredCompleted.Add(1)
			}
		}
		if r.state.Load() != retiredAccounted {
			s.retiredScan = (i + 1) % len(s.retired)
		} else {
			last := len(s.retired) - 1
			s.retired[i] = s.retired[last]
			s.retired[last] = nil
			s.retired = s.retired[:last]
			s.retiredScan = i
			s.retiredCount.Add(-1)
			c.retiredPool.put(r)
		}
	}
	return work
}

// disposeRetired 在回收执行器上运行：无索引/Flight/维护临界区。
func (c *cache[V]) disposeRetired(r *retiredRecord[V]) {
	r.state.Store(retiredDisposing)
	e := r.e
	var zero V
	e.value = zero // 释放 value 引用，由 GC 回收
	e.key = ""
	r.s.pool.put(e)
	r.e = nil
	r.state.Store(retiredDone)
}
