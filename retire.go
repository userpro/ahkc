package ahkc

// 条目退休与回收：摘链后挂入分片的侵入式退休链（tryRetire），
// 由维护者按配额推进安全释放（reclaimStep）。
//
// 没有独立的退休预算：条目区本身就是上限，退休块尚未回收时新的分配失败会被
// 优雅拒绝，天然限流。所有操作只在分片维护者上执行。

// tryRetire 退休条目：从索引与策略链摘除，账目转入退休链，等待读者全部离开后回收。
func (c *cache[V]) tryRetire(s *shard[V], e *entry) {
	e.setReadable(false)
	s.idx.remove(e)
	s.policy.remove(e)
	s.residentCount.Add(-1)
	s.residentBytes.Add(-int64(e.billedBytes))
	c.registerRetired(s, e)
}

// registerRetired 把已摘除的条目追加到退休链尾，记录退休 epoch。
// 退休链复用 policyNext（条目已不在策略链中）。
func (c *cache[V]) registerRetired(s *shard[V], e *entry) {
	er := s.arena.refOf(e)
	e.retireEpoch = uint32(s.epoch.epoch.Load())
	e.policyNext = 0
	if s.retireTail == 0 {
		s.retireHead = er
	} else {
		s.arena.entry(s.retireTail).policyNext = er
	}
	s.retireTail = er
	s.retiredCount.Add(1)
}

// reclaimStep 推进 epoch 并安全回收退休条目（RECLAIM_STEP，I9）。
// 链头按退休顺序检查：epoch 已安全且无拥有型句柄持有才释放；否则移到链尾，
// 避免被一个长期持有的条目挡住后面已可回收的条目。每轮检查数受配额限制。
//
// 有进展且退休链非空时，s.reclaimMore 为真，维护者立即续行而不等下一个周期：
// 进展指释放了块，或有条目在等 epoch 且 epoch 仍在前进。只被拥有型句柄挡住（epoch 早已安全）、
// 或被读者挡住（epoch 无法前进）时没有进展，仍按周期节流，不空转。
func (c *cache[V]) reclaimStep(s *shard[V], quota int) int {
	epochBefore, freed, waitEpoch := s.epoch.epoch.Load(), 0, false
	work := s.epoch.advanceStep((quota + 1) / 2)
	for work < quota && s.retireHead != 0 {
		er := s.retireHead
		e := s.arena.entry(er)
		work++
		if s.retireHead = e.policyNext; s.retireHead == 0 {
			s.retireTail = 0
		}
		e.policyNext = 0
		safe := s.epoch.safeForReclaim(e.retireEpoch)
		if safe && e.ownedPins.Load() == 0 {
			c.freeBlock(s, e)
			freed++
			s.retiredCount.Add(-1)
			c.metrics.retiredCompleted.Add(1)
			continue
		}
		waitEpoch = waitEpoch || !safe
		if s.retireTail == 0 {
			s.retireHead = er
		} else {
			s.arena.entry(s.retireTail).policyNext = er
		}
		s.retireTail = er
	}
	s.reclaimMore = s.retireHead != 0 && (freed > 0 || waitEpoch && (s.epoch.scanActive || s.epoch.epoch.Load() != epochBefore))
	return work
}
