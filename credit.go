package ahkc

// CreditState is a coherent snapshot of the capacity ledger. Granted includes
// resident and idle shard capacity; a unique control carries each transit grant.
type CreditState struct {
	CountLimit, CountGranted, CountFree, CountInTransit int64
	ByteLimit, ByteGranted, ByteFree, ByteInTransit     int64
}

// Credits returns ok=false during a transfer instead of waiting for its owner.
func (c *Cache[V]) Credits() (state CreditState, ok bool) {
	if c == nil || c.c == nil {
		return state, false
	}
	cc := c.c
	if !cc.creditOwner.CompareAndSwap(false, true) {
		return state, false
	}
	defer cc.creditOwner.Store(false)
	state.CountLimit, state.ByteLimit = cc.cfg.MaxEntries, cc.cfg.MaxResidentBytes
	state.CountFree, state.ByteFree = cc.globalFreeCount.Load(), cc.globalFreeBytes.Load()
	state.CountInTransit, state.ByteInTransit = cc.creditInTransitN.Load(), cc.creditInTransitB.Load()
	for _, s := range cc.shards {
		state.CountGranted += s.countQuota.Load()
		state.ByteGranted += s.byteQuota.Load()
	}
	return state, true
}
