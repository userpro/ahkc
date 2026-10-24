package ahkc

// SIEVE 策略队列。
//
// 新条目加入队列头部；命中仅尝试置访问位（不移动链表）；
// 扫描手从尾向头推进、到头回绕。遇到已置位条目清位并跳过，否则可纳入 Q。
// 链接字段仅分片维护者访问。

type policyList[V any] struct {
	head, tail *entry[V]
	hand       *entry[V]
	count      int
}

// pushHead 新条目入队头。
func (l *policyList[V]) pushHead(e *entry[V]) {
	e.inPolicy = true
	e.policyPrev = nil // policyPrev 指向“更靠近头”的方向
	e.policyNext = l.head
	if l.head != nil {
		l.head.policyPrev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
	l.count++
}

// remove 摘链并修正 hand：删除 hand 时先取后继（向头方向的下一个）。
func (l *policyList[V]) remove(e *entry[V]) {
	if !e.inPolicy {
		return
	}
	if l.hand == e {
		l.hand = l.nextTowardHeadOrWrapSkipping(e, e)
		if l.hand == e { // 单元素
			l.hand = nil
		}
	}
	if e.policyPrev != nil {
		e.policyPrev.policyNext = e.policyNext
	} else {
		l.head = e.policyNext
	}
	if e.policyNext != nil {
		e.policyNext.policyPrev = e.policyPrev
	} else {
		l.tail = e.policyPrev
	}
	e.inPolicy = false
	e.policyPrev, e.policyNext = nil, nil
	l.count--
}

// nextTowardHeadOrWrap 从 e 向头方向推进，到头回绕到尾。
func (l *policyList[V]) nextTowardHeadOrWrap(e *entry[V]) *entry[V] {
	if e.policyPrev != nil {
		return e.policyPrev
	}
	return l.tail
}

// nextTowardHeadOrWrapSkipping 推进但跳过 skip（用于删除 hand 自身）。
func (l *policyList[V]) nextTowardHeadOrWrapSkipping(e, skip *entry[V]) *entry[V] {
	n := e.policyPrev
	if n == nil {
		n = l.tail
	}
	if n == skip {
		return e // 单元素环
	}
	return n
}
