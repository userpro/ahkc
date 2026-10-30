package ahkc

// SIEVE 策略队列。
//
// 新条目加入队列头部；命中仅尝试置访问位（不移动链表）；
// 扫描手从尾向头推进、到头回绕。遇到已置位条目清位并跳过，否则可纳入 Q。
// 链接用块引用保存（0 为空），仅分片维护者访问。

type policyList struct {
	a          *arena
	head, tail blockRef
	hand       blockRef
	count      int
}

// pushHead 新条目入队头。
func (l *policyList) pushHead(e *entry) {
	er := l.a.refOf(e)
	e.inPolicy = true
	e.policyPrev = 0 // policyPrev 指向“更靠近头”的方向
	e.policyNext = l.head
	if l.head != 0 {
		l.a.entry(l.head).policyPrev = er
	}
	l.head = er
	if l.tail == 0 {
		l.tail = er
	}
	l.count++
}

// remove 摘链并修正 hand：删除 hand 时先取后继（向头方向的下一个）。
func (l *policyList) remove(e *entry) {
	if !e.inPolicy {
		return
	}
	er := l.a.refOf(e)
	if l.hand == er {
		l.hand = l.nextTowardHeadOrWrapSkipping(e, er)
		if l.hand == er { // 单元素
			l.hand = 0
		}
	}
	if e.policyPrev != 0 {
		l.a.entry(e.policyPrev).policyNext = e.policyNext
	} else {
		l.head = e.policyNext
	}
	if e.policyNext != 0 {
		l.a.entry(e.policyNext).policyPrev = e.policyPrev
	} else {
		l.tail = e.policyPrev
	}
	e.inPolicy = false
	e.policyPrev, e.policyNext = 0, 0
	l.count--
}

// nextTowardHeadOrWrap 从 e 向头方向推进，到头回绕到尾。
func (l *policyList) nextTowardHeadOrWrap(e *entry) blockRef {
	if e.policyPrev != 0 {
		return e.policyPrev
	}
	return l.tail
}

// nextTowardHeadOrWrapSkipping 推进但跳过 skip（用于删除 hand 自身）。
func (l *policyList) nextTowardHeadOrWrapSkipping(e *entry, skip blockRef) blockRef {
	n := e.policyPrev
	if n == 0 {
		n = l.tail
	}
	if n == skip {
		return skip // 单元素环
	}
	return n
}
