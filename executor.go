package ahkc

import (
	"sync"
	"sync/atomic"
)

// Stable wrappers live in a fixed pool. Even an executor that stops dispatching
// accepted tasks cannot make the library retain an unbounded callback backlog.
type executorTask struct {
	fn     func()
	run    func()
	stripe int
}

type executorTaskPool struct {
	items   []executorTask
	stripes []executorTaskStripe
	hint    atomic.Uint32
}

type executorTaskStripe struct {
	mu   sync.Mutex
	free []*executorTask
}

func newExecutorTaskPool[V any](c *cache[V], size int) *executorTaskPool {
	stripeCount := c.cfg.ShardCount
	if stripeCount > 64 {
		stripeCount = 64
	}
	if stripeCount > size {
		stripeCount = size
	}
	p := &executorTaskPool{items: make([]executorTask, size), stripes: make([]executorTaskStripe, stripeCount)}
	for i := range p.stripes {
		p.stripes[i].free = make([]*executorTask, 0, (size+stripeCount-1)/stripeCount)
	}
	for i := range p.items {
		t := &p.items[i]
		t.stripe = i % stripeCount
		t.run = func() {
			defer func() {
				t.fn = nil
				p.put(t)
				c.tasks.Add(-1)
				c.checkReclaimed()
			}()
			t.fn()
		}
		st := &p.stripes[t.stripe]
		st.free = append(st.free, t)
	}
	return p
}

func (p *executorTaskPool) tryGet() (*executorTask, bool) {
	start := p.hint.Add(1) % uint32(len(p.stripes))
	for attempt := 0; attempt < reserveRetries; attempt++ {
		st := &p.stripes[(int(start)+attempt)%len(p.stripes)]
		if !st.mu.TryLock() {
			continue
		}
		if len(st.free) == 0 {
			st.mu.Unlock()
			continue
		}
		t := st.free[len(st.free)-1]
		st.free[len(st.free)-1] = nil
		st.free = st.free[:len(st.free)-1]
		st.mu.Unlock()
		return t, true
	}
	return nil, false
}

func (p *executorTaskPool) put(t *executorTask) {
	st := &p.stripes[t.stripe]
	st.mu.Lock()
	st.free = append(st.free, t)
	st.mu.Unlock()
}

// Clear the payload closure before returning its slot, including rejection.
func (c *cache[V]) trySubmit(task func()) bool {
	t, ok := c.taskPool.tryGet()
	if !ok {
		return false
	}
	c.tasks.Add(1)
	t.fn = task
	if c.cfg.Executor.TrySubmit(t.run) {
		return true
	}
	t.fn = nil
	c.taskPool.put(t)
	c.tasks.Add(-1)
	return false
}

// ManualExecutor 是确定性执行器：任务入队但不自动运行，由调用方 Drain 驱动。
// 适用于单测与需要确定性交错验证的宿主测试。
type ManualExecutor struct {
	mu    sync.Mutex
	queue []func()
	head  int
	n     int
}

// NewManualExecutor 创建手动执行器。
func NewManualExecutor() *ManualExecutor { return &ManualExecutor{} }

// TrySubmit 实现 Executor：非 nil 任务接受并入队，至多执行一次。
func (m *ManualExecutor) TrySubmit(task func()) bool {
	if task == nil {
		return false
	}
	m.mu.Lock()
	if m.n == len(m.queue) {
		grown := make([]func(), max(1, 2*len(m.queue)))
		n := copy(grown, m.queue[m.head:])
		copy(grown[n:], m.queue[:m.head])
		m.queue, m.head = grown, 0
	}
	m.queue[(m.head+m.n)%len(m.queue)] = task
	m.n++
	m.mu.Unlock()
	return true
}

// Drain 运行全部已排队任务（含运行中新提交的任务），直到队列空或达到 maxSteps。
// 返回运行的任务数。
func (m *ManualExecutor) Drain(maxSteps int) int {
	n := 0
	for n < maxSteps {
		m.mu.Lock()
		if m.n == 0 {
			m.mu.Unlock()
			return n
		}
		task := m.queue[m.head]
		m.queue[m.head] = nil
		m.head = (m.head + 1) % len(m.queue)
		m.n--
		m.mu.Unlock()
		task()
		n++
	}
	return n
}

// Pending 返回排队任务数。
func (m *ManualExecutor) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}
