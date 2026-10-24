package ahkc

import (
	"sync/atomic"
	"time"
)

// Clock 返回单调时间（纳秒）。命中 TTL 判定必须使用当前单调时间，
// 不允许使用维护线程刷新的旧时钟。
// Now 必须有界、非阻塞且不 panic；缓存与成本模型使用同一时间原点。
type Clock interface {
	Now() int64
}

// realClock 基于 time.Since，天然单调。
type realClock struct{ base time.Time }

var processClockBase = time.Now()

func newRealClock() *realClock  { return &realClock{base: processClockBase} }
func (c *realClock) Now() int64 { return time.Since(c.base).Nanoseconds() }

// NewMonotonicClock uses the same origin as default caches and cost models.
func NewMonotonicClock() Clock { return newRealClock() }

// ManualClock 是测试用的手动时钟。
type ManualClock struct{ t atomic.Int64 }

// NewManualClock 创建从 start 纳秒开始的手动时钟。
func NewManualClock(start int64) *ManualClock {
	m := &ManualClock{}
	m.t.Store(start)
	return m
}

// Now 实现 Clock。
func (m *ManualClock) Now() int64 { return m.t.Load() }

// Advance 推进 d。
func (m *ManualClock) Advance(d time.Duration) { m.t.Add(int64(d)) }

// Set 直接设置当前时间。
func (m *ManualClock) Set(ns int64) { m.t.Store(ns) }
