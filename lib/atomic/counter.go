package atomic

import (
	"sync/atomic"
)

type Counter struct {
	max   int64
	start int64

	current int64
}

func NewCounter(start, max int64) *Counter {
	return &Counter{
		max:     max,
		start:   start,
		current: start,
	}
}

func (c *Counter) Get() int64 {
	return atomic.LoadInt64(&c.current)
}

// Incr increments the counter by 1. If the counter is at the Max value, it wraps around to 0.
func (c *Counter) Incr() int64 {
	var next int64

	for {
		prev := atomic.LoadInt64(&c.current)
		next = prev + 1

		if next >= c.max {
			next = c.start
		}

		if atomic.CompareAndSwapInt64(&c.current, prev, next) {
			break
		}
	}

	return next
}
