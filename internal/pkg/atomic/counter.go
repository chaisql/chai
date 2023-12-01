package atomic

import (
	"sync/atomic"
)

type Counter struct {
	max  int64
	min  int64
	wrap bool

	current atomic.Int64
}

func NewCounter(min, max int64, wrap bool) *Counter {
	c := Counter{
		max:  max,
		min:  min,
		wrap: wrap,
	}

	c.current.Add(min)
	return &c
}

func (c *Counter) Get() int64 {
	return c.current.Load()
}

// Incr increments the counter by 1. If the counter is at the Max value, it wraps around to Min.
func (c *Counter) Incr() int64 {
	var next int64

	for {
		prev := c.current.Load()
		next = prev + 1

		if next >= c.max {
			if !c.wrap {
				return prev
			}

			next = c.min
		}

		if c.current.CompareAndSwap(prev, next) {
			break
		}
	}

	return next
}

func (c *Counter) Decr() int64 {
	var next int64

	for {
		prev := c.current.Load()
		next = prev - 1

		if next < c.min {
			if !c.wrap {
				return prev
			}

			next = c.max
		}

		if c.current.CompareAndSwap(prev, next) {
			break
		}
	}

	return next
}
