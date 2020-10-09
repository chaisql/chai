package regexcache

import (
	"regexp"
)

const DefaultCacheSize = 128

// LRU is a Regexp LRU cache.
type LRU struct {
	size      int
	evictList *regexList
	items     map[string]*regexElement
}

func NewLRU(size int) *LRU {
	if size < 0 {
		size = DefaultCacheSize
	}
	return &LRU{
		size:      size,
		evictList: newRegexList(),
		items:     make(map[string]*regexElement, size),
	}
}

func (c *LRU) Compile(key string) (*regexp.Regexp, error) {
	value, ok := c.Get(key)
	if ok {
		return value, nil
	}

	r, err := regexp.Compile(key)
	if err != nil {
		return nil, err
	}
	c.Add(key, r)

	return r, nil
}

func (c *LRU) Add(key string, value *regexp.Regexp) {
	// Check for existing item
	if ent, ok := c.items[key]; ok {
		c.evictList.moveToFront(ent)
		ent.entry = entry{key, value}
		return
	}

	// Add new item
	ent := entry{key, value}
	entry := c.evictList.pushFront(ent)
	c.items[key] = entry

	// Verify size not exceeded
	if c.evictList.len > c.size {
		c.removeOldest()
	}
}

func (c *LRU) Get(key string) (value *regexp.Regexp, ok bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.moveToFront(ent)
		return ent.entry.value, true
	}
	return
}

func (c *LRU) Len() int {
	return c.size
}

func (c *LRU) removeOldest() {
	ent := c.evictList.back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func (c *LRU) removeElement(e *regexElement) {
	c.evictList.remove(e)
	kv := e.entry
	delete(c.items, kv.key)
}
