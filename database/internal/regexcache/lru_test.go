package regexcache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRU(t *testing.T) {
	regexps := [...]string{
		".*", // the least requested
		"[a-z]",
		"[A-z]", // the most requested
	}
	cache := NewLRU(len(regexps))
	a := require.New(t)

	for i, r := range regexps {
		for j := i + 1; j > 0; j-- {
			// request $(index + 1) times
			_, err := cache.Compile(r)
			a.NoError(err)
		}
	}

	a.Equal(len(regexps), cache.Len())
	// check order in cache queue.
	for i := range regexps {
		element := cache.evictList.back()
		a.Equal(regexps[i], element.entry.key)
		cache.evictList.remove(element)
	}
}
