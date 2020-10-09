package regexcache

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustCompile(pattern string) entry {
	return entry{
		key:   pattern,
		value: regexp.MustCompile(pattern),
	}
}

func TestRegexList(t *testing.T) {
	a := require.New(t)
	l := newRegexList()

	// check empty list
	a.Equal(l.front(), l.back())
	a.Zero(l.len)

	// add first element
	e := l.pushFront(mustCompile(".*"))
	a.Equal(l.front(), e)
	a.Equal(l.front(), l.back())
	a.Equal(1, l.len)

	// add second element to top
	e2 := l.pushFront(mustCompile("[a-z]"))
	a.Equal(l.front(), e2)
	a.Equal(l.back(), e)
	a.Equal(2, l.len)

	// move first added element to top
	l.moveToFront(e)
	a.Equal(l.front(), e)

	// delete first added element
	l.remove(e)
	a.Equal(l.front(), e2)
	a.Equal(l.front(), l.back())
	a.Equal(1, l.len)
}
