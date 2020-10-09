package regexcache

import "regexp"

// Cache is storage of compiled regular expressions.
type Cache interface {
	Add(pattern string, r *regexp.Regexp)
	Get(pattern string) (*regexp.Regexp, bool)
	Compile(pattern string) (*regexp.Regexp, error)
}
