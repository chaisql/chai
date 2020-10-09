package regexcache

import "regexp"

type entry struct {
	key   string
	value *regexp.Regexp
}
