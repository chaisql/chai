package regexcache

type regexElement struct {
	next, prev *regexElement
	entry      entry
}

// regexList is a simple linked list.
type regexList struct {
	root regexElement
	len  int
}

func newRegexList() *regexList {
	l := &regexList{}
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

// front returns the first element of list l or nil if the list is empty.
func (l *regexList) front() *regexElement {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

// back returns the last element of list l or nil if the list is empty.
func (l *regexList) back() *regexElement {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// insert inserts e after at, increments l.len, and returns e.
func (l *regexList) insert(e, at *regexElement) *regexElement {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	l.len++
	return e
}

// insertValue is a convenience wrapper for insert(&regexElement{Value: v}, at).
func (l *regexList) insertValue(v entry, at *regexElement) *regexElement {
	return l.insert(&regexElement{entry: v}, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *regexList) remove(e *regexElement) *regexElement {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	l.len--
	return e
}

// move moves e to next to at and returns e.
func (l *regexList) move(e, at *regexElement) *regexElement {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

// pushFront inserts a new element e with value v at the front of list l and returns e.
func (l *regexList) pushFront(v entry) *regexElement {
	return l.insertValue(v, &l.root)
}

// moveToFront moves element e to the front of list l.
func (l *regexList) moveToFront(e *regexElement) {
	if l.root.next == e {
		return
	}
	l.move(e, &l.root)
}
