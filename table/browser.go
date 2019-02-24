package table

import (
	"sort"

	"github.com/asdine/genji/record"
)

type Browser struct {
	Reader

	err error
}

func NewBrowser(t Reader) Browser {
	return Browser{
		Reader: t,
	}
}

func (b Browser) Err() error {
	return b.err
}

func (b Browser) ForEach(fn func(rowid []byte, r record.Record) error) Browser {
	if b.err != nil {
		return b
	}

	err := b.Iterate(func(rowid []byte, r record.Record) error {
		return fn(rowid, r)
	})

	if err != nil && b.err == nil {
		b.err = err
	}

	return b
}

func (b Browser) Filter(fn func(rowid []byte, r record.Record) (bool, error)) Browser {
	var rb RecordBuffer

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		ok, err := fn(rowid, r)
		if err != nil {
			return err
		}

		if ok {
			rb.Insert(r)
		}

		return nil
	})

	if b.err == nil {
		b.Reader = &rb
	}

	return b
}

func (b Browser) Map(fn func(rowid []byte, r record.Record) (record.Record, error)) Browser {
	var rb RecordBuffer

	b = b.ForEach(func(rowid []byte, r record.Record) error {
		r, err := fn(rowid, r)
		if err != nil {
			return err
		}

		rb.Insert(r)
		return nil
	})

	if b.err == nil {
		b.Reader = &rb
	}

	return b
}

func (b Browser) GroupBy(fieldName string) BrowserGroup {
	var g BrowserGroup

	if b.err != nil {
		g.err = b.err
		return g
	}

	m := make(map[string]*RecordBuffer)
	var values []string

	tr := b.ForEach(func(rowid []byte, r record.Record) error {
		f, err := r.Field(fieldName)
		if err != nil {
			return err
		}

		k := string(f.Data)
		tr, ok := m[k]
		if !ok {
			tr = new(RecordBuffer)
			m[k] = tr
			values = append(values, k)
		}

		tr.Insert(r)
		return nil
	})

	if err := tr.Err(); err != nil {
		g.err = err
		return g
	}

	sort.Strings(values)

	for _, v := range values {
		g.Readers = append(g.Readers, NewBrowser(m[v]))
	}

	return g
}

func (b Browser) Chunk(n int) BrowserGroup {
	var g BrowserGroup

	i := 0
	var fb RecordBuffer
	b = b.ForEach(func(rowid []byte, r record.Record) error {
		if i%n == 0 {
			fb = RecordBuffer{}
			g.Readers = append(g.Readers, NewBrowser(&fb))
		}

		fb.Insert(r)
		i++
		return nil
	})

	if b.err != nil {
		g.err = b.err
	}

	return g
}

func (b Browser) Count() (int, error) {
	if b.err != nil {
		return 0, b.err
	}

	counter := 0
	b = b.ForEach(func(rowid []byte, r record.Record) error {
		counter++
		return nil
	})

	return counter, b.err
}

type BrowserGroup struct {
	Readers []Browser
	err     error
}

func (g BrowserGroup) Err() error {
	return g.err
}

func (g BrowserGroup) Concat() Browser {
	var b Browser

	if g.err != nil {
		b.err = g.err
		return b
	}

	var fb RecordBuffer

	for _, r := range g.Readers {
		err := fb.InsertFrom(r)
		if err != nil {
			b.err = err
			return b
		}
	}

	b.Reader = &fb
	return b
}
