package lock

import (
	"hash/maphash"
	"strconv"
	"sync"
	"testing"

	"golang.org/x/sync/errgroup"
)

const maxHash = 100
const nbRequests = 10_000

type hashMap struct {
	sync.Mutex

	Head *Head
}

type Head struct {
	sync.Mutex

	Name string
	Next *Head
	List *list
}

type ListHead struct {
	sync.Mutex
	List *list
}

type list struct {
	Next  *list
	Id    int
	Count int
}

func BenchmarkHashMapWriteDistinctObj(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var g errgroup.Group

		ch := make(chan int)

		var theMap [maxHash]hashMap

		// create 100 goroutines
		for n := 0; n < 100; n++ {
			id := n
			g.Go(func() error {
				for obj := range ch {
					customHashMap(&theMap, id, obj)
				}

				return nil
			})
		}

		b.StartTimer()
		for j := 0; j < nbRequests; j++ {
			ch <- j
		}
		close(ch)
		_ = g.Wait()
	}
}

func BenchmarkHashMapWriteSameObjects(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var g errgroup.Group

		ch := make(chan int)

		var theMap [maxHash]hashMap

		// create 100 goroutines
		for n := 0; n < 100; n++ {
			id := n
			g.Go(func() error {
				for obj := range ch {
					customHashMap(&theMap, id, obj)
				}

				return nil
			})
		}

		b.StartTimer()
		for j := 0; j < nbRequests; j++ {
			ch <- j % 10
		}
		close(ch)
		_ = g.Wait()
	}
}

func BenchmarkStdHashMapWriteDistinctObj(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var g errgroup.Group

		ch := make(chan int)
		theMap := map[string]*ListHead{}
		var mx sync.Mutex

		// create 100 goroutines
		for n := 0; n < 100; n++ {
			id := n
			g.Go(func() error {
				for obj := range ch {
					stdHashMap(theMap, &mx, id, obj)
				}

				return nil
			})
		}

		b.StartTimer()
		for j := 0; j < nbRequests; j++ {
			ch <- j
		}
		close(ch)
		_ = g.Wait()
	}
}

func BenchmarkStdHashMapWriteSameObj(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var g errgroup.Group

		ch := make(chan int)
		theMap := map[string]*ListHead{}
		var mx sync.Mutex

		// create 100 goroutines
		for n := 0; n < 100; n++ {
			id := n
			g.Go(func() error {
				for obj := range ch {
					stdHashMap(theMap, &mx, id, obj)
				}

				return nil
			})
		}

		b.StartTimer()
		for j := 0; j < nbRequests; j++ {
			ch <- j % 10
		}
		close(ch)
		_ = g.Wait()
	}
}

func customHashMap(theMap *[maxHash]hashMap, id, obj int) {
	var h maphash.Hash
	name := strconv.Itoa(obj)

	_, _ = h.WriteString(name)
	hash := h.Sum64()

	chain := &theMap[hash%maxHash]
	chain.Lock()
	head := chain.Head
	for head != nil && head.Name != name {
		head = head.Next
	}
	if head == nil {
		head = &Head{
			Name: name,
		}
		chain.Unlock()
		return
	}

	head.Lock()
	chain.Unlock()

	req := head.List
	for req != nil {
		if req.Id == id {
			break
		}
		req = req.Next
	}

	if req == nil {
		req = &list{
			Id: id,
		}
		head.List = req
		head.Unlock()
		return
	}

	req.Count++
	head.Unlock()
}

func stdHashMap(theMap map[string]*ListHead, mx *sync.Mutex, id, obj int) {
	name := strconv.Itoa(obj)
	mx.Lock()
	head, ok := theMap[name]
	if !ok {
		head = &ListHead{
			List: &list{
				Id: id,
			},
		}
		theMap[name] = head
		mx.Unlock()
		return
	}

	head.Lock()
	mx.Unlock()

	req := head.List
	for req != nil {
		if req.Id == id {
			break
		}
		req = req.Next
	}

	if req == nil {
		req = &list{
			Id: id,
		}
		head.List = req
		head.Unlock()
		return
	}

	req.Count++
	head.Unlock()
}
