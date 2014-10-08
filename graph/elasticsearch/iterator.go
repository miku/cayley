package elasticsearch

import (
	"fmt"
	"log"

	"github.com/belogik/goes"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func init() {
	esType = graph.RegisterIterator("es")
}

var esType graph.Type

type Iterator struct {
	uid          uint64
	tags         graph.Tagger
	offset       int64
	hash         string
	name         string
	qs           *QuadStore
	size         int64
	result       graph.Value
	isAll        bool
	scanResponse goes.Response
	hits         chan goes.Hit
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.offset = 0
}

func (it *Iterator) Close() {
}

func (it *Iterator) Clone() graph.Iterator {
	return NewAllIterator(it.qs)
}

func (it *Iterator) Tagger() *graph.Tagger {
	log.Println("calling Tagger")
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	log.Println("calling TagResults")
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Next() bool {
	log.Println("calling Next")
	// fmt.Println(<-it.hits)
	hit := <-it.hits
	it.result = fmt.Sprintf("%s", hit.Source)
	// it.result = fmt.Sprintf("result at offset: %d", it.offset)
	if it.offset < 10 {
		it.offset++
		return true
	}
	return false
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	log.Println("calling ResultTree")
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) Result() graph.Value {
	log.Println("calling Result")
	return it.result
}

func (it *Iterator) NextPath() bool {
	log.Println("calling NextPath")
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	log.Println("calling SubIterators")
	return nil
}

func (it *Iterator) Contains(v graph.Value) bool {
	log.Println("calling Contains")
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	log.Println("calling Size")
	return it.size, true
}

func Type() graph.Type { return esType }

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return esType
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("%s/%s", it.name, it.hash),
		Type: it.Type(),
		Size: size,
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

func scrollWrap(r goes.Response) chan goes.Hit {
	hits := make(chan goes.Hit)
	conn := goes.NewConnection("localhost", "9200")
	go func() {
		for {
			scrollResponse, err := conn.Scroll(r.ScrollId, "30")
			if err != nil {
				log.Fatalln(err)
			}
			if len(scrollResponse.Hits.Hits) == 0 {
				break
			}
			for _, hit := range scrollResponse.Hits.Hits {
				hits <- hit
			}
		}
		close(hits)
	}()
	return hits
}

func NewAllIterator(qs *QuadStore) *Iterator {
	size := qs.Size()

	// contruct a scan query
	conn := goes.NewConnection("localhost", "9200")
	var query map[string]interface{}
	query = map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	r, err := conn.Scan(query, []string{"cayley"}, []string{""}, "30", 10000)
	if err != nil {
		log.Fatalln(err)
	}

	return &Iterator{
		uid:          iterator.NextUID(),
		qs:           qs,
		size:         int64(size),
		hash:         "",
		isAll:        true,
		scanResponse: r,
		hits:         scrollWrap(r),
	}
}
