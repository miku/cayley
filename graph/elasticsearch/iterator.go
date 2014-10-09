package elasticsearch

import (
	"fmt"
	"log"
	"net/url"

	"github.com/belogik/goes"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func init() {
	esType = graph.RegisterIterator("es")
}

var esType graph.Type

type Iterator struct {
	uid      uint64
	tags     graph.Tagger
	offset   int64
	hash     string
	name     string
	qs       *QuadStore
	size     int64
	result   graph.Value
	isAll    bool
	response goes.Response
	quads    []quad.Quad
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
	// hit := <-it.hits
	// log.Printf("<-hit: %+v\n", hit)
	// it.result = fmt.Sprintf("%s", hit.Source)
	// // it.result = fmt.Sprintf("result at offset: %d", it.offset)
	// if it.offset < 10 {
	// 	it.offset++
	// 	return true
	// }
	// return false
	if int64(len(it.quads)) > it.offset {
		it.result = fmt.Sprintf("%+v", it.quads[it.offset])
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

func NewIterator(qs *QuadStore, index string, d quad.Direction, val graph.Value) *Iterator {
	log.Printf("calling NewIterator(%+v, %s, %+v, %+v)\n", qs, index, d, val)
	conn := goes.NewConnection("localhost", "9200")
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				fmt.Sprintf("%s", string(d.Prefix())): fmt.Sprintf("%s", val.(string)),
			},
		},
	}
	r, err := conn.Search(query, []string{"cayley"}, []string{}, url.Values{})
	if err != nil {
		log.Fatalln(err)
	}

	var quads []quad.Quad

	for _, hit := range r.Hits.Hits {
		q := quad.Quad{Subject: hit.Source["s"].(string),
			Predicate: hit.Source["p"].(string),
			Object:    hit.Source["o"].(string),
			Label:     hit.Source["c"].(string)}
		quads = append(quads, q)
	}

	log.Printf("found: %d\n", len(quads))

	return &Iterator{
		uid:      iterator.NextUID(),
		qs:       qs,
		size:     int64(len(quads)),
		hash:     "",
		isAll:    true,
		response: r,
		quads:    quads,
	}
}

func NewAllIterator(qs *QuadStore) *Iterator {
	log.Printf("calling NewAllIterator(%v)\n", qs)

	conn := goes.NewConnection("localhost", "9200")
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	r, err := conn.Search(query, []string{"cayley"}, []string{}, url.Values{})
	if err != nil {
		log.Fatalln(err)
	}

	var quads []quad.Quad

	for _, hit := range r.Hits.Hits {
		q := quad.Quad{Subject: hit.Source["s"].(string),
			Predicate: hit.Source["p"].(string),
			Object:    hit.Source["o"].(string),
			Label:     hit.Source["c"].(string)}
		quads = append(quads, q)
	}

	log.Printf("found: %d\n", len(quads))

	return &Iterator{
		uid:      iterator.NextUID(),
		qs:       qs,
		size:     int64(len(quads)),
		hash:     "",
		isAll:    true,
		response: r,
		quads:    quads,
	}
}
