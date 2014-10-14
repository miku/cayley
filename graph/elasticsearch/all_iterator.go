package elasticsearch

import (
	"fmt"
	"log"
	"net/url"

	"github.com/belogik/goes"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func init() {
	esType = graph.RegisterIterator("es")
}

type AllIterator struct {
	uid       uint64      // uniq id of the iterator
	qs        *QuadStore  // quadstore
	position  uint64      // the current position in the iterator
	batchSize uint64      // the number of docs to retrieve in one request
	size      uint64      // the total number of docs
	result    graph.Value // the current result
	batch     []string    // currently retrieved batch
	bpos      uint64      // position inside current batch
	conn      *goes.Connection
	query     map[string]interface{}
	tags      graph.Tagger
}

func newAllIterator(qs *QuadStore) *AllIterator {
	conn := goes.NewConnection("localhost", "9200")
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	r, err := conn.Search(query, []string{qs.indexName}, []string{qs.nodeType}, url.Values{})
	if err != nil {
		log.Fatalln(err)
	}

	return &AllIterator{
		uid:       iterator.NextUID(),
		qs:        qs,
		position:  0,
		batchSize: 10,
		size:      r.Hits.Total,
		conn:      conn,
		query:     query,
	}
}

func (it *AllIterator) Next() bool {
	if it.position == it.size {
		return false
	}
	if it.position%it.batchSize == 0 {
		// request more data
		r, err := it.conn.Search(it.query, []string{it.qs.indexName}, []string{it.qs.nodeType}, url.Values{})
		if err != nil {
			log.Fatalln(err)
		}
		var batch []string
		for _, hit := range r.Hits.Hits {
			batch = append(batch, hit.Source["name"].(string))
		}
		it.batch = batch
		it.bpos = 0
	}
	if uint64(len(it.batch)) > it.bpos {
		it.result = it.batch[it.bpos]
		return true
	}
	return false
}

func (it *AllIterator) Tagger() *graph.Tagger {
	log.Println("calling Tagger")
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	log.Println("calling TagResults")
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Contains(v graph.Value) bool {
	log.Println("calling Contains")
	return graph.ContainsLogOut(it, v, false)
}

func (it *AllIterator) Result() graph.Value {
	return it.result
}

func (it *AllIterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *AllIterator) NextPath() bool {
	return false
}

func (it *AllIterator) Clone() graph.Iterator {
	return NewAllIterator(it.qs)
}

func (it *AllIterator) Type() graph.Type {
	return graph.All
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.position = 0
}

func (it *AllIterator) Close() {}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Size() (int64, bool) {
	return int64(it.size), true
}

func (it *AllIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *AllIterator) Sorted() bool { return false }

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("No name yet."),
		Type: it.Type(),
		Size: size,
	}
}

func (it *AllIterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}
