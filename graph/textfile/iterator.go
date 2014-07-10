package textfile

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"log"
)

var textType graph.Type

type Iterator struct {
	iterator.Base
	ts *TripleStore
}

func (it *Iterator) Clone() graph.Iterator {
	clone := NewSimpleIterator(it.ts)
	return clone
}

func (it *Iterator) Close() {
}

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, true
}

func init() {
	textType = graph.Register("text")
}

func Type() graph.Type { return textType }

func (it *Iterator) Type() graph.Type {
	return textType
}

func NewSimpleIterator(ts *TripleStore) *Iterator {
	log.Println("STUB: NewSimpleIterator")
	var it Iterator
	iterator.BaseInit(&it.Base)
	it.ts = ts
	return &it
}
