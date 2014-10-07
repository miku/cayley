package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"net/http"
	"sync"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterQuadStore("es", true, newQuadStore, createNewIndex)
}

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize = sha1.Size
)

func hashOf(s string) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return key
}

func (qs *QuadStore) createKeyFor(d [4]quad.Direction, q quad.Quad) []byte {
	key := make([]byte, 0, 2+(hashSize*3))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{d[0].Prefix(), d[1].Prefix()}...)
	key = append(key, hashOf(q.Get(d[0]))...)
	key = append(key, hashOf(q.Get(d[1]))...)
	key = append(key, hashOf(q.Get(d[2]))...)
	key = append(key, hashOf(q.Get(d[3]))...)
	return key
}

func (qs *QuadStore) createValueKeyFor(s string) []byte {
	key := make([]byte, 0, 1+hashSize)
	key = append(key, []byte("z")...)
	key = append(key, hashOf(s)...)
	return key
}

type Token []byte

func (t Token) Key() interface{} {
	return string(t)
}

type QuadStore struct {
	name string
}

func createNewIndex(_ string, _ graph.Options) error {
	http.NewRequest("PUT", "http://localhost:9200/cayley", nil)
	return nil
}

func newQuadStore(_ string, _ graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	qs.name = "cayley"
	return &qs, nil
}

func (qs *QuadStore) Size() int64 {
	req, _ := http.Get("http://localhost:9200/cayley/_count")
	var c map[string]interface{}
	d := json.NewDecoder(req.Body)
	d.UseNumber()

	if err := d.Decode(&c); err != nil {
		log.Println(err)
		return 0
	}

	switch t := c["count"].(type) {
	case json.Number:
		n, err := t.Int64()
		if err == nil {
			return n
		}
	}
	return 0
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta) error {
	return nil
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var q quad.Quad
	return q
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return nil
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return nil
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return nil
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return Token(qs.createValueKeyFor(s))
}

func (qs *QuadStore) NameOf(k graph.Value) string {
	return "noname"
}

func (qs *QuadStore) Horizon() int64 {
	return 0
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.(Token), b.(Token))
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareBytes)
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return it, false
}

func (qs *QuadStore) Close() {
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	return nil
}

func main() {
	qs, _ := newQuadStore("", nil)
	fmt.Println(qs.Size())
}
