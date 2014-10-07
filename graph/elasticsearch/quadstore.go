package elasticsearch

// package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
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

	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
)

type Token []byte

type QuadStore struct {
	name string
}

func (t Token) Key() interface{} {
	return string(t)
}

func hashOf(s string) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return key
}

func hexDigest(b []byte) string {
	return base64.URLEncoding.EncodeToString(b)
}

func (qs *QuadStore) createDocId(q quad.Quad) string {
	s := fmt.Sprintf("%s:%s:%s:%s", q.Subject, q.Predicate, q.Object, q.Label)
	return hexDigest(hashOf(s))
}

func createNewIndex(_ string, _ graph.Options) error {
	log.Println("creating new es index cayley")
	req, err := http.NewRequest("PUT", "http://localhost:9200/cayley", nil)
	_, err = http.DefaultClient.Do(req)
	return err
}

func newQuadStore(_ string, _ graph.Options) (graph.QuadStore, error) {
	log.Println("creating new quad store")
	var qs QuadStore
	qs.name = "cayley"
	return &qs, nil
}

func (qs *QuadStore) Size() int64 {
	log.Println("requesting size")
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
	for i, d := range deltas {
		// TODO: batch updates
		doc := map[string]string{
			"s": d.Quad.Subject,
			"p": d.Quad.Predicate,
			"o": d.Quad.Object,
			"c": d.Quad.Label,
		}
		payload, err := json.Marshal(doc)
		if err != nil {
			log.Fatal(err)
		}
		id := qs.createDocId(d.Quad)
		fmt.Printf("%d, %s\n", i, id)
		url := fmt.Sprintf("http://localhost:9200/cayley/spoc/%s", id)
		_, err = http.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
	}
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
	return NewAllIterator(qs)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return nil
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return nil
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
	qs := QuadStore{name: "cayley"}
	fmt.Println(qs.Size())
	fmt.Println(hashOf("Hello"))
	fact := quad.Quad{Subject: "Eos", Predicate: "daughter", Object: "Zeus"}
	id := qs.createDocId(fact)
	fmt.Println(id)

}
