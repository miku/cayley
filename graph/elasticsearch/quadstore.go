package elasticsearch

// package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
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

type Token []byte

type QuadStore struct {
	name string
}

type Document struct {
	Subject   string `json:"s"`
	Predicate string `json:"p"`
	Object    string `json:"o"`
	Label     string `json:"c"`
}

func (t Token) Key() interface{} {
	return string(t)
}

// hashOf returns the hexdigest of a given string
func hashOf(s string) string {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return hex.EncodeToString(key)
}

// createDocId create a SHA1 hex id from a quad ("content-addressable")
func createDocId(q quad.Quad) string {
	s := fmt.Sprintf("%s%s%s%s", q.Subject, q.Predicate, q.Object, q.Label)
	return hashOf(s)
}

// createNewIndex just PUTs a new es index
func createNewIndex(_ string, _ graph.Options) error {
	log.Println("creating new es index cayley")
	req, _ := http.NewRequest("PUT", "http://localhost:9200/cayley", nil)
	_, err := http.DefaultClient.Do(req)
	return err
}

// newQuadStore is a dummy for now, struct should hold config (host, port, index, ...) later
func newQuadStore(_ string, _ graph.Options) (graph.QuadStore, error) {
	log.Println("creating new quad store")
	return &QuadStore{name: "cayley"}, nil
}

// documentCount return the total doc count for index/docType
func documentCount(index, docType string) int64 {
	url := fmt.Sprintf("http://localhost:9200/%s/%s/_count", index, docType)
	resp, _ := http.Get(url)
	defer resp.Body.Close()
	var c map[string]interface{}
	d := json.NewDecoder(resp.Body)
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

// getQuadForID returns a Quad from the index for a given id (content-address)
func getQuadForID(index, docType, id string) (quad.Quad, error) {
	url := fmt.Sprintf("http://localhost:9200/%s/%s/%s", index, docType, id)
	req, err := http.Get(url)
	if err != nil {
		return quad.Quad{}, err
	}
	var doc Document
	d := json.NewDecoder(req.Body)
	if err := d.Decode(&doc); err != nil {
		return quad.Quad{}, err
	}

	q := quad.Quad{Subject: doc.Subject,
		Predicate: doc.Predicate,
		Object:    doc.Object,
		Label:     doc.Label}
	return q, nil
}

// indexQuad indexes a quad into the given index/docType ""
func indexQuad(index, docType string, q quad.Quad) error {
	doc := map[string]string{
		"s": q.Subject,
		"p": q.Predicate,
		"o": q.Object,
		"c": q.Label,
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	id := createDocId(q)
	log.Printf("indexing %s\n", id)
	url := fmt.Sprintf("http://localhost:9200/%s/%s/%s", index, docType, id)
	_, err = http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	return nil
}

// Size returns the number of quad stored
func (qs *QuadStore) Size() int64 {
	log.Println("calling Size")
	return documentCount("cayley", "spoc")
}

// ApplyDeltas just indexes quads into the index (no log for now)
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta) error {
	for _, d := range deltas {
		// TODO: batch updates plus parallel indexing
		indexQuad("cayley", "spoc", d.Quad)
	}
	return nil
}

// Quad returns a quad for a document ID (hashed)
func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	q, err := getQuadForID("cayley", "spoc", k.(string))
	if err != nil {
		log.Fatal(err)
	}
	return q
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	log.Println("calling QuadIterator")
	return NewIterator(qs, "spoc", d, val)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	log.Println("calling NodesAllIterator")
	return NewAllIterator(qs)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	log.Printf("calling QuadsAllIterator()\n")
	return nil
}

// ValueOf returns the interal value (maybe we can just pass it on?)
func (qs *QuadStore) ValueOf(s string) graph.Value {
	log.Printf("calling ValueOf(%s)\n", s)
	return s
}

// NameOf turns a internal value to an external string (pass on?)
func (qs *QuadStore) NameOf(k graph.Value) string {
	log.Printf("calling NameOf(%+v)\n", k)
	return k.(string)
}

func (qs *QuadStore) Horizon() int64 {
	return 0
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.(Token), b.(Token))
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	log.Printf("calling FixedIterator()\n")
	// return iterator.NewFixed(compareBytes)
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	log.Printf("calling OptimizeIterator()\n")
	return it, false
}

func (qs *QuadStore) Close() {
	log.Printf("calling Close()\n")
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	q, err := getQuadForID("cayley", "spoc", val.(string))
	if err != nil {
		log.Fatal(err)
	}
	return q.Get(d)
}

func main() {
	qs := QuadStore{name: "cayley"}
	fmt.Println(qs.Size())
	fmt.Println(hashOf("Hello"))
	fact := quad.Quad{Subject: "Eos", Predicate: "daughter", Object: "Zeus"}
	id := createDocId(fact)
	fmt.Println(id)

}
