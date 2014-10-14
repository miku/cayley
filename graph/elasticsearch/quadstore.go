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
	indexName string
	quadType  string
	nodeType  string
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

// createNewIndex just PUTs a new elasticsearch index
func createNewIndex(_ string, _ graph.Options) error {
	// TODO: get options from config
	log.Println("creating new es index cayley")
	req, _ := http.NewRequest("PUT", "http://localhost:9200/cayley", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// newQuadStore is a dummy for now, struct should hold config (host, port, index, ...) later
func newQuadStore(_ string, _ graph.Options) (graph.QuadStore, error) {
	// TODO: get options from config
	qs := QuadStore{indexName: "cayley", quadType: "quads", nodeType: "nodes"}
	log.Printf("initialized new quad store: %+v\n", qs)
	return &qs, nil
}

// DocumentCount return the total number of quads indexed
func (qs *QuadStore) DocumentCount() int64 {
	url := fmt.Sprintf("http://localhost:9200/%s/%s/_count", qs.indexName, qs.quadType)
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
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

// GetQuadForID returns a Quad from the index for a given id (content-address)
func (qs *QuadStore) GetQuadForID(id string) (quad.Quad, error) {
	url := fmt.Sprintf("http://localhost:9200/%s/%s/%s", qs.indexName, qs.quadType, id)
	resp, err := http.Get(url)
	if err != nil {
		return quad.Quad{}, err
	}
	defer resp.Body.Close()
	var doc Document
	d := json.NewDecoder(resp.Body)
	if err := d.Decode(&doc); err != nil {
		return quad.Quad{}, err
	}

	q := quad.Quad{Subject: doc.Subject,
		Predicate: doc.Predicate,
		Object:    doc.Object,
		Label:     doc.Label}
	return q, nil
}

func (qs *QuadStore) IndexNode(node string) error {
	doc := map[string]string{
		"name": node,
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	id := hashOf(node)
	url := fmt.Sprintf("http://localhost:9200/%s/%s/%s", qs.indexName, qs.nodeType, id)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// log.Printf("indexed node: %s\n", id)
	return nil
}

// IndexQuad indexes a quad into the given index/docType ""
func (qs *QuadStore) IndexQuad(q quad.Quad) error {
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
	id := qs.GetIDForQuad(q)
	url := fmt.Sprintf("http://localhost:9200/%s/%s/%s", qs.indexName, qs.quadType, id)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// log.Printf("indexed quad: %s\n", id)
	return nil
}

func (qs *QuadStore) GetIDForQuad(t quad.Quad) string {
	id := hashOf(t.Subject)
	id += hashOf(t.Predicate)
	id += hashOf(t.Object)
	id += hashOf(t.Label)
	return id
}

// Size returns the number of quad stored
func (qs *QuadStore) Size() int64 {
	log.Println("calling Size")
	return qs.DocumentCount()
}

// ApplyDeltas just indexes quads into the index (no log for now)
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta) error {
	for i, d := range deltas {
		fmt.Println(i)
		// TODO: batch updates plus parallel indexing
		qs.IndexQuad(d.Quad)
		qs.IndexNode(d.Quad.Subject)
		qs.IndexNode(d.Quad.Predicate)
		qs.IndexNode(d.Quad.Object)
		qs.IndexNode(d.Quad.Label)
	}
	return nil
}

// Quad returns a quad for a document ID (hashed)
func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	q, err := qs.GetQuadForID(k.(string))
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
	q, err := qs.GetQuadForID(val.(string))
	if err != nil {
		log.Fatal(err)
	}
	return q.Get(d)
}
