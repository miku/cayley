// Dummy store that will persist triples to a single text file.
// Very slow, error-prone, only for API exploration!
package textfile

import (
	"bufio"
	"fmt"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type TripleStore struct {
	Path string
}

// CreateNewTripleStore touches the file given in `path`, nothing more.
func CreateNewTripleStore(path string) bool {
	log.Println("STUB: NewTripleStore")
	if _, err := os.Stat(path); err == nil {
		return true
	} else {
		_, err := os.Create(path)
		if err != nil {
			panic(err)
		}
	}
	return true
}

// NewTripleStore touches the file given in `path`, nothing more.
func NewTripleStore(path string) *TripleStore {
	log.Println("STUB: NewTripleStore")
	if _, err := os.Stat(path); err == nil {
		return &TripleStore{Path: path}
	} else {
		_, err := os.Create(path)
		if err != nil {
			panic(err)
		}
	}
	return &TripleStore{}
}

// Add a triple to the store.
func (ts *TripleStore) AddTriple(triple *graph.Triple) {
	log.Println("STUB: AddTriple")
	// open file and append at the end
	file, err := os.OpenFile(ts.Path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	text := fmt.Sprintf("%s\t%s\t%s\t%s\n", triple.Subject, triple.Predicate, triple.Object, triple.Provenance)
	if _, err = file.WriteString(text); err != nil {
		panic(err)
	}
	log.Printf("Added %s.\n", triple)
}

// Add a set of triples to the store, atomically if possible.
func (ts *TripleStore) AddTripleSet(triples []*graph.Triple) {
	log.Println("STUB: AddTripleSet")
	for _, triple := range triples {
		ts.AddTriple(triple)
	}
}

// Removes a triple matching the given one  from the file,
// if it exists. Does nothing otherwise.
func (ts *TripleStore) RemoveTriple(triple *graph.Triple) {
	log.Println("STUB: RemoveTriple")

	// input file
	file, err := os.Open(ts.Path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	// output file
	output, err := ioutil.TempFile("", "cayley-textfile-")
	if err != nil {
		panic(err)
	}
	defer output.Close()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		if !(parts[0] == triple.Subject &&
			parts[1] == triple.Predicate &&
			parts[2] == triple.Object &&
			parts[3] == triple.Provenance) {
			output.WriteString(line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}

// Given an opaque token, returns the triple for that token from the store.
func (ts *TripleStore) Triple(value graph.Value) *graph.Triple {
	log.Println("STUB: Triple")
	triple := graph.Triple{Subject: "Stub", Predicate: "requested", Object: fmt.Sprintf("%+v", value)}
	return &triple
}

// Given a direction and a token, creates an iterator of links which have
// that node token in that directional field.
func (ts *TripleStore) TripleIterator(d graph.Direction, value graph.Value) graph.Iterator {
	log.Println("STUB: TripleIterator")
	it := NewSimpleIterator(ts)
	return it
}

// Returns an iterator enumerating all nodes in the graph.
func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	log.Println("STUB: TripleIterator")
	it := NewSimpleIterator(ts)
	return it
}

// Returns an iterator enumerating all links in the graph.
func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	log.Println("STUB: TripleIterator")
	it := NewSimpleIterator(ts)
	return it
}

// Given a node ID, return the opaque token used by the TripleStore
// to represent that id.
func (ts *TripleStore) ValueOf(id string) graph.Value {
	log.Println("STUB: ValueOf")
	return nil
}

// Given an opaque token, return the node that it represents.
func (ts *TripleStore) NameOf(value graph.Value) string {
	log.Println("STUB: NameOf")
	return "Nameless"
}

// Returns the number of triples currently stored.
func (ts *TripleStore) Size() int64 {
	log.Println("STUB: Size")
	return 0
}

// Creates a fixed iterator which can compare Values
func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(iterator.BasicEquality)
}

// Optimize an iterator in the context of the triple store.
// Suppose we have a better index for the passed tree; this
// gives the TripleStore the oppotunity to replace it
// with a more efficient iterator.
func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	log.Println("STUB: OptimizeIterator")
	return it, true
}

// Close the triple store and clean up. (Flush to disk, cleanly
// sever connections, etc)
func (ts *TripleStore) Close() {
	log.Println("STUB: Close")
}

// Convienence function for speed. Given a triple token and a direction
// return the node token for that direction. Sometimes, a TripleStore
// can do this without going all the way to the backing store, and
// gives the TripleStore the opportunity to make this optimization.
//
// Iterators will call this. At worst, a valid implementation is
// ts.IdFor(ts.Triple(triple_id).Get(dir))
func (ts *TripleStore) TripleDirection(id graph.Value, d graph.Direction) graph.Value {
	log.Println("STUB: TripleDirection")
	return nil
}
