// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Elasticsearch 1.0+ backend (WIP)
package elasticsearch

import (
	"hash"
	"log"
	"net/url"

	"github.com/barakmich/glog"
	"github.com/belogik/goes"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

const DefaultIndexName = "cayley"

type TripleStore struct {
	conn   *goes.Connection
	hasher hash.Hash
}

func CreateNewElasticsearchIndex() bool {
	conn := goes.NewConnection("localhost", "9200")
	mapping := map[string]interface{}{
		"settings": map[string]interface{}{
			"index.number_of_shards":   5,
			"index.number_of_replicas": 0,
		},
		"mappings": map[string]interface{}{
			"_default_": map[string]interface{}{
				"_source": map[string]interface{}{
					"enabled": true,
				},
				"_all": map[string]interface{}{
					"enabled": false,
				},
			},
		},
	}

	resp, err := conn.CreateIndex(DefaultIndexName, mapping)
	if err != nil {
		glog.Fatalln(err)
	}

	return true
}

func NewTripleStore() *TripleStore {
	var ts TripleStore
	ts.conn = goes.NewConnection("localhost", "9200")
	ts.hasher = sha1.New()
	return &ts
}

func (ts *TripleStore) getIdForTriple(t *graph.Triple) string {
	id := ts.ConvertStringToByteHash(t.Subject)
	id += ts.ConvertStringToByteHash(t.Predicate)
	id += ts.ConvertStringToByteHash(t.Object)
	id += ts.ConvertStringToByteHash(t.Provenance)
	return id
}

func (ts *TripleStore) ConvertStringToByteHash(s string) string {
	ts.hasher.Reset()
	key := make([]byte, 0, ts.hasher.Size())
	ts.hasher.Write([]byte(s))
	key = ts.hasher.Sum(key)
	return hex.EncodeToString(key)
}

func (ts *TripleStore) writeTriple(t *graph.Triple) bool {
	tripledoc := goes.Document{
		Index: DefaultIndexName,
		Type:  "triple",
		Fields: map[string]interface{}{
			"_id": ts.getIdForTriple(t),
			"s":   t.Subject,
			"p":   t.Predicate,
			"o":   t.Object,
			"c":   t.Provenance,
		},
	}
	response, err := conn.Index(d, url.Values{})
	if err != nil {
		glog.Fatalln(err)
	}
	return true
}

func (ts *TripleStore) AddTriple(t *graph.Triple) {
	_ = ts.writeTriple(t)
}

func (ts *TripleStore) AddTripleSet(in []*graph.Triple) {
	ids := make(map[string]int)
	for _, t := range in {
		wrote := ts.writeTriple(t)
	}
}

func (ts *TripleStore) RemoveTriple(t *graph.Triple) {
	tripledoc := goes.Document{
		Index: DefaultIndexName,
		Type:  "triple",
		Id:    ts.getIdForTriple(t),
	}
	response, err := ts.conn.Delete(d, url.Values{})
	if err != nil {
		glog.Debug(err)
	}
}

func (ts *TripleStore) Triple(val graph.Value) *graph.Triple {
	response, err := ts.conn.Get(DefaultIndexName, "triple", val.(string), url.Values{})

	if err != nil {
		glog.Fatalln(err)
	}

	return &graph.Triple{
		reponse.Source["s"].(string),
		response["p"].(string),
		response["o"].(string),
		response["c"].(string),
	}
}

func (ts *TripleStore) TripleIterator(d graph.Direction, val graph.Value) graph.Iterator {
	return NewIterator(ts, "triples", d, val)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "nodes")
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "triples")
}

func (ts *TripleStore) ValueOf(s string) graph.Value {
	return ts.ConvertStringToByteHash(s)
}

func (ts *TripleStore) NameOf(v graph.Value) string {
	val, ok := ts.idCache.Get(v.(string))
	if ok {
		return val
	}
	var node MongoNode
	err := ts.db.C("nodes").FindId(v.(string)).One(&node)
	if err != nil {
		log.Println("Error: Couldn't retrieve node", v.(string), err)
	}
	ts.idCache.Put(v.(string), node.Name)
	return node.Name
}

func (ts *TripleStore) Size() int64 {
	count, err := ts.db.C("triples").Count()
	if err != nil {
		glog.Error("Error: ", err)
		return 0
	}
	return int64(count)
}

func compareStrings(a, b graph.Value) bool {
	return a.(string) == b.(string)
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareStrings)
}

func (ts *TripleStore) Close() {
	ts.db.Session.Close()
}

func (ts *TripleStore) TripleDirection(in graph.Value, d graph.Direction) graph.Value {
	// Maybe do the trick here
	var offset int
	switch d {
	case graph.Subject:
		offset = 0
	case graph.Predicate:
		offset = (ts.hasher.Size() * 2)
	case graph.Object:
		offset = (ts.hasher.Size() * 2) * 2
	case graph.Provenance:
		offset = (ts.hasher.Size() * 2) * 3
	}
	val := in.(string)[offset : ts.hasher.Size()*2+offset]
	return val
}

func (ts *TripleStore) BulkLoad(t_chan chan *graph.Triple) {
	ts.session.SetSafe(nil)
	for triple := range t_chan {
		ts.writeTriple(triple)
	}
	outputTo := bson.M{"replace": "nodes", "sharded": true}
	glog.Infoln("Mapreducing")
	job := mgo.MapReduce{
		Map: `function() {
      var len = this["_id"].length
      var s_key = this["_id"].slice(0, len / 4)
      var p_key = this["_id"].slice(len / 4, 2 * len / 4)
      var o_key = this["_id"].slice(2 * len / 4, 3 * len / 4)
      var c_key = this["_id"].slice(3 * len / 4)
      emit(s_key, {"_id": s_key, "Name" : this.Subject, "Size" : 1})
      emit(p_key, {"_id": p_key, "Name" : this.Predicate, "Size" : 1})
      emit(o_key, {"_id": o_key, "Name" : this.Object, "Size" : 1})
			if (this.Provenance != "") {
				emit(c_key, {"_id": c_key, "Name" : this.Provenance, "Size" : 1})
			}
    }
    `,
		Reduce: `
      function(key, value_list) {
	out = {"_id": key, "Name": value_list[0].Name}
	count = 0
	for (var i = 0; i < value_list.length; i++) {
	  count = count + value_list[i].Size

	}
	out["Size"] = count
	return out
      }
    `,
		Out: outputTo,
	}
	ts.db.C("triples").Find(nil).MapReduce(&job, nil)
	glog.Infoln("Fixing")
	ts.db.Run(bson.D{{"eval", `function() { db.nodes.find().forEach(function (result) {
    db.nodes.update({"_id": result._id}, result.value)
  }) }`}, {"args", bson.D{}}}, nil)

	ts.session.SetSafe(&mgo.Safe{})
}
