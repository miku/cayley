TODO
====

* create a quadstore implementation

From: http://golang.org/doc/effective_go.html#init

> Finally, each source file can define its own niladic init function to set up whatever state is required.

    func init() {
        graph.RegisterQuadStore("leveldb", true, newQuadStore, createNewLevelDB)
    }

signature: `name string, persists bool, newFunc NewStoreFunc, initFunc InitStoreFunc`

* iterator and all_interators

Use elasticsearch API as directly as possible, e.g. via `http.Post` and `http.Get`.

Relevant ES facilities: bulk import in (parallel) batches, find by ID (hashed s-p-o), filter query.
