package client

import "raftkv/kv"

// Local is the initial client surface. It keeps the API independent of the
// eventual network protocol and leader routing implementation.
type Local struct{ store *kv.Store }

func NewLocal(store *kv.Store) *Local { return &Local{store: store} }

func (c *Local) Get(key string) (string, bool) { return c.store.Get(key) }
func (c *Local) Put(key, value string)         { c.store.Put(key, value) }
func (c *Local) Delete(key string)             { c.store.Delete(key) }
