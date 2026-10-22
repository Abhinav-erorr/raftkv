package kv_test

import (
	"testing"

	"raftkv/kv"
)

func TestStoreCRUD(t *testing.T) {
	store := kv.NewStore()
	store.Put("a", "1")
	if got, ok := store.Get("a"); !ok || got != "1" {
		t.Fatalf("Get(a) = %q, %v", got, ok)
	}
	store.Delete("a")
	if _, ok := store.Get("a"); ok {
		t.Fatal("deleted key still exists")
	}
}
