package main

import (
	"bytes"
	"fmt"

	"github.com/c4pt0r/kvql"
	"github.com/tidwall/btree"
)

/*
MemKV should implement the following interfaces:

type kvql.Storage interface {
	...
}

type kvql.Cursor interface {
	...
}
*/

type MemKV struct {
	// BTree is thread-safe
	tr *btree.BTree
}

type MemKVCursor struct {
	tr   *btree.BTree
	iter *btree.Iter
	key  []byte
}

func NewMemKV() *MemKV {
	return &MemKV{tr: btree.New(func(a, b interface{}) bool {
		return bytes.Compare(a.(kvql.KVPair).Key, b.(kvql.KVPair).Key) < 0
	})}
}

var _ kvql.Storage = (*MemKV)(nil)
var _ kvql.Cursor = (*MemKVCursor)(nil)

func (m *MemKV) Get(key []byte) (value []byte, err error) {
	item := m.tr.Get(kvql.KVPair{Key: key})
	if item == nil {
		return nil, nil
	}
	return item.(kvql.KVPair).Value, nil
}

func (m *MemKV) Put(key []byte, value []byte) error {
	m.tr.Set(kvql.KVPair{Key: key, Value: value})
	return nil
}

func (m *MemKV) Delete(key []byte) error {
	m.tr.Delete(key)
	return nil
}

func (m *MemKV) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		m.tr.Delete(key)
	}
	return nil
}

func (m *MemKV) BatchPut(kvs []kvql.KVPair) error {
	for _, kv := range kvs {
		m.tr.Set(kv)
	}
	return nil
}

func (m *MemKV) Cursor() (cursor kvql.Cursor, err error) {
	return &MemKVCursor{tr: m.tr}, nil
}

func (c *MemKVCursor) Seek(prefix []byte) error {
	c.key = prefix
	c.iter = nil
	return nil
}

func (c *MemKVCursor) Next() (key []byte, value []byte, err error) {
	if c.iter == nil {
		iter := c.tr.Iter()
		c.iter = &iter
		if c.iter.Seek(kvql.KVPair{Key: c.key}) {
			item := c.iter.Item().(kvql.KVPair)
			return item.Key, item.Value, nil
		}
		return nil, nil, nil
	}
	if c.iter.Next() {
		item := c.iter.Item().(kvql.KVPair)
		return item.Key, item.Value, nil
	}
	return nil, nil, nil
}

func main() {
	// test memkv

	kv := NewMemKV()
	kv.Put([]byte("a"), []byte("1"))
	kv.Put([]byte("a1"), []byte("2"))
	kv.Put([]byte("a2"), []byte("3"))
	kv.Put([]byte("a3"), []byte("4"))
	kv.Put([]byte("b"), []byte("2"))
	kv.Put([]byte("c"), []byte("3"))

	cursor, _ := kv.Cursor()
	cursor.Seek([]byte("a1"))
	for {
		key, value, _ := cursor.Next()
		if key == nil {
			break
		}
		println(string(key), string(value))
	}

	println("-----------")

	var (
		query string       = "select * where key ^= 'a'"
		txn   kvql.Storage = kv
	)

	opt := kvql.NewOptimizer(query)
	plan, err := opt.BuildPlan(txn)
	if err != nil {
		panic(err)
	}

	execCtx := kvql.NewExecuteCtx()
	for {
		rows, err := plan.Batch(execCtx)
		if err != nil {
			panic(err)
		}
		if len(rows) == 0 {
			break
		}
		execCtx.Clear()
		for _, cols := range rows {
			fmt.Println(string(cols[0].([]byte)),
				string(cols[1].([]byte)))
		}
	}
}
