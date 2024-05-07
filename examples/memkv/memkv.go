package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/c4pt0r/kvql"
)

type MemKV struct {
	data map[string][]byte
	// should use a sorted map or tree to maintain order
	// but for simplicity, we use a slice to maintain order
	orderedKeys []string
	mu          sync.RWMutex
}

type MemKVCursor struct {
	keys  []string
	index int
	data  map[string][]byte
}

func NewMemKV() *MemKV {
	return &MemKV{
		data:        make(map[string][]byte),
		orderedKeys: make([]string, 0),
	}
}

var _ kvql.Storage = (*MemKV)(nil)
var _ kvql.Cursor = (*MemKVCursor)(nil)

func (m *MemKV) Get(key []byte) (value []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.data[string(key)]
	if !ok {
		return nil, nil // Return nil if the key does not exist
	}
	return value, nil
}

func (m *MemKV) Put(key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	strKey := string(key)
	if _, exists := m.data[strKey]; !exists {
		m.orderedKeys = append(m.orderedKeys, strKey)
		sort.Strings(m.orderedKeys) // Maintain order after insertion
	}
	m.data[strKey] = value
	return nil
}

func (m *MemKV) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	strKey := string(key)
	if _, exists := m.data[strKey]; exists {
		delete(m.data, strKey)
		i := sort.SearchStrings(m.orderedKeys, strKey)
		m.orderedKeys = append(m.orderedKeys[:i], m.orderedKeys[i+1:]...)
	}
	return nil
}

func (m *MemKV) BatchPut(kvs []kvql.KVPair) error {
	for _, kv := range kvs {
		m.Put(kv.Key, kv.Value)
	}
	return nil
}

func (m *MemKV) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		m.Delete(key)
	}
	return nil
}

func (m *MemKV) Cursor() (cursor kvql.Cursor, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &MemKVCursor{data: m.data, keys: m.orderedKeys, index: -1}, nil
}

func (c *MemKVCursor) Seek(prefix []byte) error {
	c.index = sort.SearchStrings(c.keys, string(prefix))
	if c.index < len(c.keys) && strings.HasPrefix(c.keys[c.index], string(prefix)) {
		return nil
	}
	c.index = len(c.keys)
	return nil
}

func (c *MemKVCursor) Next() (key []byte, value []byte, err error) {
	if c.index < 0 || c.index >= len(c.keys) {
		return nil, nil, nil
	}
	keyStr := c.keys[c.index]
	value = c.data[keyStr]
	c.index++
	return []byte(keyStr), value, nil
}

func (c *MemKVCursor) Close() error {
	// No resources to release in this simple cursor
	return nil
}

func repl(storage kvql.Storage) {
	buf := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("kvql> ")
		query, err := buf.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		query = strings.TrimSpace(query)

		opt := kvql.NewOptimizer(query)
		plan, err := opt.BuildPlan(storage)
		if err != nil {
			fmt.Println("Error building plan:", err)
			continue
		}

		execCtx := kvql.NewExecuteCtx()
		for {
			rows, err := plan.Batch(execCtx)
			if err != nil {
				fmt.Println("Error executing plan:", err)
				break
			}
			if len(rows) == 0 {
				break
			}
			execCtx.Clear()
			for _, row := range rows {
				for _, col := range row {
					switch col := col.(type) {
					case int, int32, int64:
						fmt.Printf("%d ", col)
					case []byte:
						fmt.Printf("%s ", string(col))
					default:
						fmt.Printf("%v ", col)
					}
				}
				fmt.Println()
			}
		}
	}
}

func main() {
	kv := NewMemKV()
	// put some test data
	kv.Put([]byte("a"), []byte("1"))
	kv.Put([]byte("a1"), []byte("2"))
	kv.Put([]byte("a2"), []byte("3"))
	kv.Put([]byte("a3"), []byte("4"))
	kv.Put([]byte("b"), []byte("2"))
	kv.Put([]byte("c"), []byte("3"))

	repl(kv)
}
