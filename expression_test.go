package kvql

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

var benchmarkChunkSize = 32

type mockQueryStorage struct {
	data []KVPair
}

func newMockQueryStorage(data []KVPair) *mockQueryStorage {
	sort.Slice(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) < 0
	})
	return &mockQueryStorage{
		data: data,
	}
}

func (t *mockQueryStorage) Get(key []byte) ([]byte, error) {
	for _, kvp := range t.data {
		if bytes.Equal(kvp.Key, key) {
			return kvp.Value, nil
		}
	}
	return nil, nil
}

func (t *mockQueryStorage) Put(key []byte, value []byte) error {
	return nil
}

func (t *mockQueryStorage) BatchPut(kvs []KVPair) error {
	return nil
}

func (t *mockQueryStorage) Delete(key []byte) error {
	return nil
}

func (t *mockQueryStorage) BatchDelete(key [][]byte) error {
	return nil
}

func (t *mockQueryStorage) Cursor() (Cursor, error) {
	return &mockCursor{
		data:   t.data,
		idx:    0,
		length: len(t.data),
	}, nil
}

type mockCursor struct {
	data   []KVPair
	idx    int
	length int
}

func (c *mockCursor) Seek(key []byte) error {
	for c.idx < c.length {
		row := c.data[c.idx]
		if bytes.Compare(row.Key, key) >= 0 {
			break
		}
		c.idx++
	}
	return nil
}

func (c *mockCursor) Next() (key []byte, val []byte, err error) {
	if c.idx >= c.length {
		return nil, nil, nil
	}
	ret := c.data[c.idx]
	c.idx++
	return ret.Key, ret.Value, nil
}

func generateChunk(size int) []KVPair {
	ret := make([]KVPair, size)
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("%d", i)
		ret[i] = NewKVPStr(key, val)
	}
	return ret
}

func BenchmarkExpressionEvalVec(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	ctx := NewExecuteCtx()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err = exec.filterChunk(chunk, ctx)
		if err != nil {
			b.Fatal(err)
		}
		ctx.Clear()
	}
}

func BenchmarkExpressionEval(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	ctx := NewExecuteCtx()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < len(chunk); i++ {
			_, err = exec.Filter(chunk[i], ctx)
			if err != nil {
				b.Fatal(err)
			}
			ctx.Clear()
		}
	}
}

func BenchmarkExpressionEvalHalfVec(b *testing.B) {
	chunk := generateChunk(benchmarkChunkSize)
	query := "where key ^= 'key-1' & int(value) + int(value) * 8 > 10"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		b.Fatal(err)
	}
	ctx := NewExecuteCtx()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err = exec.filterBatch(chunk, ctx)
		if err != nil {
			b.Fatal(err)
		}
		ctx.Clear()
	}
}

func BenchmarkQuery(b *testing.B) {
	query := "select sum(int(value)) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' group by kk, ival order by ival desc"
	data := generateChunk(1000)
	qtxn := newMockQueryStorage(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRows(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryBatch(b *testing.B) {
	query := "select sum(int(value)) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' group by kk, ival order by ival desc"
	data := generateChunk(1000)
	qtxn := newMockQueryStorage(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRowsBatch(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySimple(b *testing.B) {
	query := "select int(value) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' limit 100"
	data := generateChunk(1000)
	qtxn := newMockQueryStorage(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRows(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySimpleBatch(b *testing.B) {
	query := "select int(value) * 2, key + '_' + 'end' as kk, int(value) as ival where key between 'k' and 'l' limit 100"
	data := generateChunk(1000)
	qtxn := newMockQueryStorage(data)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		opt := NewOptimizer(query)
		plan, err := opt.BuildPlan(qtxn)
		if err != nil {
			b.Fatal(err)
		}
		err = getRowsBatch(plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func getRows(plan FinalPlan) error {
	ctx := NewExecuteCtx()
	for {
		cols, err := plan.Next(ctx)
		if err != nil {
			return err
		}
		if cols == nil {
			break
		}
		ctx.Clear()
	}
	return nil
}

func getRowsBatch(plan FinalPlan) error {
	ctx := NewExecuteCtx()
	for {
		rows, err := plan.Batch(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		ctx.Clear()
	}
	return nil
}
