package kvql

import (
	"bytes"
	"fmt"
	"testing"
)

func TestExec1(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "where key = 'test' & value = 'x'"
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	kv := NewKVPStr("test", "x")
	ok, err := exec.Filter(kv, ctx)
	if err != nil || !ok {
		t.Fatal(err)
	}
	fmt.Println(ok)
	ctx.Clear()
	kv = NewKVPStr("test", "z")
	ok, err = exec.Filter(kv, ctx)
	if err != nil || ok {
		t.Fatal(err)
	}
	fmt.Println(ok)
}

func TestExec2(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "where key ^= 'test' & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs, ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
	if fmt.Sprintf("%v", ret) != "[true true true false]" {
		t.Fatalf("Return got wrong: %v", ret)
	}
}

func TestExec3(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "where (key = 'test1' | key = 'test4') & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs, ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
	if fmt.Sprintf("%v", ret) != "[true false false false]" {
		t.Fatalf("Return got wrong: %v", ret)
	}
}

func TestExec4(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "where key != 'test1' & value ^= 'z'"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs, ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
	if fmt.Sprintf("%v", ret) != "[false true true false]" {
		t.Fatalf("Return got wrong: %v", ret)
	}
}

func TestExec5(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "where key in ('test1', 'test2')"
	kvs := []KVPair{
		NewKVPStr("test1", "z1"),
		NewKVPStr("test2", "z2"),
		NewKVPStr("test3", "z3"),
		NewKVPStr("test4", "x1"),
	}
	_, exec, err := BuildExecutor(query)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := exec.FilterBatch(kvs, ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(ret)
	if fmt.Sprintf("%v", ret) != "[true true false false]" {
		t.Fatalf("Return got wrong: %v", ret)
	}
}

type mockTxn struct {
	data []KVPair
}

func newMockTxn(kvs []KVPair) Txn {
	return &mockTxn{
		data: kvs,
	}
}

func (t *mockTxn) Get(key []byte) ([]byte, error) {
	for _, d := range t.data {
		if bytes.Equal(key, d.Key) {
			return d.Value, nil
		}
	}
	return nil, nil
}

func (t *mockTxn) Put(key []byte, value []byte) error {
	return nil
}

func (t *mockTxn) BatchPut(kvs []KVPair) error {
	return nil
}

func (t *mockTxn) Delete(key []byte) error {
	return nil
}

func (t *mockTxn) BatchDelete(key [][]byte) error {
	return nil
}

func (t *mockTxn) Cursor() (Cursor, error) {
	return &mockSmokeCursor{
		txn: t,
		idx: 0,
	}, nil
}

type mockSmokeCursor struct {
	txn *mockTxn
	idx int
}

func (c *mockSmokeCursor) Seek(prefix []byte) error {
	return nil
}

func (c *mockSmokeCursor) Next() ([]byte, []byte, error) {
	if c.idx >= len(c.txn.data) {
		return nil, nil, nil
	}
	kvp := c.txn.data[c.idx]
	c.idx += 1
	return kvp.Key, kvp.Value, nil
}

func TestExec6(t *testing.T) {
	ctx := NewExecuteCtx()
	query := "select key, value, int(split(value, '_')[1]) as sv where key ^= 'k' & sv > 10 & sv < 50"
	kvs := []KVPair{}
	for i := 0; i < 100; i++ {
		gkey := fmt.Sprintf("k%d", i+1)
		gval := fmt.Sprintf("%s_%d", gkey, i+1)
		kvs = append(kvs, NewKVPStr(gkey, gval))
	}
	txn := newMockTxn(kvs)
	opt := NewOptimizer(query)
	plan, err := opt.BuildPlan(txn)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := plan.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(rows)
	if len(rows) < PlanBatchSize {
		t.Fatal("Should more than PlanBatchSize")
	}
	if ctx.Hit < 1 {
		t.Fatal("Should has hits")
	} else {
		fmt.Println("Hits:", ctx.Hit)
	}
}
