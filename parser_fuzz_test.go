package kvql

import "testing"

type fuzzQueryStorage struct{}

func (t *fuzzQueryStorage) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (t *fuzzQueryStorage) Put(key []byte, value []byte) error {
	return nil
}

func (t *fuzzQueryStorage) BatchPut(kvs []KVPair) error {
	return nil
}

func (t *fuzzQueryStorage) Delete(key []byte) error {
	return nil
}

func (t *fuzzQueryStorage) BatchDelete(key [][]byte) error {
	return nil
}

func (t *fuzzQueryStorage) Cursor() (Cursor, error) {
	return &fuzzQueryCursor{}, nil
}

type fuzzQueryCursor struct{}

func (c *fuzzQueryCursor) Seek(key []byte) error {
	return nil
}

func (c *fuzzQueryCursor) Next() ([]byte, []byte, error) {
	return nil, nil, nil
}

func FuzzSQLParser(f *testing.F) {
	tests := []string{
		"select key, int(value) where int(key) + 1 >= 1 & (int(value) - 1 > 10 | int(value) <= 20)",
		"select key, int(value) where key ^= 'key' order by key limit 20, 10",
		"select * where key in ('k1', 'k2', 'k3')",
		"select * where (key between 'k1' and 'k3') & int(value) between 1 and 10",
		"select key, json(value)['test'] where key ^= 'k' & json(value)['test'][1] = 'v1'",
		"put ('k1', 'v1'), ('k1', 'V_' + upper(key)), ('k3', lower('V3'))",
		"remove 'k1', 'k2'",
		"delete where key ^='prefix' and value = 'v2'",
		"delete where key in ('k1', 'k2')",
		"delete where (key = 'k1' | key = 'k2') and key ^= 'k'",
	}

	for _, t := range tests {
		f.Add(t)
	}
	txn := &fuzzQueryStorage{}
	f.Fuzz(func(t *testing.T, query string) {
		o := NewOptimizer(query)
		o.buildPlan(txn)
	})
}
