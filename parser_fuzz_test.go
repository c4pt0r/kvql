package kvql

import "testing"

func FuzzSQLParser(f *testing.F) {
	tests := []string{
		"select key, int(value) where int(key) + 1 >= 1 & (int(value) - 1 > 10 | int(value) <= 20)",
		"select key, int(value) where key ^= 'key' order by key limit 20, 10",
		"select * where key in ('k1', 'k2', 'k3')",
		"select * where (key between 'k1' and 'k3') & int(value) between 1 and 10",
		"select key, json(value)['test'] where key ^= 'k' & json(value)['test'][1] = 'v1'",
	}

	for _, t := range tests {
		f.Add(t)
	}
	f.Fuzz(func(t *testing.T, query string) {
		o := NewOptimizer(query)
		o.buildPlan(nil)
	})
}
