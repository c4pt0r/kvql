package kvql

import (
	"fmt"
	"sort"
	"testing"
)

func optimizeQuery(query string) (*ScanType, error) {
	p := NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	o := NewFilterOptimizer(stmt.Where, nil, nil)
	ret := o.optimizeExpr(stmt.Where.Expr)
	return ret, nil
}

func assertScanType(t *testing.T, st *ScanType, tp byte, keys []string) {
	assertScanTypeWithID(0, t, st, tp, keys)
}

func assertScanTypeWithID(id int, t *testing.T, st *ScanType, tp byte, keys []string) {
	if tp != st.scanTp {
		t.Errorf("[%d] Scan Type expect %v but got %v", id, ScanTypeToString(tp), ScanTypeToString(st.scanTp))
		return
	}
	kstrs := make([]string, len(st.keys))
	for i, k := range st.keys {
		kstrs[i] = string(k)
	}

	if len(keys) != len(kstrs) {
		t.Errorf("[%d] Scan Type keys expect %v but got %v", id, keys, kstrs)
		return
	}

	if st.scanTp == MGET {
		sort.Strings(kstrs)
	}
	for i, k := range keys {
		if k != kstrs[i] {
			t.Errorf("[%d] Scan Type keys expect %v but got %v", id, keys, kstrs)
		}
	}
}

type optTData struct {
	query  string
	scanTp byte
	keys   []string
}

func TestOptimizers(t *testing.T) {
	tdata := []optTData{
		// PREFIX & MGET
		optTData{
			"select * where key ^= 'k' & (key = 'k1' | key = 'm1')",
			MGET, []string{"k1"},
		},
		optTData{
			"select * where key ^= 'k' & (key = 'm1' | key = 'm2')",
			EMPTY, nil,
		},
		// PREFIX | MGET
		optTData{
			"select * where key ^= 'k' | (key = 'k1' | key = 'k2')",
			PREFIX, []string{"k"},
		},
		optTData{
			"select * where key ^= 'k' | (key in ('k1', 'k2'))",
			PREFIX, []string{"k"},
		},

		optTData{
			"select * where key ^= 'k' | (key = 'k1' | key = 'm2')",
			FULL, nil,
		},
		// PREFIX & RANGE
		optTData{
			"select * where key ^= 'k' & (key > 'k1' & key < 'k8')",
			RANGE, []string{"k1", "k8"},
		},
		optTData{
			"select * where key ^= 'k' & (key > 'j1' & key < 'l8')",
			PREFIX, []string{"k"},
		},
		optTData{
			"select * where key ^= 'k' & (key > 'k1' & key < 'l8')",
			RANGE, []string{"k1", "l8"},
		},
		optTData{
			"select * where key ^= 'l' & (key > 'k1' & key < 'l8')",
			RANGE, []string{"l", "l8"},
		},
		optTData{
			"select * where key ^= 'j' & (key > 'k1' & key < 'l8')",
			EMPTY, nil,
		},
		optTData{
			"select * where key ^= 'm' & (key > 'k1' & key < 'l8')",
			EMPTY, nil,
		},
		optTData{
			"select * where key ^= 'm' & (key > 'k1' & key < 'm')",
			MGET, []string{"m"},
		},
		// PREFIX | RANGE
		optTData{
			"select * where key ^= 'k' | (key > 'k1' & key < 'k8')",
			PREFIX, []string{"k"},
		},
		optTData{
			"select * where key ^= 'k' | (key > 'j1' & key < 'l8')",
			RANGE, []string{"j1", "l8"},
		},
		optTData{
			"select * where key ^= 'k' | (key > 'k1' & key < 'l8')",
			RANGE, []string{"k", "l8"},
		},
		optTData{
			"select * where key ^= 'l' | (key > 'k1' & key < 'l8')",
			RANGE, []string{"k1", ""},
		},
		optTData{
			"select * where key ^= 'j' | (key > 'k1' & key < 'l8')",
			RANGE, []string{"j", "l8"},
		},
		optTData{
			"select * where key ^= 'm' | (key > 'k1' & key < 'l8')",
			RANGE, []string{"k1", ""},
		},
		optTData{
			"select * where key ^= 'm' | (key > 'k1' & key < 'm')",
			RANGE, []string{"k1", ""},
		},
		// RANGE & RANGE
		optTData{
			"select * where (key > 'k01' & key < 'k10') & (key > 'k05' & key < 'k12')",
			RANGE, []string{"k05", "k10"},
		},
		// RANGE | RANGE
		optTData{
			"select * where (key > 'k1' & key < 'k5') | (key > 'k4' & key < 'k9')",
			RANGE, []string{"k1", "k9"},
		},
		optTData{
			"select * where (key > 'k1' & key < 'k5') | (key > 'k8' & key < 'k9')",
			RANGE, []string{"k1", "k9"},
		},
		// RANGE & MGET
		optTData{
			"select * where (key > 'i' & key <= 'k') & (key = 'j1' | key = 'k' | key = 'k1')",
			MGET, []string{"j1", "k"},
		},
		optTData{
			"select * where (key > 'i' & key <= 'k') & (key = 'l1' | key = 'l2' | key = 'l3')",
			EMPTY, nil,
		},
		// RANGE | MGET
		optTData{
			"select * where (key > 'k1' & key < 'k9') | (key = 'k5')",
			RANGE, []string{"k1", "k9"},
		},
		optTData{
			"select * where (key > 'k1' & key < 'k9') | (key = 'l1')",
			RANGE, []string{"k1", "l1"},
		},
		optTData{
			"select * where (key > 'k1' & key < 'k9') | (key = 'j1')",
			RANGE, []string{"j1", "k9"},
		},
		optTData{
			"select * where (key > 'k1' & key < 'k9') | (key = 'j1' | key = 'm1')",
			FULL, nil,
		},
		// NOT RANGE
		optTData{
			"select * where !(key > 'k1' & key < 'k9')",
			FULL, nil,
		},
		// Just MGET
		optTData{
			"select * where key in ('k1', 'k2')",
			MGET, []string{"k1", "k2"},
		},
		// Just RANGE use between and
		optTData{
			"select * where key between 'k1' and 'k2'",
			RANGE, []string{"k1", "k2"},
		},
	}

	for i, item := range tdata {
		st, err := optimizeQuery(item.query)
		if err != nil {
			t.Errorf("[%d] %v", i, err)
		}
		fmt.Printf("[%d] %s\n    `- %s\n\n", i, item.query, st.String())
		assertScanTypeWithID(i, t, st, item.scanTp, item.keys)
	}
}
