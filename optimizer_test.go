package kvql

import "testing"

type builderTest struct {
	query     string
	needError bool
}

func TestPlanBuilder(t *testing.T) {
	tdata := []builderTest{
		builderTest{
			"select key, sum(int(value)) where true", true,
		},
		builderTest{
			"select key, value", true,
		},
		builderTest{
			"select key, value where true;", false,
		},
	}

	txn := &fuzzQueryStorage{}
	for i, item := range tdata {
		opt := NewOptimizer(item.query)
		_, err := opt.buildPlan(txn)
		if berr, ok := err.(QueryBinder); ok {
			berr.BindQuery(item.query)
			berr.SetPadding(0)
		}
		if err == nil && item.needError {
			t.Errorf("[%d] query: `%s` need error, but got nil", i, item.query)
		} else if err != nil && !item.needError {
			t.Errorf("[%d] query: `%s` should not return error but got:\n%s", i, item.query, err.Error())
		}
	}
}

func buildPlan(query string) (FinalPlan, error) {
	txn := &fuzzQueryStorage{}
	opt := NewOptimizer(query)
	return opt.buildPlan(txn)
}

func TestOptimizeDelete(t *testing.T) {
	queries := []string{
		"delete where key in ('k1', 'k2')",
		"delete where key = 'k1' | key = 'k2'",
	}
	for _, query := range queries {
		plan, err := buildPlan(query)
		if err != nil {
			t.Fatal(err)
		}
		if p, ok := plan.(*RemovePlan); ok {
			if len(p.Keys) != 2 {
				t.Fatal("Remove plan should contains 2 keys")
			}
		} else {
			t.Fatal("Should optimize as remove plan")
		}
	}
}

func TestOptimizeDelete2(t *testing.T) {
	queries := []string{
		"delete where key in ('k1', 'k2') and upper(key) = 'K1'",
	}
	for _, query := range queries {
		plan, err := buildPlan(query)
		if err != nil {
			t.Fatal(err)
		}
		if p, ok := plan.(*DeletePlan); ok {
			cp := p.ChildPlan
			if _, ok := cp.(*MultiGetPlan); !ok {
				t.Fatal("Should optimize as multi get plan")
			}
		} else {
			t.Fatal("Should optimize as delete plan")
		}
	}
}
