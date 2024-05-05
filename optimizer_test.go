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

	txn := &fuzzQueryTxn{}
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
