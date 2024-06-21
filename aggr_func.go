package kvql

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/beorn7/perks/quantile"
)

var (
	_ AggrFunction = (*aggrCountFunc)(nil)
	_ AggrFunction = (*aggrSumFunc)(nil)
	_ AggrFunction = (*aggrAvgFunc)(nil)
	_ AggrFunction = (*aggrMinFunc)(nil)
	_ AggrFunction = (*aggrMaxFunc)(nil)
	_ AggrFunction = (*aggrQuantileFunc)(nil)
	_ AggrFunction = (*aggrJsonArrayAggFunc)(nil)
	_ AggrFunction = (*aggrGroupConcatFunc)(nil)
)

func convertToNumber(value any) (int64, float64, bool) {
	switch val := value.(type) {
	case string:
		ival, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			return ival, float64(ival), false
		}
		fval, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return int64(fval), fval, true
		}
	case []byte:
		ival, err := strconv.ParseInt(string(val), 10, 64)
		if err == nil {
			return ival, float64(ival), false
		}
		fval, err := strconv.ParseFloat(string(val), 64)
		if err == nil {
			return int64(fval), fval, true
		}
	case int8:
		return int64(val), float64(val), false
	case int16:
		return int64(val), float64(val), false
	case int:
		return int64(val), float64(val), false
	case int32:
		return int64(val), float64(val), false
	case int64:
		return val, float64(val), false
	case uint8:
		return int64(val), float64(val), false
	case uint16:
		return int64(val), float64(val), false
	case uint:
		return int64(val), float64(val), false
	case uint32:
		return int64(val), float64(val), false
	case uint64:
		return int64(val), float64(val), false
	case float32:
		return int64(val), float64(val), true
	case float64:
		return int64(val), val, true
	case bool:
		if val {
			return 1, 1.0, false
		}
	}
	return 0, 0.0, false
}

// Aggr Count
type aggrCountFunc struct {
	args    []Expression
	counter int64
}

func newAggrCountFunc(args []Expression) (AggrFunction, error) {
	return &aggrCountFunc{counter: 0}, nil
}

func (f *aggrCountFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	f.counter++
	return nil
}

func (f *aggrCountFunc) Complete() (any, error) {
	return f.counter, nil
}

func (f *aggrCountFunc) Clone() AggrFunction {
	ret, _ := newAggrCountFunc(f.args)
	return ret
}

// Aggr Sum
type aggrSumFunc struct {
	args    []Expression
	isum    int64
	fsum    float64
	isFloat bool
}

func newAggrSumFunc(args []Expression) (AggrFunction, error) {
	return &aggrSumFunc{
		args:    args,
		isum:    0,
		fsum:    0.0,
		isFloat: false,
	}, nil
}

func (f *aggrSumFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	f.isum += ival
	f.fsum += fval
	if !f.isFloat && isFloat {
		f.isFloat = true
	}
	return nil
}

func (f *aggrSumFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fsum, nil
	}
	return f.isum, nil
}

func (f *aggrSumFunc) Clone() AggrFunction {
	ret, _ := newAggrSumFunc(f.args)
	return ret
}

// Aggr Avg
type aggrAvgFunc struct {
	args    []Expression
	isum    int64
	fsum    float64
	count   int64
	isFloat bool
}

func newAggrAvgFunc(args []Expression) (AggrFunction, error) {
	return &aggrAvgFunc{
		args:    args,
		isum:    0,
		fsum:    0.0,
		count:   0,
		isFloat: false,
	}, nil
}

func (f *aggrAvgFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	f.isum += ival
	f.fsum += fval
	if !f.isFloat && isFloat {
		f.isFloat = true
	}
	f.count++
	return nil
}

func (f *aggrAvgFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fsum / float64(f.count), nil
	}
	return float64(f.isum) / float64(f.count), nil
}

func (f *aggrAvgFunc) Clone() AggrFunction {
	ret, _ := newAggrAvgFunc(f.args)
	return ret
}

// Aggr Min
type aggrMinFunc struct {
	args    []Expression
	imin    int64
	fmin    float64
	isFloat bool
	first   bool
}

func newAggrMinFunc(args []Expression) (AggrFunction, error) {
	return &aggrMinFunc{
		args:    args,
		imin:    0,
		fmin:    0.0,
		isFloat: false,
		first:   false,
	}, nil
}

func (f *aggrMinFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	if !f.first {
		f.first = true
		f.imin = ival
		f.fmin = fval
		f.isFloat = isFloat
		return nil
	}
	if f.isFloat {
		if f.fmin > fval {
			f.imin = ival
			f.fmin = fval
			f.isFloat = isFloat
		}
	} else {
		if f.imin > ival {
			f.imin = ival
			f.fmin = fval
			f.isFloat = isFloat
		}
	}
	return nil
}

func (f *aggrMinFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fmin, nil
	}
	return f.imin, nil
}

func (f *aggrMinFunc) Clone() AggrFunction {
	ret, _ := newAggrMinFunc(f.args)
	return ret
}

// Aggr Max
type aggrMaxFunc struct {
	args    []Expression
	imax    int64
	fmax    float64
	isFloat bool
	first   bool
}

func newAggrMaxFunc(args []Expression) (AggrFunction, error) {
	return &aggrMaxFunc{
		args:    args,
		imax:    0,
		fmax:    0.0,
		isFloat: false,
		first:   false,
	}, nil
}

func (f *aggrMaxFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	ival, fval, isFloat := convertToNumber(rarg)
	if !f.first {
		f.first = true
		f.imax = ival
		f.fmax = fval
		f.isFloat = isFloat
		return nil
	}
	if f.isFloat {
		if f.fmax < fval {
			f.imax = ival
			f.fmax = fval
			f.isFloat = isFloat
		}
	} else {
		if f.imax < ival {
			f.imax = ival
			f.fmax = fval
			f.isFloat = isFloat
		}
	}
	return nil
}

func (f *aggrMaxFunc) Complete() (any, error) {
	if f.isFloat {
		return f.fmax, nil
	}
	return f.imax, nil
}

func (f *aggrMaxFunc) Clone() AggrFunction {
	ret, _ := newAggrMaxFunc(f.args)
	return ret
}

// Aggr Quantile
type aggrQuantileFunc struct {
	args    []Expression
	percent float64
	stream  *quantile.Stream
}

func newAggrQuantileFunc(args []Expression) (AggrFunction, error) {
	if args[1].ReturnType() != TNUMBER {
		return nil, NewSyntaxError(args[1].GetPos(), "quantile function second parameter require number type")
	}

	pvar, err := args[1].Execute(NewKVP(nil, nil), nil)
	if err != nil {
		return nil, err
	}
	percent, ok := convertToFloat(pvar)
	if !ok {
		return nil, NewExecuteError(args[1].GetPos(), "quantile function second parameter type should be float")
	}
	if percent > 1.0 {
		return nil, NewExecuteError(args[1].GetPos(), "quantile function second parameter type should be less than 1")
	}
	stream := quantile.NewTargeted(map[float64]float64{
		percent: 0.0001,
	})
	return &aggrQuantileFunc{
		percent: percent,
		stream:  stream,
	}, nil
}

func (f *aggrQuantileFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	_, fval, _ := convertToNumber(rarg)
	f.stream.Insert(fval)
	return nil
}

func (f *aggrQuantileFunc) Complete() (any, error) {
	ret := f.stream.Query(f.percent)
	return ret, nil
}

func (f *aggrQuantileFunc) Clone() AggrFunction {
	percent := f.percent
	return &aggrQuantileFunc{
		args:    f.args,
		percent: percent,
		stream: quantile.NewTargeted(map[float64]float64{
			percent: 0.0001,
		}),
	}
}

// Aggr json_arrayagg
type aggrJsonArrayAggFunc struct {
	args  []Expression
	items []any
}

func newAggrJsonArrayAggFunc(args []Expression) (AggrFunction, error) {
	return &aggrJsonArrayAggFunc{
		args:  args,
		items: make([]any, 0, 10),
	}, nil
}

func (f *aggrJsonArrayAggFunc) Clone() AggrFunction {
	ret, _ := newAggrJsonArrayAggFunc(f.args)
	return ret
}

func (f *aggrJsonArrayAggFunc) Complete() (any, error) {
	ret, err := json.Marshal(f.items)
	if err != nil {
		return nil, err
	}
	return string(ret), nil
}

func (f *aggrJsonArrayAggFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	switch val := rarg.(type) {
	case int8, int16, int, int32, int64,
		uint8, uint16, uint, uint32, uint64:
		f.items = append(f.items, val)
	case float32, float64:
		f.items = append(f.items, val)
	case []byte:
		f.items = append(f.items, string(val))
	case bool:
		f.items = append(f.items, val)
	default:
		f.items = append(f.items, toString(val))
	}
	return nil
}

// Aggr group_concat
type aggrGroupConcatFunc struct {
	args  []Expression
	sep   string
	items []string
}

func newAggrGroupConcatFunc(args []Expression) (AggrFunction, error) {
	if args[1].ReturnType() != TSTR {
		return nil, NewSyntaxError(args[1].GetPos(), "group concat second parameter require string type")
	}
	svar, err := args[1].Execute(NewKVP(nil, nil), nil)
	if err != nil {
		return nil, err
	}
	return &aggrGroupConcatFunc{
		args:  args,
		sep:   toString(svar),
		items: make([]string, 0, 10),
	}, nil
}

func (f *aggrGroupConcatFunc) Clone() AggrFunction {
	return &aggrGroupConcatFunc{
		args:  f.args,
		sep:   f.sep,
		items: make([]string, 0, 10),
	}
}

func (f *aggrGroupConcatFunc) Complete() (any, error) {
	return strings.Join(f.items, f.sep), nil
}

func (f *aggrGroupConcatFunc) Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	sval := toString(rarg)
	f.items = append(f.items, sval)
	return nil
}
