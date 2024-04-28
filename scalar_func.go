package kvql

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func funcToLower(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToLower(arg), nil
}

func funcToUpper(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	arg := toString(rarg)
	return strings.ToUpper(arg), nil
}

func funcToInt(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	ret := toInt(rarg, 0)
	return ret, nil
}

func funcToFloat(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	ret := toFloat(rarg, 0.0)
	return ret, nil
}

func funcToString(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	ret := toString(rarg)
	return ret, nil
}

func funcIsInt(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	switch val := rarg.(type) {
	case string:
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			return true, nil
		}
	case []byte:
		if _, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return true, nil
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true, nil
	}
	return false, nil
}

func funcIsFloat(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	switch val := rarg.(type) {
	case string:
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			return true, nil
		}
	case []byte:
		if _, err := strconv.ParseFloat(string(val), 64); err == nil {
			return true, nil
		}
	case float32, float64:
		return true, nil
	}
	return false, nil
}

func funcSubStr(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	val := toString(rarg)
	if args[1].ReturnType() != TNUMBER {
		return nil, NewExecuteError(args[1].GetPos(), "substr function second parameter require number type")
	}
	if args[2].ReturnType() != TNUMBER {
		return nil, NewExecuteError(args[2].GetPos(), "substr function third parameter require number type")
	}
	rarg, err = args[1].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	start := int(toInt(rarg, 0))
	rarg, err = args[2].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	length := int(toInt(rarg, 0))
	vlen := len(val)
	if start > vlen-1 {
		return "", nil
	}
	length = min(length, vlen-start)
	return val[start:length], nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type JSON map[string]any

func funcJson(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	jsonData, ok := convertToByteArray(rarg)
	if !ok {
		return nil, NewExecuteError(args[0].GetPos(), "Cannot convert to byte array")
	}
	ret := make(JSON)
	json.Unmarshal(jsonData, &ret)
	return ret, nil
}

func funcSplit(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	if args[1].ReturnType() != TSTR {
		return nil, NewExecuteError(args[1].GetPos(), "split function second parameter require string type")
	}
	rspliter, err := args[1].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	val := toString(rarg)
	spliter := toString(rspliter)
	ret := strings.Split(val, spliter)
	return ret, nil
}

func funcJoin(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	if args[0].ReturnType() != TSTR {
		return nil, NewExecuteError(args[0].GetPos(), "join function first parameter require string type")
	}
	rseparator, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	separator := toString(rseparator)
	vals := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		rval, err := arg.Execute(kv, ctx)
		if err != nil {
			return nil, err
		}
		vals[i] = toString(rval)
	}
	ret := strings.Join(vals, separator)
	return ret, nil
}

func funcCosineDistance(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	larg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	rarg, err := args[1].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	lvec, err := toFloatList(larg)
	if err != nil {
		return nil, err
	}
	rvec, err := toFloatList(rarg)
	if err != nil {
		return nil, err
	}
	ret, err := cosineDistance(lvec, rvec)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func cosineDistance(left, right []float64) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("length must equals")
	}
	var t1 float64
	var t2 float64
	var t3 float64
	for i := 0; i < len(left); i++ {
		t1 += left[i] * right[i]
		t2 += left[i] * left[i]
		t3 += right[i] * right[i]
	}
	return 1 - t1/(math.Sqrt(t2)*math.Sqrt(t3)), nil
}

func funcL2Distance(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	larg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	rarg, err := args[1].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	lvec, err := toFloatList(larg)
	if err != nil {
		return nil, err
	}
	rvec, err := toFloatList(rarg)
	if err != nil {
		return nil, err
	}
	ret, err := l2Distance(lvec, rvec)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func l2Distance(left, right []float64) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("length must equals")
	}
	var total float64 = 0
	for i := 0; i < len(left); i++ {
		diff := math.Abs(left[i] - right[i])
		total += diff * diff
	}
	return math.Sqrt(total), nil
}

func funcFloatList(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	ret := make([]float64, len(args))
	for i := 0; i < len(args); i++ {
		val, err := args[i].Execute(kv, ctx)
		if err != nil {
			return nil, err
		}
		ret[i] = toFloat(val, 0.0)
	}
	return ret, nil
}

func funcIntList(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	ret := make([]int64, len(args))
	for i := 0; i < len(args); i++ {
		val, err := args[i].Execute(kv, ctx)
		if err != nil {
			return nil, err
		}
		ret[i] = toInt(val, 0)
	}
	return ret, nil
}

func funcToList(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	if len(args) == 0 {
		return []int64{}, nil
	}

	first, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	useInt := false
	switch fval := first.(type) {
	case string:
		if _, err := strconv.ParseInt(fval, 10, 64); err == nil {
			useInt = true
		} else if _, err := strconv.ParseFloat(fval, 64); err == nil {
			useInt = false
		}
	case []byte:
		if _, err := strconv.ParseInt(string(fval), 10, 64); err == nil {
			useInt = true
		} else if _, err := strconv.ParseFloat(string(fval), 64); err == nil {
			useInt = false
		}
	case int, uint, int32, uint32, int64, uint64:
		useInt = true
	case float32, float64:
		useInt = false
	}
	if useInt {
		return funcIntList(kv, args, ctx)
	}
	return funcFloatList(kv, args, ctx)
}

func funcLen(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	ret, err := getListLength(rarg)
	if err != nil {
		return nil, NewExecuteError(args[0].GetPos(), err.Error())
	}
	return ret, nil
}

func getListLength(data any) (int, error) {
	switch val := data.(type) {
	case string:
		return len(val), nil
	case int, int32, int64, uint, uint32, uint64, float32, float64:
		return 0, nil
	case []byte:
		return len(val), nil
	case []int:
		return len(val), nil
	case []int32:
		return len(val), nil
	case []int64:
		return len(val), nil
	case []uint:
		return len(val), nil
	case []uint32:
		return len(val), nil
	case []uint64:
		return len(val), nil
	case []float32:
		return len(val), nil
	case []float64:
		return len(val), nil
	}
	return 0, fmt.Errorf("invalid type")
}