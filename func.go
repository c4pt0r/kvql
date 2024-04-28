package kvql

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	funcMap = map[string]*Function{
		"lower":      &Function{"lower", 1, false, TSTR, funcToLower, funcToLowerVec},
		"upper":      &Function{"upper", 1, false, TSTR, funcToUpper, funcToUpperVec},
		"int":        &Function{"int", 1, false, TNUMBER, funcToInt, funcToIntVec},
		"float":      &Function{"float", 1, false, TNUMBER, funcToFloat, funcToFloatVec},
		"str":        &Function{"str", 1, false, TSTR, funcToString, funcToStringVec},
		"is_int":     &Function{"is_int", 1, false, TBOOL, funcIsInt, funcIsIntVec},
		"is_float":   &Function{"is_float", 1, false, TBOOL, funcIsFloat, funcIsFloatVec},
		"substr":     &Function{"substr", 3, false, TSTR, funcSubStr, funcSubStrVec},
		"json":       &Function{"json", 1, false, TJSON, funcJson, funcJsonVec},
		"split":      &Function{"split", 2, false, TLIST, funcSplit, funcSplitVec},
		"list":       &Function{"list", 1, true, TLIST, funcToList, funcToListVec},
		"float_list": &Function{"float_list", 1, true, TLIST, funcFloatList, funcFloatListVec},
		"int_list":   &Function{"int_list", 1, true, TLIST, funcIntList, funcIntListVec},
		"flist":      &Function{"flist", 1, true, TLIST, funcFloatList, funcFloatListVec},
		"ilist":      &Function{"ilist", 1, true, TLIST, funcIntList, funcIntListVec},
		"len":        &Function{"len", 1, false, TNUMBER, funcLen, funcLenVec},
		"join":       &Function{"join", 2, true, TSTR, funcJoin, funcJoinVec},

		"cosine_distance": &Function{"cosine_distance", 2, false, TNUMBER, funcCosineDistance, funcCosineDistanceVec},
		"l2_distance":     &Function{"l2_distance", 2, false, TNUMBER, funcL2Distance, funcL2DistanceVec},
	}

	aggrFuncMap = map[string]*AggrFunc{
		"count":    &AggrFunc{"count", 1, false, TNUMBER, newAggrCountFunc},
		"sum":      &AggrFunc{"sum", 1, false, TNUMBER, newAggrSumFunc},
		"avg":      &AggrFunc{"avg", 1, false, TNUMBER, newAggrAvgFunc},
		"min":      &AggrFunc{"min", 1, false, TNUMBER, newAggrMinFunc},
		"max":      &AggrFunc{"max", 1, false, TNUMBER, newAggrMaxFunc},
		"quantile": &AggrFunc{"quantile", 2, false, TNUMBER, newAggrQuantileFunc},
	}
)

type FunctionBody func(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error)
type VectorFunctionBody func(chunk []KVPair, args []Expression, ctx *ExecuteCtx) ([]any, error)

type Function struct {
	Name       string
	NumArgs    int
	VarArgs    bool
	ReturnType Type
	Body       FunctionBody
	BodyVec    VectorFunctionBody
}

type AggrFunc struct {
	Name       string
	NumArgs    int
	VarArgs    bool
	ReturnType Type
	Body       AggrFunctor
}

type AggrFunctor func(args []Expression) (AggrFunction, error)

type AggrFunction interface {
	Update(kv KVPair, args []Expression, ctx *ExecuteCtx) error
	Complete() (any, error)
	Clone() AggrFunction
}

func GetFuncNameFromExpr(expr Expression) (string, error) {
	fc, ok := expr.(*FunctionCallExpr)
	if !ok {
		return "", NewSyntaxError(expr.GetPos(), "Not function call expression")
	}
	rfname, err := fc.Name.Execute(NewKVP(nil, nil), nil)
	if err != nil {
		return "", err
	}
	fname, ok := rfname.(string)
	if !ok {
		return "", NewSyntaxError(expr.GetPos(), "Invalid function name")
	}
	return strings.ToLower(fname), nil
}

func GetScalarFunction(expr Expression) (*Function, error) {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return nil, err
	}
	fobj, have := funcMap[fname]
	if !have {
		return nil, NewSyntaxError(expr.GetPos(), "Cannot find function %s", fname)
	}
	return fobj, nil
}

func GetScalarFunctionByName(name string) (*Function, bool) {
	fobj, have := funcMap[name]
	return fobj, have
}

func GetAggrFunctionByName(name string) (*AggrFunc, bool) {
	fobj, have := aggrFuncMap[name]
	return fobj, have
}

func AddScalarFunction(f *Function) {
	fname := strings.ToLower(f.Name)
	funcMap[fname] = f
}

func AddAggrFunction(f *AggrFunc) {
	fname := strings.ToLower(f.Name)
	aggrFuncMap[fname] = f
}

func IsScalarFuncExpr(expr Expression) bool {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return false
	}
	if _, have := funcMap[fname]; have {
		return true
	}
	return false
}

func IsAggrFuncExpr(expr Expression) bool {
	fname, err := GetFuncNameFromExpr(expr)
	if err != nil {
		return false
	}
	if _, have := aggrFuncMap[fname]; have {
		return true
	}
	return false
}

func IsAggrFunc(fname string) bool {
	_, have := aggrFuncMap[fname]
	return have
}

func toString(value any) string {
	switch val := value.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		if val == nil {
			return "<nil>"
		}
		return ""
	}
}

func toInt(value any, defVal int64) int64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseInt(val, 10, 64); err == nil {
			return ret
		} else {
			if ret, err := strconv.ParseFloat(val, 64); err == nil {
				return int64(ret)
			}
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			return ret
		} else {
			if ret, err := strconv.ParseFloat(string(val), 64); err == nil {
				return int64(ret)
			}
			return defVal
		}
	case int8, int16, uint8, uint16:
		if ret, err := strconv.ParseInt(fmt.Sprintf("%d", val), 10, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int:
		return int64(val)
	case uint:
		return int64(val)
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case int64:
		return val
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return defVal
	}
}

func toFloat(value any, defVal float64) float64 {
	switch val := value.(type) {
	case string:
		if ret, err := strconv.ParseFloat(val, 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case []byte:
		if ret, err := strconv.ParseFloat(string(val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int8, int16, uint8, uint16:
		if ret, err := strconv.ParseFloat(fmt.Sprintf("%d", val), 64); err == nil {
			return ret
		} else {
			return defVal
		}
	case int:
		return float64(val)
	case uint:
		return float64(val)
	case int32:
		return float64(val)
	case uint32:
		return float64(val)
	case int64:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	default:
		return defVal
	}
}

func toFloatList(value any) ([]float64, error) {
	switch val := value.(type) {
	case []string:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			fval, err := strconv.ParseFloat(val[i], 64)
			if err != nil {
				return nil, err
			}
			ret[i] = fval
		}
		return ret, nil
	case [][]byte:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			fval, err := strconv.ParseFloat(string(val[i]), 64)
			if err != nil {
				return nil, err
			}
			ret[i] = fval
		}
		return ret, nil
	case []int:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []uint:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []int32:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []uint32:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []int64:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []uint64:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []float32:
		ret := make([]float64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = float64(val[i])
		}
		return ret, nil
	case []float64:
		return val, nil
	default:
		return nil, fmt.Errorf("Cannot convert to float list")
	}
}

func toIntList(value any) ([]int64, error) {
	switch val := value.(type) {
	case []string:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			fval, err := strconv.ParseInt(val[i], 10, 64)
			if err != nil {
				return nil, err
			}
			ret[i] = fval
		}
		return ret, nil
	case [][]byte:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			fval, err := strconv.ParseInt(string(val[i]), 10, 64)
			if err != nil {
				return nil, err
			}
			ret[i] = fval
		}
		return ret, nil
	case []int:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []uint:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []int32:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []uint32:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []int64:
		return val, nil
	case []uint64:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []float32:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	case []float64:
		ret := make([]int64, len(val))
		for i := 0; i < len(val); i++ {
			ret[i] = int64(val[i])
		}
		return ret, nil
	default:
		return nil, fmt.Errorf("Cannot convert to float list")
	}
}