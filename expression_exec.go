package kvql

import (
	"bytes"
	"regexp"
)

type FilterExec struct {
	Ast *WhereStmt
}

func (e *FilterExec) Explain() string {
	return e.Ast.Expr.String()
}

func (e *FilterExec) Filter(kvp KVPair, ctx *ExecuteCtx) (bool, error) {
	ret, err := e.filterBatch([]KVPair{kvp}, ctx)
	if err != nil {
		return false, err
	}
	return ret[0], nil
}

func (e *FilterExec) FilterBatch(chunk []KVPair, ctx *ExecuteCtx) ([]bool, error) {
	// return e.filterBatch(chunk)
	return e.filterChunk(chunk, ctx)
}

func (e *FilterExec) filterChunk(chunk []KVPair, ctx *ExecuteCtx) ([]bool, error) {
	result, err := e.Ast.Expr.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	var (
		ret = make([]bool, len(result))
		ok  bool
	)
	for i := 0; i < len(result); i++ {
		ret[i], ok = result[i].(bool)
		if !ok {
			return nil, NewExecuteError(e.Ast.Expr.GetPos(), "where expression result is not boolean")
		}
	}
	return ret, nil
}

func (e *FilterExec) filterBatch(kvps []KVPair, ctx *ExecuteCtx) ([]bool, error) {
	ret := make([]bool, len(kvps))
	for idx, kvp := range kvps {
		result, err := e.Ast.Expr.Execute(kvp, ctx)
		if err != nil {
			return nil, err
		}
		bresult, ok := result.(bool)
		if !ok {
			return nil, NewExecuteError(e.Ast.Expr.GetPos(), "where expression result is not boolean")
		}
		ret[idx] = bresult
	}
	return ret, nil
}

func (e *StringExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return []byte(e.Data), nil
}

func (e *FieldExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	switch e.Field {
	case KeyKW:
		return kv.Key, nil
	case ValueKW:
		return kv.Value, nil
	}
	return nil, NewExecuteError(e.GetPos(), "Invalid field name %v", e.Field)
}

func (e *BinaryOpExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	leftTp := e.Left.ReturnType()
	switch e.Op {
	case Eq:
		return e.execEqual(kv, ctx)
	case NotEq:
		return e.execNotEqual(kv, ctx)
	case PrefixMatch:
		return e.execPrefixMatch(kv, ctx)
	case RegExpMatch:
		return e.execRegexpMatch(kv, ctx)
	case And:
		return e.execAnd(kv, ctx)
	case Or:
		return e.execOr(kv, ctx)
	case Add:
		if e.Left.ReturnType() == TSTR {
			return e.execStringConcate(kv, ctx)
		}
		return e.execMath(kv, '+', ctx)
	case Sub:
		return e.execMath(kv, '-', ctx)
	case Mul:
		return e.execMath(kv, '*', ctx)
	case Div:
		return e.execMath(kv, '/', ctx)
	case Gt:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, ">", ctx)
		default:
			return e.execNumberCompare(kv, ">", ctx)
		}
	case Gte:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, ">=", ctx)
		default:
			return e.execNumberCompare(kv, ">=", ctx)
		}
	case Lt:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, "<", ctx)
		default:
			return e.execNumberCompare(kv, "<", ctx)
		}
	case Lte:
		switch leftTp {
		case TSTR:
			return e.execStringCompare(kv, "<=", ctx)
		default:
			return e.execNumberCompare(kv, "<=", ctx)
		}
	case In:
		switch leftTp {
		case TSTR:
			return e.execStringIn(kv, ctx)
		default:
			return e.execNumberIn(kv, ctx)
		}
	case Between:
		switch leftTp {
		case TSTR:
			return e.execStringBetween(kv, ctx)
		default:
			return e.execNumberBetween(kv, ctx)
		}
	}
	return nil, NewExecuteError(e.GetPos(), "Unknown operator %v", e.Op)
}

func (e *BinaryOpExpr) execEqual(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	rleft, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	switch rleft.(type) {
	case string, []byte:
		left, lok := convertToByteArray(rleft)
		right, rok := convertToByteArray(rright)
		if lok && rok {
			return bytes.Equal(left, right), nil
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		lint, lok := convertToInt(rleft)
		rint, rok := convertToInt(rright)
		if lok && rok {
			return lint == rint, nil
		}
	case bool:
		lbool, lok := rleft.(bool)
		rbool, rok := rright.(bool)
		if lok && rok {
			return lbool == rbool, nil
		}
	}
	return false, NewExecuteError(e.GetPos(), "= operator left or right expression has wrong type")
}

func (e *BinaryOpExpr) execNotEqual(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	ret, err := e.execEqual(kv, ctx)
	if err != nil {
		return false, err
	}
	return !ret, nil
}

func (e *BinaryOpExpr) execPrefixMatch(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	rleft, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	left, lok := convertToByteArray(rleft)
	right, rok := convertToByteArray(rright)
	if !lok || !rok {
		return false, NewExecuteError(e.GetPos(), "= operator left or right expression has wrong type")
	}
	return bytes.HasPrefix(left, right), nil
}

func (e *BinaryOpExpr) execRegexpMatch(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	rleft, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	left, lok := convertToByteArray(rleft)
	right, rok := convertToByteArray(rright)
	if !lok || !rok {
		return false, NewExecuteError(e.GetPos(), "~= operator left or right expression has wrong type")
	}
	re, err := regexp.Compile(string(right))
	if err != nil {
		return false, err
	}
	return re.Match(left), nil
}

func (e *BinaryOpExpr) execAnd(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	rleft, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	if !lok {
		return false, NewExecuteError(e.Left.GetPos(), "& operator left expression has wrong type, not boolean")
	}
	if !left {
		return false, nil
	}
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	right, rok := rright.(bool)
	if !rok {
		return false, NewExecuteError(e.Left.GetPos(), "& operator right expression has wrong type, not boolean")
	}
	return left && right, nil
}

func (e *BinaryOpExpr) execOr(kv KVPair, ctx *ExecuteCtx) (bool, error) {
	rleft, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	left, lok := rleft.(bool)
	if !lok {
		return false, NewExecuteError(e.Left.GetPos(), "| operator left expression has wrong type, not boolean")
	}
	if left {
		return true, nil
	}
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	right, rok := rright.(bool)
	if !rok {
		return false, NewExecuteError(e.Left.GetPos(), "| operator right expression has wrong type, not boolean")
	}
	return left || right, nil
}

func (e *BinaryOpExpr) execMath(kv KVPair, op byte, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	return executeMathOp(left, right, op, e.Right)
}

func (e *BinaryOpExpr) execNumberCompare(kv KVPair, op string, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	return execNumberCompare(left, right, op)
}

func (e *BinaryOpExpr) execStringCompare(kv KVPair, op string, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	right, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	return execStringCompare(left, right, op)
}

func (e *BinaryOpExpr) execStringIn(kv KVPair, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	switch rlist := e.Right.(type) {
	case *ListExpr:
		for _, expr := range rlist.List {
			if expr.ReturnType() != TSTR {
				return false, NewExecuteError(expr.GetPos(), "in operator right expression element has wrong type")
			}
			lvalue, err := expr.Execute(kv, ctx)
			if err != nil {
				return false, err
			}
			cmp, err := execStringCompare(left, lvalue, "=")
			if err != nil {
				return false, err
			}
			if cmp {
				return true, nil
			}
		}
		return false, nil
	case *FunctionCallExpr:
		if rlist.ReturnType() != TLIST {
			return false, NewExecuteError(rlist.GetPos(), "in operator right expression has wrong type, not list 1")
		}
		fret, err := rlist.Execute(kv, ctx)
		if err != nil {
			return false, err
		}
		vals, ok := fret.([]any)
		if !ok {
			return false, NewExecuteError(rlist.GetPos(), "in operator right expression has wrong type, not list 2")
		}
		for _, val := range vals {
			cmp, err := execStringCompare(left, val, "=")
			if err != nil {
				return false, nil
			}
			if cmp {
				return true, nil
			}
		}
		return false, nil
	}
	return false, NewExecuteError(e.GetPos(), "in operator right expression has wrong type, not list 3")
}

func (e *BinaryOpExpr) execNumberIn(kv KVPair, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	switch rlist := e.Right.(type) {
	case *ListExpr:
		for _, expr := range rlist.List {
			if expr.ReturnType() != TNUMBER {
				return false, NewExecuteError(expr.GetPos(), "in operator right expression has wrong type, not number")
			}
			lvalue, err := expr.Execute(kv, ctx)
			if err != nil {
				return false, err
			}
			cmp, err := execNumberCompare(left, lvalue, "=")
			if err != nil {
				return false, err
			}
			if cmp {
				return true, nil
			}
		}
		return false, nil
	case *FunctionCallExpr:
		if rlist.ReturnType() != TLIST {
			return false, NewExecuteError(rlist.GetPos(), "in operator right expression has wrong type, not list")
		}
		fret, err := rlist.Execute(kv, ctx)
		if err != nil {
			return false, err
		}
		vals, ok := fret.([]any)
		if !ok {
			return false, NewExecuteError(rlist.GetPos(), "in operator right expression has wrong type, not list")
		}
		for _, val := range vals {
			cmp, err := execNumberCompare(left, val, "=")
			if err != nil {
				return false, nil
			}
			if cmp {
				return true, nil
			}
		}
		return false, nil
	}
	return false, NewExecuteError(e.Right.GetPos(), "in operator right expression has wrong type, not list")
}

func (e *BinaryOpExpr) execStringBetween(kv KVPair, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	rlist, ok := e.Right.(*ListExpr)
	if !ok || len(rlist.List) != 2 {
		return false, NewExecuteError(e.Right.GetPos(), "between operator right expression invalid")
	}
	lexpr := rlist.List[0]
	uexpr := rlist.List[1]
	if lexpr.ReturnType() != TSTR {
		return false, NewExecuteError(lexpr.GetPos(), "between operator lower boundary expression has wrong type, not string")
	}
	if uexpr.ReturnType() != TSTR {
		return false, NewExecuteError(uexpr.GetPos(), "between operator upper boundary expression has wrong type, not string")
	}
	lval, err := lexpr.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	uval, err := uexpr.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	cmp, err := execStringCompare(lval, uval, "<")
	if err != nil {
		return false, err
	}
	if !cmp {
		return false, NewExecuteError(e.GetPos(), "between operator lower boundary is greater than upper boundary")
	}
	lcmp, err := execStringCompare(lval, left, "<=")
	if err != nil {
		return false, err
	}
	// left < lower, return false
	if !lcmp {
		return false, nil
	}
	ucmp, err := execStringCompare(left, uval, "<=")
	if err != nil {
		return false, err
	}
	// left < upper
	return ucmp, nil
}

func (e *BinaryOpExpr) execNumberBetween(kv KVPair, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	rlist, ok := e.Right.(*ListExpr)
	if !ok || len(rlist.List) != 2 {
		return false, NewExecuteError(e.Right.GetPos(), "between operator right expression invalid")
	}
	lexpr := rlist.List[0]
	uexpr := rlist.List[1]
	if lexpr.ReturnType() != TNUMBER {
		return false, NewExecuteError(lexpr.GetPos(), "between operator lower boundary expression has wrong type, not number")
	}
	if uexpr.ReturnType() != TNUMBER {
		return false, NewExecuteError(uexpr.GetPos(), "between operator upper boundary expression has wrong type, not number")
	}
	lval, err := lexpr.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	uval, err := uexpr.Execute(kv, ctx)
	if err != nil {
		return false, err
	}
	cmp, err := execNumberCompare(lval, uval, "<")
	if err != nil {
		return false, err
	}
	if !cmp {
		return false, NewExecuteError(e.GetPos(), "between operator lower boundary is greater than upper boundary")
	}
	lcmp, err := execNumberCompare(lval, left, "<=")
	if err != nil {
		return false, err
	}
	// left < lower, return false
	if !lcmp {
		return false, nil
	}
	ucmp, err := execNumberCompare(left, uval, "<=")
	if err != nil {
		return false, err
	}
	// left < upper
	return ucmp, nil
}

func (e *BinaryOpExpr) execStringConcate(kv KVPair, ctx *ExecuteCtx) (any, error) {
	lval, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return "", err
	}
	rval, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return "", err
	}
	lstr := toString(lval)
	rstr := toString(rval)
	return lstr + rstr, nil
}

func (e *NotExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	rright, err := e.Right.Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	right, rok := rright.(bool)
	if !rok {
		return nil, NewExecuteError(e.Right.GetPos(), "! operator right expression has wrong type, not boolean")
	}
	return !right, nil
}

func (e *FunctionCallExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	if e.Result != nil {
		return e.Result, nil
	}
	funcObj, err := GetScalarFunction(e)
	if err != nil {
		return nil, err
	}
	return e.executeFunc(kv, funcObj, ctx)
}

func (e *FunctionCallExpr) executeFunc(kv KVPair, funcObj *Function, ctx *ExecuteCtx) (any, error) {
	// Check arguments
	if !funcObj.VarArgs && len(e.Args) != funcObj.NumArgs {
		return nil, NewExecuteError(e.GetPos(), "Function %s require %d arguments but got %d", funcObj.Name, funcObj.NumArgs, len(e.Args))
	}
	return funcObj.Body(kv, e.Args, ctx)
}

func (e *NameExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return e.Data, nil
}

func (e *NumberExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return e.Int, nil
}

func (e *FloatExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return e.Float, nil
}

func (e *BoolExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return e.Bool, nil
}

func (e *ListExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	return e.List, nil
}

func (e *FieldAccessExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	left, err := e.Left.Execute(kv, ctx)
	if err != nil {
		return nil, err
	}

	switch fnval := e.FieldName.(type) {
	case *StringExpr:
		return e.execDictAccess(fnval.Data, left)
	case *NumberExpr:
		return e.execListAccess(int(fnval.Int), left)
	}
	return nil, NewSyntaxError(e.FieldName.GetPos(), "Invalid field name")
}

func (e *FieldAccessExpr) execDictAccess(fieldName string, left any) (any, error) {
	var (
		fval any
		have bool
	)
	switch lval := left.(type) {
	case map[string]any:
		fval, have = lval[fieldName]
	case JSON:
		fval, have = lval[fieldName]
	case string:
		if lval == "" {
			have = false
		} else {
			return nil, NewExecuteError(e.Left.GetPos(), "Field access left expression has wrong type, not JSON")
		}
	default:
		return nil, NewExecuteError(e.Left.GetPos(), "Field access left expression has wrong type, not JSON")
	}
	if !have {
		return "", nil
	}
	return fval, nil
}

func (e *FieldAccessExpr) execListAccess(idx int, left any) (any, error) {
	var (
		fval any
		have bool
	)
	switch lval := left.(type) {
	case []any:
		lvallen := len(lval)
		if idx < lvallen {
			have = true
			fval = lval[idx]
		}
	case string:
		if lval == "" {
			have = false
		} else {
			return nil, NewExecuteError(e.Left.GetPos(), "Field access left expression has wrong type, not List")
		}
	default:
		return nil, NewExecuteError(e.Left.GetPos(), "Field access left expression has wrong type, not List")
	}
	if !have {
		return "", nil
	}
	return fval, nil
}

func (e *FieldReferenceExpr) Execute(kv KVPair, ctx *ExecuteCtx) (any, error) {
	if ctx != nil {
		cval, have := ctx.GetFieldResult(e.Name.Data)
		if have {
			ctx.UpdateHit()
			return cval, nil
		}
	}
	ret, err := e.FieldExpr.Execute(kv, ctx)
	if err != nil {
		return ret, err
	}
	if ctx != nil {
		ctx.SetFieldResult(e.Name.Data, ret)
	}
	return ret, err
}
