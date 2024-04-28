package kvql

import (
	"bytes"
	"regexp"
)

func (e *StringExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = []byte(e.Data)
	}
	return ret, nil
}

func (e *FieldExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	if e.Field != KeyKW && e.Field != ValueKW {
		return nil, NewExecuteError(e.GetPos(), "Invalid field name %v", e.Field)
	}
	ret := make([]any, len(chunk))
	isKey := e.Field == KeyKW
	for i := 0; i < len(chunk); i++ {
		if isKey {
			ret[i] = chunk[i].Key
		} else {
			ret[i] = chunk[i].Value
		}
	}
	return ret, nil
}

func (e *NotExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	right, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		rval, rok := right[i].(bool)
		if !rok {
			return nil, NewExecuteError(e.Right.GetPos(), "! operator right expression has wrong type, not boolean")
		}
		right[i] = !rval
	}
	return right, nil
}

func (e *NameExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Data
	}
	return ret, nil
}

func (e *NumberExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Int
	}
	return ret, nil
}

func (e *FloatExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Float
	}
	return ret, nil
}

func (e *BoolExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.Bool
	}
	return ret, nil
}

func (e *ListExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	ret := make([]any, len(chunk))
	for i := 0; i < len(chunk); i++ {
		ret[i] = e.List
	}
	return ret, nil
}

func (e *BinaryOpExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	leftTp := e.Left.ReturnType()
	switch e.Op {
	case Eq:
		return e.execEqualBatch(chunk, false, ctx)
	case NotEq:
		return e.execEqualBatch(chunk, true, ctx)
	case PrefixMatch:
		return e.execPrefixMatchBatch(chunk, ctx)
	case RegExpMatch:
		return e.execRegexpMatchBatch(chunk, ctx)
	case And:
		return e.execAndOrBatch(chunk, true, ctx)
	case Or:
		return e.execAndOrBatch(chunk, false, ctx)
	case Add:
		if e.Left.ReturnType() == TSTR {
			return e.execStringConcateBatch(chunk, ctx)
		}
		return e.execMathBatch(chunk, '+', ctx)
	case Sub:
		return e.execMathBatch(chunk, '-', ctx)
	case Mul:
		return e.execMathBatch(chunk, '*', ctx)
	case Div:
		return e.execMathBatch(chunk, '/', ctx)
	case Gt:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, ">", ctx)
		default:
			return e.execNumberCompareBatch(chunk, ">", ctx)
		}
	case Gte:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, ">=", ctx)
		default:
			return e.execNumberCompareBatch(chunk, ">=", ctx)
		}
	case Lt:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, "<", ctx)
		default:
			return e.execNumberCompareBatch(chunk, "<", ctx)
		}
	case Lte:
		switch leftTp {
		case TSTR:
			return e.execStringCompareBatch(chunk, "<=", ctx)
		default:
			return e.execNumberCompareBatch(chunk, "<=", ctx)
		}
	case In:
		switch leftTp {
		case TSTR:
			return e.execInBatch(chunk, false, ctx)
		default:
			return e.execInBatch(chunk, true, ctx)
		}
	case Between:
		switch leftTp {
		case TSTR:
			return e.execBetweenBatch(chunk, false, ctx)
		default:
			return e.execBetweenBatch(chunk, true, ctx)
		}
	}
	return nil, NewExecuteError(e.GetPos(), "Unknown operator %v", e.Op)
}

func (e *BinaryOpExpr) execEqualBatch(chunk []KVPair, not bool, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	var (
		isStr  = false
		isInt  = false
		isBool = false
	)
	if len(chunk) == 0 {
		return nil, nil
	}

	switch rleft[0].(type) {
	case string, []byte:
		isStr = true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		isInt = true
	case bool:
		isBool = true
	default:
		return nil, NewExecuteError(e.GetPos(), "= operator left expression has wrong type")
	}

	for i := 0; i < len(chunk); i++ {
		if isStr {
			left, lok := convertToByteArray(rleft[i])
			right, rok := convertToByteArray(rright[i])
			if !lok || !rok {
				return nil, NewExecuteError(e.GetPos(), "= operator left or right expression has wrong type")
			}
			if not {
				rleft[i] = !bytes.Equal(left, right)
			} else {
				rleft[i] = bytes.Equal(left, right)
			}
		}
		if isInt {
			left, lok := convertToInt(rleft[i])
			right, rok := convertToInt(rright[i])
			if !lok || !rok {
				return nil, NewExecuteError(e.GetPos(), "= operator left or right expression has wrong type")
			}
			if not {
				rleft[i] = left != right
			} else {
				rleft[i] = left == right
			}
		}
		if isBool {
			left, lok := rleft[i].(bool)
			right, rok := rright[i].(bool)
			if !lok || !rok {
				return nil, NewExecuteError(e.GetPos(), "= operator left or right expression has wrong type")
			}
			if not {
				rleft[i] = left != right
			} else {
				rleft[i] = left == right
			}
		}
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execPrefixMatchBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		left, lok := convertToByteArray(rleft[i])
		right, rok := convertToByteArray(rright[i])
		if !lok || !rok {
			return nil, NewExecuteError(e.GetPos(), "^= operator left or right expression has wrong type")
		}
		rleft[i] = bytes.HasPrefix(left, right)
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execRegexpMatchBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	var (
		regexpCache = make(map[string]*regexp.Regexp)
	)
	for i := 0; i < len(chunk); i++ {
		left, lok := convertToByteArray(rleft[i])
		right, rok := convertToByteArray(rright[i])
		if !lok || !rok {
			return nil, NewExecuteError(e.GetPos(), "~= operator left or right expression has wrong type")
		}
		regKey := string(right)
		reg, have := regexpCache[regKey]
		if !have {
			reg, err = regexp.Compile(regKey)
			if err != nil {
				return nil, err
			}
			regexpCache[regKey] = reg
		}
		rleft[i] = reg.Match(left)
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execAndOrBatch(chunk []KVPair, and bool, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		left, lok := rleft[i].(bool)
		right, rok := rright[i].(bool)
		if !lok || !rok {
			if and {
				return nil, NewExecuteError(e.GetPos(), "& operator left or right expression has wrong type, not boolean")
			} else {
				return nil, NewExecuteError(e.GetPos(), "| operator left or right expression has wrong type, not boolean")
			}
		}
		if and {
			rleft[i] = left && right
		} else {
			rleft[i] = left || right
		}
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execMathBatch(chunk []KVPair, op byte, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := executeMathOp(rleft[i], rright[i], op, e.Right)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execNumberCompareBatch(chunk []KVPair, op string, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := execNumberCompare(rleft[i], rright[i], op)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execStringCompareBatch(chunk []KVPair, op string, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rright, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		val, err := execStringCompare(rleft[i], rright[i], op)
		if err != nil {
			return nil, err
		}
		rleft[i] = val
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execInBatch(chunk []KVPair, number bool, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}

	var (
		listValues [][]any
		cmp        bool
		values     []any
		cmpRet     bool
		ok         bool
	)

	switch rlist := e.Right.(type) {
	case *ListExpr:
		listValues = make([][]any, len(rlist.List))
		for l, expr := range rlist.List {
			if number && expr.ReturnType() != TNUMBER {
				return nil, NewExecuteError(expr.GetPos(), "in operator right expression element has wrong type, not number")
			}
			if !number && expr.ReturnType() != TSTR {
				return nil, NewExecuteError(expr.GetPos(), "in operator right expression element has wrong type, not string")
			}
			values, err = expr.ExecuteBatch(chunk, ctx)
			if err != nil {
				return nil, err
			}
			listValues[l] = values
		}
		for i := 0; i < len(chunk); i++ {
			cmpRet = false
			left := rleft[i]
			for j := 0; j < len(listValues); j++ {
				lval := listValues[j][i]
				if number {
					cmp, err = execNumberCompare(left, lval, "=")
				} else {
					cmp, err = execStringCompare(left, lval, "=")
				}
				if err != nil {
					return nil, err
				}
				if cmp {
					cmpRet = true
					break
				}
			}
			rleft[i] = cmpRet
		}
		return rleft, nil
	case *FunctionCallExpr, *FieldReferenceExpr:
		frets, err := rlist.ExecuteBatch(chunk, ctx)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(chunk); i++ {
			cmpRet = false
			values, ok = unpackArray(frets[i])
			if !ok {
				return nil, NewExecuteError(e.GetPos(), "in operator right expression has wrong type, not list")
			}
			left := rleft[i]
			for j := 0; j < len(values); j++ {
				lval := values[j]
				if number {
					cmp, err = execNumberCompare(left, lval, "=")
				} else {
					cmp, err = execStringCompare(left, lval, "=")
				}
				if err != nil {
					return nil, err
				}
				if cmp {
					cmpRet = true
					break
				}
			}
			rleft[i] = cmpRet
		}
		return rleft, nil
	default:
		return nil, NewExecuteError(e.GetPos(), "in operator right expression has wrong type, not list 2")
	}
}

func (e *BinaryOpExpr) execBetweenBatch(chunk []KVPair, number bool, ctx *ExecuteCtx) ([]any, error) {
	rleft, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	rlist, ok := e.Right.(*ListExpr)
	if !ok || len(rlist.List) != 2 {
		return nil, NewExecuteError(e.Right.GetPos(), "between operator right expression invalid")
	}
	lexpr := rlist.List[0]
	uexpr := rlist.List[1]
	if !number && lexpr.ReturnType() != TSTR {
		return nil, NewExecuteError(lexpr.GetPos(), "between operator lower boundary expression has wrong type, not string")
	}
	if !number && lexpr.ReturnType() != TSTR {
		return nil, NewExecuteError(uexpr.GetPos(), "between operator upper boundary expression has wrong type, not string")
	}
	if number && lexpr.ReturnType() != TNUMBER {
		return nil, NewExecuteError(lexpr.GetPos(), "between operator lower boundary expression has wrong type, not number")
	}
	if number && uexpr.ReturnType() != TNUMBER {
		return nil, NewExecuteError(uexpr.GetPos(), "between operator upper boundary expression has wrong type, not number")
	}
	lbvals, err := lexpr.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	ubvals, err := uexpr.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	var (
		cmp, lcmp, ucmp bool
	)
	for i := 0; i < len(chunk); i++ {
		if number {
			cmp, err = execNumberCompare(lbvals[i], ubvals[i], "<")
		} else {
			cmp, err = execStringCompare(lbvals[i], ubvals[i], "<")
		}
		if err != nil {
			return nil, err
		}
		if !cmp {
			return nil, NewExecuteError(e.GetPos(), "between operator lower boundary is greater than upper boundary")
		}
		if number {
			lcmp, err = execNumberCompare(lbvals[i], rleft[i], "<=")
		} else {
			lcmp, err = execStringCompare(lbvals[i], rleft[i], "<=")
		}
		if err != nil {
			return nil, err
		}
		// left < lower, next
		if !lcmp {
			rleft[i] = false
			continue
		}
		if number {
			ucmp, err = execNumberCompare(rleft[i], ubvals[i], "<=")
		} else {
			ucmp, err = execStringCompare(rleft[i], ubvals[i], "<=")
		}
		if err != nil {
			return nil, err
		}
		// left < upper
		rleft[i] = ucmp
	}
	return rleft, nil
}

func (e *BinaryOpExpr) execStringConcateBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	left, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(chunk); i++ {
		lval, lok := convertToByteArray(left[i])
		rval, rok := convertToByteArray(right[i])
		if !lok || !rok {
			return nil, NewExecuteError(e.GetPos(), "+ operator left or right expression has wrong type, not string")
		}
		cval := make([]byte, 0, len(lval)+len(rval))
		cval = append(cval, lval...)
		cval = append(cval, rval...)
		left[i] = cval
	}
	return left, nil
}

func (e *FunctionCallExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	var (
		ret = make([]any, len(chunk))
	)
	if e.Result != nil {
		for i := 0; i < len(chunk); i++ {
			ret[i] = e.Result
		}
		return ret, nil
	}

	funcObj, err := GetScalarFunction(e)
	if err != nil {
		return nil, err
	}
	if !funcObj.VarArgs && len(e.Args) != funcObj.NumArgs {
		return nil, NewExecuteError(e.GetPos(), "Function %s require %d arguments but got %d", funcObj.Name, funcObj.NumArgs, len(e.Args))
	}
	return e.executeFuncBatch(funcObj, chunk, ctx)
}

func (e *FunctionCallExpr) executeFuncBatch(funcObj *Function, chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	if funcObj.BodyVec != nil {
		return funcObj.BodyVec(chunk, e.Args, ctx)
	}

	var (
		ret = make([]any, len(chunk))
		err error
	)
	for i := 0; i < len(chunk); i++ {
		ret[i], err = funcObj.Body(chunk[i], e.Args, ctx)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (e *FieldAccessExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	left, err := e.Left.ExecuteBatch(chunk, ctx)
	if err != nil {
		return nil, err
	}
	switch fnval := e.FieldName.(type) {
	case *StringExpr:
		return e.execDictAccessBatch(fnval.Data, left)
	case *NumberExpr:
		return e.execListAccessBatch(int(fnval.Int), left)
	}
	return nil, NewSyntaxError(e.FieldName.GetPos(), "Invalid field name")
}

func (e *FieldAccessExpr) execDictAccessBatch(fieldName string, left []any) ([]any, error) {
	for i := 0; i < len(left); i++ {
		var (
			fval any
			have bool
		)
		switch lval := left[i].(type) {
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
			left[i] = ""
		} else {
			left[i] = fval
		}
	}
	return left, nil
}

func (e *FieldAccessExpr) execListAccessBatch(idx int, left []any) ([]any, error) {
	for i := 0; i < len(left); i++ {
		var (
			fval any
			have bool
		)
		switch lval := left[i].(type) {
		case []any:
			lvallen := len(lval)
			if idx < lvallen {
				have = true
				fval = lval[idx]
			}
		case []string:
			lvallen := len(lval)
			if idx < lvallen {
				have = true
				fval = lval[idx]
			}
		case []int64:
			lvallen := len(lval)
			if idx < lvallen {
				have = true
				fval = lval[idx]
			}
		case []float64:
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
			left[i] = ""
		} else {
			left[i] = fval
		}
	}
	return left, nil
}

func (e *FieldReferenceExpr) ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error) {
	if ctx != nil {
		cval, have := ctx.GetChunkFieldResult(e.Name.Data, chunk[0].Key)
		if have {
			// Copy cached data
			retCopy := make([]any, len(cval))
			copy(retCopy, cval)
			ctx.UpdateHit()
			return retCopy, nil
		}
	}
	ret, err := e.FieldExpr.ExecuteBatch(chunk, ctx)
	if err != nil {
		return ret, err
	}
	if ctx != nil {
		// We should copy result to refuse result overwrite by later execute functions
		retCopy := make([]any, len(ret))
		copy(retCopy, ret)
		ctx.SetChunkFieldResult(e.Name.Data, chunk[0].Key, retCopy)
	}
	return ret, err
}
