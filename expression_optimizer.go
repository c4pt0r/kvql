package kvql

import "fmt"

type ExpressionOptimizer struct {
	Root   Expression
	parent Expression
}

func (o *ExpressionOptimizer) Optimize() Expression {
	// Optimize twice will fully evaluate constant
	newRoot := o.optimize(o.Root)
	newRoot = o.optimize(newRoot)
	return newRoot
}

func (o *ExpressionOptimizer) optimize(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		o.tryReorderBinaryOp(e)
		nexpr, _ := o.tryOptimizeBinaryOpExecute(e)
		nexpr, _ = o.tryOptimizeAndOr(nexpr)
		return nexpr
	case *FunctionCallExpr:
		nexpr, _ := o.tryOptimizeFunctionCall(e)
		return nexpr
	}
	return expr
}

func (o *ExpressionOptimizer) tryReorderBinaryOp(e *BinaryOpExpr) {
	var (
		leftIsValue  = false
		rightIsValue = false
		leftIsOp     = false
		rightIsOp    = false
		leftOpExpr   *BinaryOpExpr
	)

	switch left := e.Left.(type) {
	case *BinaryOpExpr:
		o.tryReorderBinaryOp(left)
		leftOpExpr = left
		leftIsOp = true
	case *StringExpr, *NumberExpr, *FloatExpr:
		leftIsValue = true
	}

	switch right := e.Right.(type) {
	case *BinaryOpExpr:
		o.tryReorderBinaryOp(right)
		rightIsOp = true
	case *StringExpr, *NumberExpr, *FloatExpr:
		rightIsValue = true
	}

	if e.Op != Add && e.Op != Mul {
		return
	}

	if !leftIsValue && leftIsOp && rightIsValue && !rightIsOp {
		// fmt.Println("DEBUG:", e)
		if leftOpExpr.Op == e.Op {
			switch rexpr := leftOpExpr.Right.(type) {
			case *StringExpr, *NumberExpr, *FloatExpr:
				// (ANY op VALUE) op VALUE
				e.Left = leftOpExpr.Left
				e.Right = &BinaryOpExpr{Pos: e.GetPos(), Op: e.Op, Left: leftOpExpr.Right, Right: e.Right}
			case *BinaryOpExpr:
				if isBinaryOpExprAllValue(rexpr, e.Op) {
					e.Left = leftOpExpr.Left
					e.Right = &BinaryOpExpr{Pos: e.GetPos(), Op: e.Op, Left: leftOpExpr.Right, Right: e.Right}
				}
			}
		}
		// fmt.Println("DEBUG:", e)
	}
	return
}

func isBinaryOpExprAllValue(expr *BinaryOpExpr, op Operator) bool {
	if expr.Op != op {
		return false
	}
	lIsValue := false
	rIsValue := false
	switch le := expr.Left.(type) {
	case *StringExpr, *NumberExpr, *FloatExpr:
		lIsValue = true
	case *BinaryOpExpr:
		lIsValue = isBinaryOpExprAllValue(le, op)
	}

	switch re := expr.Right.(type) {
	case *StringExpr, *NumberExpr, *FloatExpr:
		rIsValue = true
	case *BinaryOpExpr:
		rIsValue = isBinaryOpExprAllValue(re, op)
	}
	return lIsValue && rIsValue
}

func (o *ExpressionOptimizer) tryOptimizeBinaryOpExecute(e *BinaryOpExpr) (Expression, bool) {
	leftIsValue := false
	rightIsValue := false
	switch left := e.Left.(type) {
	case *BinaryOpExpr:
		e.Left, leftIsValue = o.tryOptimizeBinaryOpExecute(left)
	case *FunctionCallExpr:
		e.Left, leftIsValue = o.tryOptimizeFunctionCall(left)
	case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
		leftIsValue = true
	}

	switch right := e.Right.(type) {
	case *BinaryOpExpr:
		e.Right, rightIsValue = o.tryOptimizeBinaryOpExecute(right)
	case *FunctionCallExpr:
		e.Right, rightIsValue = o.tryOptimizeFunctionCall(right)
	case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
		rightIsValue = true
	}
	// Not value
	if !(leftIsValue && rightIsValue) {
		return e, false
	}
	leftPos := e.Left.GetPos()
	switch e.Op {
	case Add, Sub, Mul, Div:
		ret, err := e.Execute(NewKVP(nil, nil), nil)
		if err == nil {
			switch e.Left.(type) {
			case *StringExpr:
				return &StringExpr{Pos: leftPos, Data: ret.(string)}, true
			case *NumberExpr:
				switch cret := ret.(type) {
				case int64:
					return &NumberExpr{Pos: leftPos, Data: fmt.Sprintf("%v", cret), Int: cret}, true
				case float64:
					return &NumberExpr{Pos: leftPos, Data: fmt.Sprintf("%v", int64(cret)), Int: int64(cret)}, true
				}
			case *FloatExpr:
				switch cret := ret.(type) {
				case int64:
					return &FloatExpr{Pos: leftPos, Data: fmt.Sprintf("%v", float64(cret)), Float: float64(cret)}, true
				case float64:
					return &FloatExpr{Pos: leftPos, Data: fmt.Sprintf("%v", cret), Float: cret}, true
				}
			}
		}
	case And, Or:
		ret, err := e.Execute(NewKVP(nil, nil), nil)
		if err == nil {
			return &BoolExpr{Pos: leftPos, Data: fmt.Sprintf("%v", ret), Bool: ret.(bool)}, true
		}
	case Eq, NotEq, Gt, Gte, Lt, Lte:
		ret, err := e.Execute(NewKVP(nil, nil), nil)
		if err == nil {
			return &BoolExpr{Pos: leftPos, Data: fmt.Sprintf("%v", ret), Bool: ret.(bool)}, true
		}
	}
	return e, false
}

func (o *ExpressionOptimizer) tryOptimizeAndOr(expr Expression) (Expression, bool) {
	var (
		leftVal      bool
		rightVal     bool
		leftIsValue  = false
		rightIsValue = false
	)

	e, ok := expr.(*BinaryOpExpr)
	if !ok {
		return expr, false
	}
	if e.Op != And && e.Op != Or {
		return e, false
	}
	switch left := e.Left.(type) {
	case *BoolExpr:
		leftIsValue = true
		leftVal = left.Bool
	}

	switch right := e.Right.(type) {
	case *BoolExpr:
		rightIsValue = true
		rightVal = right.Bool
	}

	if leftIsValue && !rightIsValue {
		switch e.Op {
		case And:
			if leftVal {
				// true & Expr => Expr
				return e.Right, true
			} else {
				// false & Expr => false
				return &BoolExpr{Pos: e.Left.GetPos(), Data: "false", Bool: false}, true
			}
		case Or:
			if leftVal {
				// true | Expr => true
				return &BoolExpr{Pos: e.Left.GetPos(), Data: "true", Bool: true}, true
			} else {
				// false | Expr => Expr
				return e.Right, true
			}
		}
	}

	if rightIsValue && !leftIsValue {
		switch e.Op {
		case And:
			if rightVal {
				// Expr & true => Expr
				return e.Left, true
			} else {
				// Expr & false => false
				return &BoolExpr{Pos: e.Right.GetPos(), Data: "false", Bool: false}, true
			}
		case Or:
			if rightVal {
				// Expr | true => true
				return &BoolExpr{Pos: e.Right.GetPos(), Data: "true", Bool: true}, true
			} else {
				// Expr | false => Expr
				return e.Left, true
			}
		}
	}

	if rightIsValue && leftIsValue {
		switch e.Op {
		case And:
			if leftVal && rightVal {
				return &BoolExpr{Pos: e.Left.GetPos(), Data: "true", Bool: true}, true
			}
			return &BoolExpr{Pos: e.Left.GetPos(), Data: "false", Bool: false}, true
		case Or:
			if leftVal || rightVal {
				return &BoolExpr{Pos: e.Left.GetPos(), Data: "true", Bool: true}, true
			}
			return &BoolExpr{Pos: e.Left.GetPos(), Data: "false", Bool: false}, true
		}
	}

	return e, false
}

func (o *ExpressionOptimizer) tryOptimizeFunctionCall(e *FunctionCallExpr) (Expression, bool) {
	allIsValue := true
	for i, arg := range e.Args {
		nexpr := o.optimize(arg)
		e.Args[i] = nexpr
		switch nexpr.(type) {
		case *StringExpr, *NumberExpr, *FloatExpr, *BoolExpr:
			// Value
		default:
			allIsValue = false
		}
	}

	if !(allIsValue && IsScalarFuncExpr(e)) {
		return e, false
	}

	retTp := e.ReturnType()
	switch retTp {
	case TJSON:
		return e, false
	}
	ret, err := e.Execute(NewKVP(nil, nil), nil)
	if err == nil {
		switch retTp {
		case TSTR:
			return &StringExpr{Pos: e.GetPos(), Data: ret.(string)}, true
		case TNUMBER:
			iret, ok := ret.(int64)
			if ok {
				return &NumberExpr{Pos: e.GetPos(), Data: fmt.Sprintf("%v", ret), Int: iret}, true
			}
			fret, ok := ret.(float64)
			if ok {
				return &FloatExpr{Pos: e.GetPos(), Data: fmt.Sprintf("%v", ret), Float: fret}, true
			}
		case TBOOL:
			if ret.(bool) {
				return &BoolExpr{Pos: e.GetPos(), Data: "true", Bool: true}, true
			}
			return &BoolExpr{Pos: e.GetPos(), Data: "false", Bool: false}, true
		}
	}
	return e, false
}
