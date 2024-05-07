package kvql

func (e *BinaryOpExpr) Walk(cb WalkCallback) {
	if cb(e) {
		e.Left.Walk(cb)
		e.Right.Walk(cb)
	}
}

func (e *FieldExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *FieldReferenceExpr) Walk(cb WalkCallback) {
	if cb(e) {
		e.FieldExpr.Walk(cb)
	}
}

func (e *StringExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *NotExpr) Walk(cb WalkCallback) {
	if cb(e) {
		e.Right.Walk(cb)
	}
}

func (e *FunctionCallExpr) Walk(cb WalkCallback) {
	if cb(e) {
		e.Name.Walk(cb)
		for _, arg := range e.Args {
			arg.Walk(cb)
		}
	}
}

func (e *NameExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *NumberExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *FloatExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *BoolExpr) Walk(cb WalkCallback) {
	cb(e)
}

func (e *ListExpr) Walk(cb WalkCallback) {
	if cb(e) {
		for _, item := range e.List {
			item.Walk(cb)
		}
	}
}

func (e *FieldAccessExpr) Walk(cb WalkCallback) {
	if cb(e) {
		e.Left.Walk(cb)
		e.FieldName.Walk(cb)
	}
}
