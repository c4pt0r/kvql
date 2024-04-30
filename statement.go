package kvql

import "fmt"

var (
	_ Statement = (*WhereStmt)(nil)
	_ Statement = (*OrderStmt)(nil)
	_ Statement = (*GroupByStmt)(nil)
	_ Statement = (*LimitStmt)(nil)
	_ Statement = (*PutStmt)(nil)
	_ Statement = (*RemoveStmt)(nil)
)

type Statement interface {
	Name() string
}

type SelectStmt struct {
	Pos        int
	AllFields  bool
	FieldNames []string
	FieldTypes []Type
	Fields     []Expression
	Where      *WhereStmt
	Order      *OrderStmt
	Limit      *LimitStmt
	GroupBy    *GroupByStmt
}

func (s *SelectStmt) Name() string {
	return "SELECT"
}

type WhereStmt struct {
	Pos  int
	Expr Expression
}

func (s *WhereStmt) Name() string {
	return "WHERE"
}

type OrderField struct {
	Name  string
	Field Expression
	Order TokenType
}

type OrderStmt struct {
	Pos    int
	Orders []OrderField
}

func (s *OrderStmt) Name() string {
	return "ORDER BY"
}

type GroupByField struct {
	Name string
	Expr Expression
}

type GroupByStmt struct {
	Pos    int
	Fields []GroupByField
}

func (s *GroupByStmt) Name() string {
	return "GROUP BY"
}

type LimitStmt struct {
	Pos   int
	Start int
	Count int
}

func (s *LimitStmt) Name() string {
	return "LIMIT"
}

type PutKVPair struct {
	Key   Expression
	Value Expression
}

func (p *PutKVPair) String() string {
	return fmt.Sprintf("{%s: %s}", p.Key.String(), p.Value.String())
}

type PutStmt struct {
	Pos     int
	KVPairs []*PutKVPair
}

func (s *PutStmt) Name() string {
	return "PUT"
}

type RemoveStmt struct {
	Pos  int
	Keys []Expression
}

func (s *RemoveStmt) Name() string {
	return "REMOVE"
}

type DeleteStmt struct {
	Pos   int
	Where *WhereStmt
	Limit *LimitStmt
}

func (s *DeleteStmt) Name() string {
	return "DELETE"
}

func (s *RemoveStmt) Validate(ctx *CheckCtx) error {
	for _, expr := range s.Keys {
		rtype := expr.ReturnType()
		if rtype != TSTR && rtype != TNUMBER {
			return NewSyntaxError(expr.GetPos(), "need str or number type")
		}
		if err := expr.Check(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *PutStmt) Validate(ctx *CheckCtx) error {
	for _, kv := range s.KVPairs {
		if err := s.validateKVPair(kv, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *PutStmt) validateKVPair(kv *PutKVPair, ctx *CheckCtx) error {
	if err := kv.Key.Check(ctx); err != nil {
		return err
	}
	switch kv.Key.ReturnType() {
	case TSTR, TNUMBER:
		break
	default:
		return NewSyntaxError(kv.Key.GetPos(), "need str or number type")
	}
	if err := kv.Value.Check(ctx); err != nil {
		return err
	}
	switch kv.Value.ReturnType() {
	case TSTR, TNUMBER:
		break
	default:
		return NewSyntaxError(kv.Value.GetPos(), "need str or number type")
	}
	return nil
}

func (s *DeleteStmt) Validate(ctx *CheckCtx) error {
	return s.Where.Expr.Check(ctx)
}

func (s *SelectStmt) ValidateFields(ctx *CheckCtx) error {
	for _, f := range s.Fields {
		if err := s.validateField(f, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStmt) validateField(f Expression, ctx *CheckCtx) error {
	if err := f.Check(ctx); err != nil {
		return err
	}

	return s.checkAggrFunctionArgs(f)
}

func (s *SelectStmt) checkAggrFunctionArgs(expr Expression) error {
	var err error
	switch e := expr.(type) {
	case *BinaryOpExpr:
		err = s.checkAggrFunctionArgs(e.Left)
		if err != nil {
			return err
		}
		err = s.checkAggrFunctionArgs(e.Right)
		if err != nil {
			return err
		}
	case *FunctionCallExpr:
		fname, err := GetFuncNameFromExpr(e)
		if err == nil && IsAggrFunc(fname) {
			err = s.checkAggrFuncArgs(e.Args)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SelectStmt) checkAggrFuncArgs(args []Expression) error {
	for _, arg := range args {
		if err := s.checkAggrFuncArg(arg); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStmt) checkAggrFuncArg(arg Expression) error {
	var err error
	switch e := arg.(type) {
	case *BinaryOpExpr:
		err = s.checkAggrFuncArg(e.Left)
		if err != nil {
			return err
		}
		err = s.checkAggrFuncArg(e.Right)
		if err != nil {
			return err
		}
	case *FunctionCallExpr:
		fname, err := GetFuncNameFromExpr(e)
		if err == nil && IsAggrFunc(fname) {
			return NewSyntaxError(arg.GetPos(), "Aggregate function arguments should not contains aggregate function")
		}
	}
	return nil
}
