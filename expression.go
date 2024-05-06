package kvql

import (
	"fmt"
	"strconv"
	"strings"
)

/*
Query Examples:
	query 'where key ^= "test"'        // key prefix match
	query 'where key ~= "[regexp]"'    // key regexp match
	query 'where value ^= "test"'      // value prefix match
	query 'where value ~= "[regexp]"'  // value regexp match
*/

type KVKeyword byte
type Operator byte
type Type byte

const (
	KeyKW   KVKeyword = 1
	ValueKW KVKeyword = 2

	Unknown     Operator = 0
	And         Operator = 1
	Or          Operator = 2
	Not         Operator = 3
	Eq          Operator = 4
	NotEq       Operator = 5
	PrefixMatch Operator = 6
	RegExpMatch Operator = 7
	Add         Operator = 8
	Sub         Operator = 9
	Mul         Operator = 10
	Div         Operator = 11
	Gt          Operator = 12
	Gte         Operator = 13
	Lt          Operator = 14
	Lte         Operator = 15
	In          Operator = 16
	Between     Operator = 17
	KWAnd       Operator = 18
	KWOr        Operator = 19

	TUNKNOWN Type = 0
	TBOOL    Type = 1
	TSTR     Type = 2
	TNUMBER  Type = 3
	TIDENT   Type = 4
	TLIST    Type = 5
	TJSON    Type = 6
)

var (
	KVKeywordToString = map[KVKeyword]string{
		KeyKW:   "KEY",
		ValueKW: "VALUE",
	}

	OperatorToString = map[Operator]string{
		Eq:          "=",
		NotEq:       "!=",
		And:         "&",
		Or:          "|",
		Not:         "!",
		PrefixMatch: "^=",
		RegExpMatch: "~=",
		Add:         "+",
		Sub:         "-",
		Mul:         "*",
		Div:         "/",
		Gt:          ">",
		Gte:         ">=",
		Lt:          "<",
		Lte:         "<=",
		In:          "in",
		Between:     "between",
		KWAnd:       "and",
		KWOr:        "or",
	}

	StringToOperator = map[string]Operator{
		"=":       Eq,
		"&":       And,
		"|":       Or,
		"!":       Not,
		"^=":      PrefixMatch,
		"~=":      RegExpMatch,
		"!=":      NotEq,
		"+":       Add,
		"-":       Sub,
		"*":       Mul,
		"/":       Div,
		">":       Gt,
		">=":      Gte,
		"<":       Lt,
		"<=":      Lte,
		"in":      In,
		"between": Between,
		"and":     KWAnd,
		"or":      KWOr,
	}
)

func BuildOp(pos int, op string) (Operator, error) {
	ret, have := StringToOperator[op]
	if !have {
		return Unknown, NewSyntaxError(pos, "Unknown operator")
	}
	return ret, nil
}

/*
query: where key ^= "test" & value ~= "test"
WhereStmt {
	Expr: BinaryOpExpr {
		Op: "&",
		Left: BinaryOpExpr {
			Op: "^=",
			Left: FieldExpr{Field: KEY},
			Right: StringExpr{Data: "test"},
		},
		Right: BinaryOpExpr {
			Op: "~=",
			Left: FieldExpr{Field: VALUE},
			Right: StringExpr{Data: "test"},
		}
	},
}
*/

var (
	_ Expression = (*BinaryOpExpr)(nil)
	_ Expression = (*FieldExpr)(nil)
	_ Expression = (*FieldReferenceExpr)(nil)
	_ Expression = (*StringExpr)(nil)
	_ Expression = (*NotExpr)(nil)
	_ Expression = (*FunctionCallExpr)(nil)
	_ Expression = (*NameExpr)(nil)
	_ Expression = (*NumberExpr)(nil)
	_ Expression = (*FloatExpr)(nil)
	_ Expression = (*BoolExpr)(nil)
	_ Expression = (*ListExpr)(nil)
	_ Expression = (*FieldAccessExpr)(nil)
)

type CheckCtx struct {
	Fields        []Expression
	FieldNames    []string
	FieldTypes    []Type
	NotAllowKey   bool
	NotAllowValue bool
}

func (c *CheckCtx) GetNamedExpr(name string) (Expression, bool) {
	for i, fname := range c.FieldNames {
		if fname == name {
			if len(c.Fields) > i {
				return c.Fields[i], true
			}
		}
	}
	return nil, false
}

type WalkCallback func(e Expression) bool

type Expression interface {
	Check(ctx *CheckCtx) error
	String() string
	Execute(kv KVPair, ctx *ExecuteCtx) (any, error)
	ExecuteBatch(chunk []KVPair, ctx *ExecuteCtx) ([]any, error)
	ReturnType() Type
	GetPos() int
	Walk(cb WalkCallback)
}

type BinaryOpExpr struct {
	Pos   int
	Op    Operator
	Left  Expression
	Right Expression
}

func (e *BinaryOpExpr) String() string {
	op := OperatorToString[e.Op]
	switch op {
	case "between":
		list, ok := e.Right.(*ListExpr)
		if !ok || len(list.List) != 2 {
			return fmt.Sprintf("(%s %s %s)", e.Left.String(), op, e.Right.String())
		}
		return fmt.Sprintf("(%s BETWEEN %s AND %s)", e.Left.String(), list.List[0].String(), list.List[1].String())
	default:
		return fmt.Sprintf("(%s %s %s)", e.Left.String(), op, e.Right.String())
	}
}

func (e *BinaryOpExpr) GetPos() int {
	return e.Pos
}

func (e *BinaryOpExpr) ReturnType() Type {
	switch e.Op {
	case And, Or, Not, Eq, NotEq, PrefixMatch, RegExpMatch, Gt, Gte, Lt, Lte, In, Between, KWAnd, KWOr:
		return TBOOL
	case Sub, Mul, Div:
		return TNUMBER
	case Add:
		if e.Left.ReturnType() == TSTR {
			return TSTR
		}
		return TNUMBER
	}
	return TUNKNOWN
}

type FieldExpr struct {
	Pos   int
	Field KVKeyword
}

func (e *FieldExpr) String() string {
	return fmt.Sprintf("%s", KVKeywordToString[e.Field])
}

func (e *FieldExpr) ReturnType() Type {
	return TSTR
}

func (e *FieldExpr) GetPos() int {
	return e.Pos
}

type StringExpr struct {
	Pos  int
	Data string
}

func (e *StringExpr) String() string {
	return fmt.Sprintf("'%s'", e.Data)
}

func (e *StringExpr) ReturnType() Type {
	return TSTR
}

func (e *StringExpr) GetPos() int {
	return e.Pos
}

type NotExpr struct {
	Pos   int
	Right Expression
}

func (e *NotExpr) String() string {
	return fmt.Sprintf("!(%s)", e.Right.String())
}

func (e *NotExpr) ReturnType() Type {
	return TBOOL
}

func (e *NotExpr) GetPos() int {
	return e.Pos
}

type FunctionCallExpr struct {
	Pos    int
	Name   Expression
	Args   []Expression
	Result any
}

func (e *FunctionCallExpr) GetPos() int {
	return e.Pos
}

func (e *FunctionCallExpr) String() string {
	args := make([]string, len(e.Args))
	for i, expr := range e.Args {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", e.Name.String(), strings.Join(args, ", "))
}

func (e *FunctionCallExpr) ReturnType() Type {
	fname, err := GetFuncNameFromExpr(e)
	if err != nil {
		return TUNKNOWN
	}

	if funcObj, have := GetScalarFunctionByName(fname); have {
		return funcObj.ReturnType
	}
	if funcObj, have := GetAggrFunctionByName(fname); have {
		return funcObj.ReturnType
	}
	return TUNKNOWN
}

type NameExpr struct {
	Pos  int
	Data string
}

func (e *NameExpr) GetPos() int {
	return e.Pos
}

func (e *NameExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *NameExpr) ReturnType() Type {
	return TIDENT
}

type FieldReferenceExpr struct {
	Name      *NameExpr
	FieldExpr Expression
}

func (e *FieldReferenceExpr) GetPos() int {
	return e.Name.Pos
}

func (e *FieldReferenceExpr) String() string {
	return fmt.Sprintf("`%s`", e.Name.Data)
}

func (e *FieldReferenceExpr) ReturnType() Type {
	return e.FieldExpr.ReturnType()
}

type NumberExpr struct {
	Pos  int
	Data string
	Int  int64
}

func (e *NumberExpr) GetPos() int {
	return e.Pos
}

func newNumberExpr(pos int, data string) *NumberExpr {
	num, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		num = 0
	}
	return &NumberExpr{
		Pos:  pos,
		Data: data,
		Int:  num,
	}
}

func (e *NumberExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *NumberExpr) ReturnType() Type {
	return TNUMBER
}

type FloatExpr struct {
	Pos   int
	Data  string
	Float float64
}

func (e *FloatExpr) GetPos() int {
	return e.Pos
}

func newFloatExpr(pos int, data string) *FloatExpr {
	num, err := strconv.ParseFloat(data, 64)
	if err != nil {
		num = 0.0
	}
	return &FloatExpr{
		Pos:   pos,
		Data:  data,
		Float: num,
	}
}

func (e *FloatExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *FloatExpr) ReturnType() Type {
	return TNUMBER
}

type BoolExpr struct {
	Pos  int
	Data string
	Bool bool
}

func (e *BoolExpr) String() string {
	return fmt.Sprintf("%s", e.Data)
}

func (e *BoolExpr) ReturnType() Type {
	return TBOOL
}

func (e *BoolExpr) GetPos() int {
	return e.Pos
}

type ListExpr struct {
	Pos  int
	List []Expression
}

func (e *ListExpr) GetPos() int {
	return e.Pos
}

func (e *ListExpr) String() string {
	ret := make([]string, len(e.List))
	for i, item := range e.List {
		ret[i] = item.String()
	}
	return fmt.Sprintf("(%s)", strings.Join(ret, ", "))
}

func (e *ListExpr) ReturnType() Type {
	return TLIST
}

type FieldAccessExpr struct {
	Pos       int
	Left      Expression
	FieldName Expression
}

func (e *FieldAccessExpr) GetPos() int {
	return e.Pos
}

func (e *FieldAccessExpr) String() string {
	left := e.Left.String()
	fname := e.FieldName.String()
	return fmt.Sprintf("%s[%s]", left, fname)
}

func (e *FieldAccessExpr) ReturnType() Type {
	return TSTR
}
