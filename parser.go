package kvql

import (
	"errors"
)

const MaxNestLevel = 1e5

type Parser struct {
	Query   string
	lex     *Lexer
	toks    []*Token
	tok     *Token
	pos     int
	numToks int
	nestLev int
	exprLev int
}

func NewParser(query string) *Parser {
	lex := NewLexer(query)
	toks := lex.Split()
	return &Parser{
		Query:   query,
		lex:     lex,
		toks:    toks,
		pos:     0,
		numToks: len(toks),
		nestLev: 0,
		exprLev: 0,
	}
}

func (p *Parser) incNestLev() error {
	p.nestLev++
	if p.nestLev > MaxNestLevel {
		return errors.New("exceed max nesting depth")
	}
	return nil
}

func (p *Parser) decNestLev() {
	p.nestLev--
}

func (p *Parser) next() *Token {
	if p.pos >= p.numToks {
		p.tok = nil
		return nil
	}
	p.tok = p.toks[p.pos]
	p.pos += 1
	return p.tok
}

func (p *Parser) expect(tok *Token) error {
	if p.tok == nil {
		return NewSyntaxError(-1, "Expect token %s but got EOF", tok.Data)
	}
	if p.tok.Tp != tok.Tp {
		return NewSyntaxError(p.tok.Pos, "Expect token %s bug got %s", tok.Data, p.tok.Data)
	}
	p.next()
	return nil
}

func (p *Parser) tokPrec() (*Token, int) {
	tok := p.tok
	if tok == nil {
		return nil, LowestPrec
	}
	return tok, tok.Precedence()
}

func (p *Parser) expectOp() (*Token, error) {
	if p.tok == nil {
		return nil, nil
	}
	tp := p.tok.Tp
	switch tp {
	case OPERATOR:
		return p.tok, nil
	}
	return nil, NewSyntaxError(p.tok.Pos, "Expect operator but got %s", p.tok.Data)
}

func (p *Parser) parseExpr() (Expression, error) {
	return p.parseBinaryExpr(nil, LowestPrec+1)
}

func (p *Parser) parseBinaryExpr(x Expression, prec1 int) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseUnaryExpr()
		if err != nil {
			return nil, err
		}
	}
	var n int
	defer func() {
		p.nestLev -= n
	}()
	for n = 1; ; n++ {
		err = p.incNestLev()
		if err != nil {
			return nil, err
		}

		opTok, oprec := p.tokPrec()
		if oprec < prec1 {
			return x, nil
		}
		if opTok == nil {
			return x, nil
		}
		err = p.expect(opTok)
		if err != nil {
			return nil, err
		}
		var (
			y   Expression
			err error
		)
		switch opTok.Data {
		case "in":
			// If `(` just parse list expression
			// else just continue to parse as normal expression
			if p.tok.Tp == LPAREN {
				y, err = p.parseList(opTok.Pos)
			} else {
				y, err = p.parseBinaryExpr(nil, oprec+1)
			}
		case "between":
			y, err = p.parseBetween(opTok.Pos, oprec+1)
		default:
			y, err = p.parseBinaryExpr(nil, oprec+1)
		}
		if err != nil {
			return nil, err
		}
		op, err := BuildOp(opTok.Pos, opTok.Data)
		if err != nil {
			return nil, err
		}
		x = &BinaryOpExpr{Pos: opTok.Pos, Op: op, Left: x, Right: y}
	}
}

func (p *Parser) parseUnaryExpr() (Expression, error) {
	p.incNestLev()
	defer func() {
		p.decNestLev()
	}()
	if p.tok == nil {
		return nil, NewSyntaxError(-1, "Unexpected EOF")
	}
	switch p.tok.Tp {
	case OPERATOR:
		switch p.tok.Data {
		case "!":
			pos := p.tok.Pos
			p.next()
			x, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}
			return &NotExpr{Pos: pos, Right: x}, nil
		}
	}
	return p.parsePrimaryExpr(nil)
}

func (p *Parser) parseFuncCall(fun Expression) (Expression, error) {
	err := p.expect(&Token{Tp: LPAREN, Data: "("})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	var list []Expression
	for p.tok != nil && p.tok.Tp != RPAREN {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, arg)
		if p.tok != nil {
			if p.tok.Tp == RPAREN {
				break
			} else if p.tok.Tp == SEP && p.tok.Data == "," {
				// Correct do nothing
			} else {
				return nil, NewSyntaxError(p.tok.Pos, "Function argument expect `,` or `)` but got %s", p.tok.Data)
			}
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: RPAREN, Data: ")"})
	if err != nil {
		return nil, err
	}
	return &FunctionCallExpr{Pos: fun.GetPos(), Name: fun, Args: list}, nil
}

func (p *Parser) parseFieldAccess(pos int, left Expression) (Expression, error) {
	err := p.expect(&Token{Tp: LBRACK, Data: "["})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	var fieldNames []Expression
	for p.tok != nil && p.tok.Tp != RBRACK {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		fieldNames = append(fieldNames, arg)
		if p.tok != nil && p.tok.Tp == RBRACK {
			break
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: RBRACK, Data: "]"})
	if err != nil {
		return nil, err
	}
	if len(fieldNames) != 1 {
		return nil, NewSyntaxError(pos, "Field access operator should only have one field name")
	}
	return &FieldAccessExpr{Pos: pos, Left: left, FieldName: fieldNames[0]}, nil
}

func (p *Parser) parseList(pos int) (Expression, error) {
	err := p.expect(&Token{Tp: LPAREN, Data: "("})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	var list []Expression
	for p.tok != nil && p.tok.Tp != RPAREN {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, arg)
		if p.tok != nil && p.tok.Tp == RPAREN {
			break
		}
		p.next()
	}
	p.exprLev--
	err = p.expect(&Token{Tp: RPAREN, Data: ")"})
	if err != nil {
		return nil, err
	}
	return &ListExpr{Pos: pos, List: list}, nil
}

func (p *Parser) parseBetween(pos int, oprec int) (Expression, error) {
	lower, err := p.parseBinaryExpr(nil, oprec)
	if err != nil {
		return nil, err
	}
	err = p.expect(&Token{Tp: NAME, Data: "and"})
	if err != nil {
		return nil, err
	}
	upper, err := p.parseBinaryExpr(nil, oprec)
	if err != nil {
		return nil, err
	}
	list := []Expression{lower, upper}
	return &ListExpr{Pos: pos, List: list}, nil
}

func (p *Parser) parsePrimaryExpr(x Expression) (Expression, error) {
	var err error
	if x == nil {
		x, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
	}

	var n int
	defer func() {
		p.nestLev -= n
	}()

	for n = 1; ; n++ {
		p.incNestLev()
		if p.tok == nil {
			return x, nil
		}
		switch p.tok.Tp {
		case LPAREN:
			x, err = p.parseFuncCall(x)
			if err != nil {
				return nil, err
			}
		case LBRACK:
			x, err = p.parseFieldAccess(p.tok.Pos, x)
			if err != nil {
				return nil, err
			}
		default:
			return x, nil
		}
	}
}

func (p *Parser) parseOperand() (Expression, error) {
	switch p.tok.Tp {
	case KEY:
		x := &FieldExpr{Pos: p.tok.Pos, Field: KeyKW}
		p.next()
		return x, nil
	case VALUE:
		x := &FieldExpr{Pos: p.tok.Pos, Field: ValueKW}
		p.next()
		return x, nil
	case STRING:
		x := &StringExpr{Pos: p.tok.Pos, Data: p.tok.Data}
		p.next()
		return x, nil
	case LPAREN:
		p.next()
		p.exprLev++
		x, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.exprLev--
		err = p.expect(&Token{Tp: RPAREN, Data: ")"})
		if err != nil {
			return nil, err
		}
		return x, nil
	case NAME:
		x := &NameExpr{Pos: p.tok.Pos, Data: p.tok.Data}
		p.next()
		return x, nil
	case NUMBER:
		x := newNumberExpr(p.tok.Pos, p.tok.Data)
		p.next()
		return x, nil
	case FLOAT:
		x := newFloatExpr(p.tok.Pos, p.tok.Data)
		p.next()
		return x, nil
	case TRUE:
		x := &BoolExpr{Pos: p.tok.Pos, Data: p.tok.Data, Bool: true}
		p.next()
		return x, nil
	case FALSE:
		x := &BoolExpr{Pos: p.tok.Pos, Data: p.tok.Data, Bool: false}
		p.next()
		return x, nil
	}
	return nil, NewSyntaxError(p.tok.Pos, "Bad Expression")
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	var (
		fields     = []Expression{}
		fieldNames = []string{}
		fieldTypes = []Type{}
		allFields  = false
		err        error
		pos        = p.tok.Pos
	)
	err = p.expect(&Token{Tp: SELECT, Data: "select"})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	for p.tok != nil && p.tok.Tp != WHERE {
		if p.tok.Tp == OPERATOR && p.tok.Data == "*" {
			allFields = true
			p.next()
			if p.tok != nil && p.tok.Tp != WHERE {
				return nil, NewSyntaxError(p.tok.Pos, "Invalid field expression")
			}
			if len(fields) > 0 {
				if p.tok == nil {
					return nil, NewSyntaxError(-1, "Invalid field expression")
				}
				return nil, NewSyntaxError(p.tok.Pos, "Invalid field expression")
			}
			break
		}
		field, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		fieldName := field.String()
		if p.tok != nil {
			if p.tok.Tp == AS {
				p.next()
				if p.tok == nil {
					return nil, NewSyntaxError(-1, "Require field name")
				} else if p.tok.Tp != NAME {
					return nil, NewSyntaxError(p.tok.Pos, "Invalid field name")
				}
				fieldName = p.tok.Data
				p.next()
			} else if p.tok.Tp == SEP && p.tok.Data == "," {
				// Correct do nothing
			} else if p.tok.Tp == WHERE {
				// Correct do nothing
			} else {
				return nil, NewSyntaxError(p.tok.Pos, "Expect `as` or `,` but got %s", p.tok.Data)
			}
		}
		fields = append(fields, field)
		fieldNames = append(fieldNames, fieldName)
		fieldTypes = append(fieldTypes, field.ReturnType())
		if p.tok != nil && p.tok.Tp == WHERE {
			break
		}
		p.next()
	}
	p.exprLev--
	if len(fields) == 0 && !allFields {
		return nil, NewSyntaxError(pos, "Empty fields in select statement")
	}

	if allFields {
		fields = []Expression{&FieldExpr{0, KeyKW}, &FieldExpr{0, ValueKW}}
		fieldNames = []string{fields[0].String(), fields[1].String()}
	}

	return &SelectStmt{
		Pos:        pos,
		Fields:     fields,
		FieldNames: fieldNames,
		FieldTypes: fieldTypes,
		AllFields:  allFields,
	}, nil
}

func (p *Parser) parseLimit() (*LimitStmt, error) {
	var (
		err         error
		shouldBreak bool          = false
		exprs       []*NumberExpr = make([]*NumberExpr, 0, 2)
		ret         *LimitStmt    = &LimitStmt{Pos: p.tok.Pos}
	)
	err = p.expect(&Token{Tp: LIMIT, Data: "limit"})
	if err != nil {
		return nil, err
	}
	for p.tok != nil && !shouldBreak {
		switch p.tok.Tp {
		case NUMBER:
			x := newNumberExpr(p.tok.Pos, p.tok.Data)
			p.next()
			exprs = append(exprs, x)
		case SEP:
			prevPos := p.tok.Pos
			p.next()
			if p.tok == nil {
				return nil, NewSyntaxError(prevPos, "Invalid limit parameters after separator")
			} else if p.tok.Tp != NUMBER {
				return nil, NewSyntaxError(p.tok.Pos, "Invalid limit parameters after separator, require number")
			}
		default:
			shouldBreak = true
		}
	}
	if len(exprs) > 2 {
		if p.tok == nil {
			return nil, NewSyntaxError(-1, "Too many limit parameters")
		}
		return nil, NewSyntaxError(p.tok.Pos, "Too many limit parameters")
	}
	switch len(exprs) {
	case 0:
		if p.tok == nil {
			return nil, NewSyntaxError(-1, "Invalid limit parameters")
		}
		return nil, NewSyntaxError(p.tok.Pos, "Invalid limit parameters")
	case 1:
		ret.Count = int(exprs[0].Int)
	case 2:
		ret.Start = int(exprs[0].Int)
		ret.Count = int(exprs[1].Int)
	}
	return ret, nil
}

func (p *Parser) findFieldInSelect(selStmt *SelectStmt, fieldName string, pos int) (Expression, error) {
	foundIdx := -1
	for i, fname := range selStmt.FieldNames {
		if fname == fieldName {
			foundIdx = i
			break
		}
	}
	if foundIdx < 0 {
		return nil, NewSyntaxError(pos, "Cannot find field %s in select statement", fieldName)
	}
	fexpr := selStmt.Fields[foundIdx]
	switch fexpr.ReturnType() {
	case TSTR, TNUMBER, TBOOL:
		break
	default:
		return nil, NewSyntaxError(fexpr.GetPos(), "Field %s return wrong type", fieldName)
	}
	return fexpr, nil
}

func (p *Parser) parseGroupBy(selStmt *SelectStmt, ctx *CheckCtx) (*GroupByStmt, error) {
	var (
		err         error
		shouldBreak = false
		fields      = []GroupByField{}
		ret         = &GroupByStmt{Pos: p.tok.Pos}
	)
	err = p.expect(&Token{Tp: GROUP, Data: "group"})
	if err != nil {
		return nil, err
	}
	err = p.expect(&Token{Tp: BY, Data: "by"})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	for p.tok != nil && !shouldBreak {
		field, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		switch e := field.(type) {
		case *NameExpr:
			fexpr, err := p.findFieldInSelect(selStmt, e.Data, e.GetPos())
			if err != nil {
				return nil, err
			}
			fields = append(fields, GroupByField{e.Data, fexpr})
		case *FieldExpr:
			fields = append(fields, GroupByField{field.String(), field})
		case *FunctionCallExpr:
			fexpr, err := p.findFieldInSelect(selStmt, field.String(), e.GetPos())
			if err != nil {
				return nil, err
			}
			fname, err := GetFuncNameFromExpr(e)
			if err != nil {
				return nil, err
			}
			if _, have := GetAggrFunctionByName(fname); have {
				return nil, NewSyntaxError(fexpr.GetPos(), "Cannot find aggregate function: %s", fname)
			}
			fields = append(fields, GroupByField{field.String(), fexpr})
		default:
			fexpr, err := p.findFieldInSelect(selStmt, field.String(), e.GetPos())
			if err != nil {
				return nil, err
			}
			fields = append(fields, GroupByField{field.String(), fexpr})
		}
		if p.tok != nil {
			switch p.tok.Tp {
			case SEP:
				p.next()
			default:
				shouldBreak = true
			}
		}
	}
	p.exprLev--
	if len(fields) > 0 {
		for _, f := range fields {
			if err := f.Expr.Check(ctx); err != nil {
				return nil, err
			}
		}
	}
	ret.Fields = fields
	return ret, nil
}

func (p *Parser) parseOrderBy(selStmt *SelectStmt) (*OrderStmt, error) {
	var (
		err         error
		shouldBreak bool         = false
		fields      []OrderField = make([]OrderField, 0, 2)
		ret         *OrderStmt   = &OrderStmt{Pos: p.tok.Pos}
	)
	err = p.expect(&Token{Tp: ORDER, Data: "order"})
	if err != nil {
		return nil, err
	}
	err = p.expect(&Token{Tp: BY, Data: "by"})
	if err != nil {
		return nil, err
	}
	p.exprLev++
	for p.tok != nil && !shouldBreak {
		field, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		var (
			of        OrderField
			fieldName string
		)

		switch e := field.(type) {
		case *NameExpr:
			fieldName = e.Data
		default:
			fieldName = field.String()
		}
		fexpr, err := p.findFieldInSelect(selStmt, fieldName, field.GetPos())
		if err != nil {
			return nil, err
		}
		of = OrderField{
			Name:  fieldName,
			Field: fexpr,
			Order: ASC,
		}

		if p.tok != nil {
			switch p.tok.Tp {
			case SEP:
				p.next()
			case ASC:
				of.Order = ASC
				p.next()
				if p.tok != nil && p.tok.Tp == SEP {
					p.next()
				} else {
					shouldBreak = true
				}
			case DESC:
				of.Order = DESC
				p.next()
				if p.tok != nil && p.tok.Tp == SEP {
					p.next()
				} else {
					shouldBreak = true
				}
			default:
				shouldBreak = true
			}
		}
		fields = append(fields, of)
	}
	p.exprLev--
	ret.Orders = fields
	return ret, nil
}

func (p *Parser) parsePutKVPair() (*PutKVPair, error) {
	var (
		err   error
		key   Expression
		value Expression
	)
	err = p.expect(&Token{Tp: LPAREN, Data: "("})
	if err != nil {
		return nil, err
	}
	key, err = p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.tok.Tp == SEP && p.tok.Data == "," {
		// Correct
		p.next()
	} else {
		return nil, NewSyntaxError(p.tok.Pos, "Put key-value pair expect `,` but got %s", p.tok.Data)
	}
	value, err = p.parseExpr()
	if err != nil {
		return nil, err
	}
	err = p.expect(&Token{Tp: RPAREN, Data: ")"})
	if err != nil {
		return nil, err
	}
	return &PutKVPair{Key: key, Value: value}, nil
}

func (p *Parser) parsePut() (*PutStmt, error) {
	var (
		pos     = p.tok.Pos
		kvpairs = []*PutKVPair{}
		err     error
	)
	err = p.expect(&Token{Tp: PUT, Data: "put"})
	if err != nil {
		return nil, err
	}
	for p.tok != nil {
		kvp, err := p.parsePutKVPair()
		if err != nil {
			return nil, err
		}
		kvpairs = append(kvpairs, kvp)
		if p.tok == nil {
			break
		}
		err = p.expect(&Token{Tp: SEP, Data: ","})
		if err != nil {
			return nil, err
		}
	}
	stmt := &PutStmt{
		Pos:     pos,
		KVPairs: kvpairs,
	}
	checkCtx := &CheckCtx{
		NotAllowValue: true,
	}
	err = stmt.Validate(checkCtx)
	return stmt, err
}

func (p *Parser) parseRemove() (Statement, error) {
	var (
		pos  = p.tok.Pos
		keys = []Expression{}
		err  error
	)
	err = p.expect(&Token{Tp: REMOVE, Data: "remove"})
	if err != nil {
		return nil, err
	}
	for p.tok != nil {
		kexpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		keys = append(keys, kexpr)
		if p.tok == nil {
			break
		}
		err = p.expect(&Token{Tp: SEP, Data: ","})
		if err != nil {
			return nil, err
		}
	}
	stmt := &RemoveStmt{
		Pos:  pos,
		Keys: keys,
	}
	checkCtx := &CheckCtx{
		NotAllowKey:   true,
		NotAllowValue: true,
	}
	err = stmt.Validate(checkCtx)
	return stmt, err
}

func (p *Parser) trimEndSemis() {
	semis := 0
	for i := p.numToks - 1; i > 0; i-- {
		if tok := p.toks[i]; tok.Tp == SEMI {
			semis++
		} else {
			break
		}
	}
	if semis > 0 {
		p.toks = p.toks[:p.numToks-semis]
		p.numToks -= semis
	}
}

func (p *Parser) Parse() (Statement, error) {
	p.trimEndSemis()
	if p.numToks == 0 {
		return nil, NewSyntaxError(-1, "Expect select or where keyword")
	}
	p.next()
	if p.tok == nil {
		return nil, NewSyntaxError(-1, "Expect select or where keyword")
	} else {
		switch p.tok.Tp {
		case WHERE, SELECT, PUT, REMOVE:
			break
		default:
			return nil, NewSyntaxError(p.tok.Pos, "Expect put, select or where keyword")
		}
	}
	var (
		selectStmt  *SelectStmt  = nil
		limitStmt   *LimitStmt   = nil
		orderStmt   *OrderStmt   = nil
		groupByStmt *GroupByStmt = nil
		err         error
		wherePos    int
	)

	if p.tok.Tp == PUT {
		return p.parsePut()
	} else if p.tok.Tp == REMOVE {
		return p.parseRemove()
	} else if p.tok.Tp == SELECT {
		selectStmt, err = p.parseSelect()
		if err != nil {
			return nil, err
		}
		if p.tok != nil {
			wherePos = p.tok.Pos
		} else {
			return nil, NewSyntaxError(-1, "Expect where keyword")
		}
		p.next()
	} else {
		if p.tok.Tp != WHERE {
			return nil, NewSyntaxError(p.tok.Pos, "Expect where keyword")
		}
		if p.tok != nil {
			wherePos = p.tok.Pos
		}
		p.next()
	}

	if p.tok == nil {
		return nil, NewSyntaxError(-1, "Expect where statement")
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if selectStmt == nil {
		selectStmt = &SelectStmt{
			Fields:     nil,
			FieldNames: nil,
			FieldTypes: nil,
			AllFields:  true,
		}
	}

	checkCtx := &CheckCtx{
		Fields:     selectStmt.Fields,
		FieldNames: selectStmt.FieldNames,
		FieldTypes: selectStmt.FieldTypes,
	}

	for p.tok != nil {
		switch p.tok.Tp {
		case ORDER:
			if orderStmt != nil {
				return nil, NewSyntaxError(p.tok.Pos, "Duplicate order by expression")
			}
			orderStmt, err = p.parseOrderBy(selectStmt)
			if err != nil {
				return nil, err
			}
			if len(orderStmt.Orders) == 0 {
				return nil, NewSyntaxError(orderStmt.Pos, "Require order by fields")
			}
		case GROUP:
			if groupByStmt != nil {
				return nil, NewSyntaxError(p.tok.Pos, "Duplicate group by expression")
			}
			groupByStmt, err = p.parseGroupBy(selectStmt, checkCtx)
			if err != nil {
				return nil, err
			}
			if len(groupByStmt.Fields) == 0 {
				return nil, NewSyntaxError(groupByStmt.Pos, "Require group by fields")
			}
		case LIMIT:
			if limitStmt != nil {
				return nil, NewSyntaxError(p.tok.Pos, "Duplicate limit expression")
			}
			limitStmt, err = p.parseLimit()
			if err != nil {
				return nil, err
			}
			if p.tok != nil {
				return nil, NewSyntaxError(p.tok.Pos, "Has more expression in limit expression")
			}
		default:
			return nil, NewSyntaxError(p.tok.Pos, "Missing operator")
		}
	}

	// Check syntax
	err = expr.Check(checkCtx)
	if err != nil {
		return nil, err
	}
	if expr.ReturnType() != TBOOL {
		return nil, NewSyntaxError(expr.GetPos(), "where statement result type should be boolean")
	}
	whereStmt := &WhereStmt{
		Pos:  wherePos,
		Expr: expr,
	}
	selectStmt.Where = whereStmt
	selectStmt.Limit = limitStmt
	selectStmt.Order = orderStmt
	selectStmt.GroupBy = groupByStmt
	err = selectStmt.ValidateFields(checkCtx)
	return selectStmt, err
}