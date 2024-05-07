package kvql

import "fmt"

type Optimizer struct {
	Query  string
	stmt   Statement
	filter *FilterExec
}

func NewOptimizer(query string) *Optimizer {
	return &Optimizer{
		Query: query,
	}
}

func (o *Optimizer) init() error {
	p := NewParser(o.Query)
	stmt, err := p.Parse()
	if err != nil {
		return err
	}
	o.stmt = stmt
	switch vstmt := stmt.(type) {
	case *SelectStmt:
		o.optimizeSelectExpressions(vstmt)
		o.filter = &FilterExec{
			Ast: vstmt.Where,
		}
	case *DeleteStmt:
		o.optimizeDeleteExpressions(vstmt)
		o.filter = &FilterExec{
			Ast: vstmt.Where,
		}
	}
	return nil
}

func (o *Optimizer) optimizeDeleteExpressions(stmt *DeleteStmt) {
	eo := ExpressionOptimizer{
		Root: stmt.Where.Expr,
	}
	stmt.Where.Expr = eo.Optimize()
}

func (o *Optimizer) optimizeSelectExpressions(stmt *SelectStmt) {
	eo := ExpressionOptimizer{
		Root: stmt.Where.Expr,
	}
	stmt.Where.Expr = eo.Optimize()
	for i, field := range stmt.Fields {
		// fmt.Println("Before opt", field)
		eo.Root = field
		stmt.Fields[i] = eo.Optimize()
		// fmt.Println("After opt", o.stmt.Fields[i])
	}
}

func (o *Optimizer) findAggrFunc(expr Expression) bool {
	switch e := expr.(type) {
	case *BinaryOpExpr:
		if o.findAggrFunc(e.Left) {
			return true
		}
		if o.findAggrFunc(e.Right) {
			return true
		}
	case *FunctionCallExpr:
		return IsAggrFuncExpr(expr)
	}
	return false
}

func (o *Optimizer) buildFinalPlan(t Txn, fp Plan, stmt *SelectStmt) (FinalPlan, error) {
	hasAggr := false
	aggrFields := 0
	aggrAll := true
	for _, field := range stmt.Fields {
		if o.findAggrFunc(field) {
			hasAggr = true
			aggrFields++
		}
	}
	if stmt.GroupBy != nil && len(stmt.Fields) == len(stmt.GroupBy.Fields) {
		allInSelect := true
		for _, gf := range stmt.GroupBy.Fields {
			gfNameInSelect := false
			for _, fn := range stmt.FieldNames {
				if fn == gf.Name {
					gfNameInSelect = true
					break
				}
			}
			if !gfNameInSelect {
				allInSelect = false
				break
			}
		}
		hasAggr = allInSelect
	}
	var ffp FinalPlan
	if !hasAggr && stmt.GroupBy != nil && len(stmt.GroupBy.Fields) > 0 {
		return nil, NewSyntaxError(stmt.Pos, "No aggregate fields in select statement")
	}
	if !hasAggr {
		ffp = &ProjectionPlan{
			Txn:        t,
			ChildPlan:  fp,
			AllFields:  stmt.AllFields,
			FieldNames: stmt.FieldNames,
			FieldTypes: stmt.FieldTypes,
			Fields:     stmt.Fields,
		}

		// Build order
		if stmt.Order != nil {
			ffp = o.buildFinalOrderPlan(t, ffp, false, stmt)
		}

		// Build limit
		if stmt.Limit != nil {
			ffp = o.buildFinalLimitPlan(t, ffp, stmt)
		}

		return ffp, nil
	}

	// Update limit
	limit := -1
	start := 0
	doNotBuildLimit := false
	// no order by only has limit
	if stmt.Limit != nil && stmt.Order == nil {
		doNotBuildLimit = true
		start = stmt.Limit.Start
		limit = stmt.Limit.Count
	}
	var groupByFields []GroupByField = nil
	if stmt.GroupBy != nil {
		groupByFields = stmt.GroupBy.Fields
		aggrAll = false
	} else {
		aggrAll = true
	}

	if aggrFields == 0 && len(groupByFields) > 0 {
		return nil, NewSyntaxError(stmt.Pos, "No aggregate fields in select statement")
	}

	if aggrFields+len(groupByFields) < len(stmt.Fields) {
		if stmt.GroupBy != nil {
			return nil, NewSyntaxError(stmt.GroupBy.Pos, "Missing aggregate fields in group by statement")
		} else {
			return nil, NewSyntaxError(-1, "Missing group by statement")
		}
	}

	ffp = &AggregatePlan{
		Txn:           t,
		ChildPlan:     fp,
		AggrAll:       aggrAll,
		FieldNames:    stmt.FieldNames,
		FieldTypes:    stmt.FieldTypes,
		Fields:        stmt.Fields,
		GroupByFields: groupByFields,
		Limit:         limit,
		Start:         start,
	}

	if stmt.Order != nil {
		ffp = o.buildFinalOrderPlan(t, ffp, true, stmt)
	}

	if stmt.Limit != nil && !doNotBuildLimit {
		ffp = o.buildFinalLimitPlan(t, ffp, stmt)
	}
	return ffp, nil
}

func (o *Optimizer) buildPlan(t Txn) (FinalPlan, error) {
	err := o.init()
	if err != nil {
		return nil, err
	}
	switch stmt := o.stmt.(type) {
	case *SelectStmt:
		return o.buildSelectPlan(t, stmt)
	case *PutStmt:
		return o.buildPutPlan(t, stmt)
	case *RemoveStmt:
		return o.buildRemovePlan(t, stmt)
	case *DeleteStmt:
		return o.buildDeletePlan(t, stmt)
	default:
		return nil, fmt.Errorf("Cannot build query plan without a select statement")
	}
}

func (o *Optimizer) buildPutPlan(t Txn, stmt *PutStmt) (FinalPlan, error) {
	plan := &PutPlan{
		Txn:     t,
		KVPairs: stmt.KVPairs,
	}
	err := plan.Init()
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (o *Optimizer) buildRemovePlan(t Txn, stmt *RemoveStmt) (FinalPlan, error) {
	plan := &RemovePlan{
		Txn:  t,
		Keys: stmt.Keys,
	}
	err := plan.Init()
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (o *Optimizer) optimizeDeletePlanToRemovePlan(t Txn, mgPlan *MultiGetPlan) (FinalPlan, error) {
	keys := make([]Expression, len(mgPlan.Keys))
	for i, key := range mgPlan.Keys {
		kexpr := &StringExpr{
			Pos:  0,
			Data: key,
		}
		keys[i] = kexpr
	}

	removePlan := &RemovePlan{
		Txn:  t,
		Keys: keys,
	}
	err := removePlan.Init()
	return removePlan, err
}

func (o *Optimizer) canOptimizeDeletePlanToRemovePlan(mgPlan *MultiGetPlan) bool {
	if mgPlan.Filter.Ast == nil || mgPlan.Filter.Ast.Expr == nil {
		return false
	}
	fexpr := mgPlan.Filter.Ast.Expr
	hasAndOp := false
	fexpr.Walk(func(e Expression) bool {
		switch eval := e.(type) {
		case *BinaryOpExpr:
			if eval.Op == And || eval.Op == KWAnd {
				hasAndOp = true
				return false
			}
		}
		return true
	})
	// For safety, if filter expressions has `and` operator it should not optimize to remove plan
	if hasAndOp {
		return false
	}
	return true
}

func (o *Optimizer) buildDeletePlan(t Txn, stmt *DeleteStmt) (FinalPlan, error) {
	var err error
	// Build Scan
	fp := o.buildScanPlan(t)

	// Just build an empyt result plan so we can
	// ignore limit plan just return the delete plan
	// with empty result plan directly
	if _, ok := fp.(*EmptyResultPlan); ok {
		delPlan := &DeletePlan{
			Txn:       t,
			ChildPlan: fp,
		}
		err = delPlan.Init()
		if err != nil {
			return nil, err
		}
		return delPlan, nil
	}

	if mgPlan, ok := fp.(*MultiGetPlan); ok && stmt.Limit == nil {
		// Only multi get plan and no limit statement can be optimize to remove plan
		if o.canOptimizeDeletePlanToRemovePlan(mgPlan) {
			return o.optimizeDeletePlanToRemovePlan(t, mgPlan)
		}
	}

	delPlan := &DeletePlan{
		Txn:       t,
		ChildPlan: fp,
	}

	if stmt.Limit != nil {
		limitPlan := &LimitPlan{
			Txn:       t,
			Start:     stmt.Limit.Start,
			Count:     stmt.Limit.Count,
			ChildPlan: fp,
		}
		delPlan.ChildPlan = limitPlan
	}
	err = delPlan.Init()
	if err != nil {
		return nil, err
	}
	return delPlan, nil
}

func (o *Optimizer) buildSelectPlan(t Txn, stmt *SelectStmt) (FinalPlan, error) {
	// Build Scan
	fp := o.buildScanPlan(t)

	// Just build an empty result plan so we can
	// ignore order and limit plan just return
	// the projection plan with empty result plan
	if _, ok := fp.(*EmptyResultPlan); ok {
		ret, err := o.buildFinalPlan(t, fp, stmt)
		if err != nil {
			return nil, err
		}
		err = ret.Init()
		if err != nil {
			return nil, err
		}
		return ret, nil
	}

	ret, err := o.buildFinalPlan(t, fp, stmt)
	if err != nil {
		return nil, err
	}
	err = ret.Init()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Optimizer) BuildPlan(t Txn) (FinalPlan, error) {
	ret, err := o.buildPlan(t)
	if err != nil {
		return nil, err
	}
	err = ret.Init()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Optimizer) buildFinalLimitPlan(t Txn, ffp FinalPlan, stmt *SelectStmt) FinalPlan {
	return &FinalLimitPlan{
		Txn:        t,
		Start:      stmt.Limit.Start,
		Count:      stmt.Limit.Count,
		FieldNames: ffp.FieldNameList(),
		FieldTypes: ffp.FieldTypeList(),
		ChildPlan:  ffp,
	}
}

func (o *Optimizer) buildFinalOrderPlan(t Txn, ffp FinalPlan, hasAggr bool, stmt *SelectStmt) FinalPlan {
	if !hasAggr && len(stmt.Order.Orders) == 1 {
		order := stmt.Order.Orders[0]
		switch expr := order.Field.(type) {
		case *FieldExpr:
			// If order by key asc just ignore it
			if expr.Field == KeyKW && order.Order == ASC {
				return ffp
			}
		}
	}
	return &FinalOrderPlan{
		Txn:        t,
		Orders:     stmt.Order.Orders,
		FieldNames: ffp.FieldNameList(),
		FieldTypes: ffp.FieldTypeList(),
		ChildPlan:  ffp,
	}
}

func (o *Optimizer) buildScanPlan(t Txn) Plan {
	fopt := NewFilterOptimizer(o.filter.Ast, t, o.filter)
	return fopt.Optimize()
}
