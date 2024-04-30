package kvql

import "fmt"

type DeletePlan struct {
	Txn       Txn
	ChildPlan Plan
	executed  bool
}

func (p *DeletePlan) Init() error {
	p.executed = false
	return p.ChildPlan.Init()
}

func (p *DeletePlan) String() string {
	return fmt.Sprintf("DeletePlan{}")
}

func (p *DeletePlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *DeletePlan) FieldNameList() []string {
	return []string{"Rows"}
}

func (p *DeletePlan) FieldTypeList() []Type {
	return []Type{TNUMBER}
}

func (p *DeletePlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		return []Column{n}, err
	}
	return nil, nil
}

func (p *DeletePlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		row := []Column{n}
		return [][]Column{row}, err
	}
	return nil, nil
}

func (p *DeletePlan) execute(ctx *ExecuteCtx) (int, error) {
	count := 0
	for {
		rows, err := p.ChildPlan.Batch(ctx)
		if err != nil {
			return count, err
		}
		nrows := len(rows)
		if nrows == 0 {
			return count, nil
		}
		keys := make([][]byte, nrows)
		for i, kv := range rows {
			keys[i] = kv.Key
		}
		err = p.Txn.BatchDelete(keys)
		if err != nil {
			return count, err
		}
		count += nrows
	}
}
