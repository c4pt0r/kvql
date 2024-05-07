package kvql

import "fmt"

type FinalLimitPlan struct {
	Storage    Storage
	Start      int
	Count      int
	current    int
	skips      int
	ChildPlan  FinalPlan
	FieldNames []string
	FieldTypes []Type
}

func (p *FinalLimitPlan) Init() error {
	p.current = 0
	p.skips = 0
	return p.ChildPlan.Init()
}

func (p *FinalLimitPlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	for p.skips < p.Start {
		cols, err := p.ChildPlan.Next(ctx)
		if err != nil {
			return nil, err
		}
		if cols == nil && err == nil {
			return nil, nil
		}
		p.skips++
	}
	if p.current >= p.Count {
		return nil, nil
	}
	cols, err := p.ChildPlan.Next(ctx)
	if err != nil {
		return nil, err
	}
	if cols == nil && err == nil {
		return nil, nil
	}

	p.current++
	return cols, nil

}

func (p *FinalLimitPlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	var (
		rows   [][]Column
		err    error
		finish = false
		count  = 0
		ret    = make([][]Column, 0, PlanBatchSize)
	)
	for p.skips < p.Start {
		restSkips := p.Start - p.skips
		rows, err = p.ChildPlan.Batch(ctx)
		if err != nil {
			return nil, err
		}
		nrows := len(rows)
		if nrows == 0 {
			return nil, nil
		}
		if nrows <= restSkips {
			p.skips += nrows
		} else {
			p.skips += restSkips
			rows = rows[restSkips:]
			// Skip finish break is OK
			break
		}
	}
	if len(rows) > 0 {
		for _, row := range rows {
			if p.current >= p.Count {
				break
			}
			ret = append(ret, row)
			count++
			p.current++
		}
	}
	if p.current >= p.Count {
		return ret, nil
	}
	for !finish {
		rows, err = p.ChildPlan.Batch(ctx)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			finish = true
			break
		}
		for _, row := range rows {
			ret = append(ret, row)
			count++
			p.current++
			if p.current >= p.Count {
				finish = true
				break
			}
		}
		if count >= PlanBatchSize {
			finish = true
			break
		}
	}
	return ret, nil
}

func (p *FinalLimitPlan) String() string {
	return fmt.Sprintf("LimitPlan{Start = %d, Count = %d}", p.Start, p.Count)
}

func (p *FinalLimitPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *FinalLimitPlan) FieldNameList() []string {
	return p.FieldNames
}

func (p *FinalLimitPlan) FieldTypeList() []Type {
	return p.FieldTypes
}

type LimitPlan struct {
	Storage   Storage
	Start     int
	Count     int
	current   int
	skips     int
	ChildPlan Plan
}

func (p *LimitPlan) Init() error {
	p.current = 0
	p.skips = 0
	return p.ChildPlan.Init()
}

func (p *LimitPlan) String() string {
	return fmt.Sprintf("LimitPlan{Start = %d, Count = %d}", p.Start, p.Count)
}

func (p *LimitPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *LimitPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	for p.skips < p.Start {
		key, value, err := p.ChildPlan.Next(ctx)
		if err != nil {
			return nil, nil, err
		}
		if key == nil && value == nil && err == nil {
			return nil, nil, nil
		}
		p.skips++
	}
	if p.current >= p.Count {
		return nil, nil, nil
	}
	k, v, err := p.ChildPlan.Next(ctx)
	if err != nil {
		return nil, nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil, nil
	}
	p.current++
	return k, v, nil
}

func (p *LimitPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	var (
		rows   []KVPair
		ret    = make([]KVPair, 0, PlanBatchSize)
		err    error
		finish = false
		count  = 0
	)
	for p.skips < p.Start {
		restSkips := p.Start - p.skips
		rows, err = p.ChildPlan.Batch(ctx)
		if err != nil {
			return nil, err
		}
		nrows := len(rows)
		if nrows == 0 {
			return nil, nil
		}
		if nrows <= restSkips {
			p.skips += nrows
		} else {
			p.skips += restSkips
			rows = rows[restSkips:]
			// Skip finish break it OK
			break
		}
	}
	if len(rows) > 0 {
		for _, row := range rows {
			if p.current >= p.Count {
				break
			}
			ret = append(ret, row)
			count++
			p.current++
		}
	}
	if p.current >= p.Count {
		return ret, nil
	}
	for !finish {
		rows, err = p.ChildPlan.Batch(ctx)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			finish = true
			break
		}
		for _, row := range rows {
			ret = append(ret, row)
			count++
			p.current++
			if p.current >= p.Count {
				finish = true
				break
			}
		}
		if count >= PlanBatchSize {
			finish = true
			break
		}
	}
	return ret, nil
}
