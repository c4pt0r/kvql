package kvql

import (
	"fmt"
	"strings"
)

type ProjectionPlan struct {
	Txn        Txn
	ChildPlan  Plan
	AllFields  bool
	FieldNames []string
	FieldTypes []Type
	Fields     []Expression
}

func (p *ProjectionPlan) Init() error {
	return p.ChildPlan.Init()
}

func (p *ProjectionPlan) FieldNameList() []string {
	if p.AllFields {
		return []string{"KEY", "VALUE"}
	}
	return p.FieldNames
}

func (p *ProjectionPlan) FieldTypeList() []Type {
	if p.AllFields {
		return []Type{TSTR, TSTR}
	}
	return p.FieldTypes
}

func (p *ProjectionPlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	ctx.Clear()
	k, v, err := p.ChildPlan.Next(ctx)
	if err != nil {
		return nil, err
	}
	if k == nil && v == nil && err == nil {
		return nil, nil
	}
	if p.AllFields {
		return []Column{k, v}, nil
	}
	return p.processProjection(NewKVP(k, v), ctx)
}

func (p *ProjectionPlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	ctx.Clear()
	kvps, err := p.ChildPlan.Batch(ctx)
	if err != nil {
		return nil, err
	}
	if len(kvps) == 0 {
		return nil, nil
	}
	if p.AllFields {
		ret := make([][]Column, 0, len(kvps))
		for _, kvp := range kvps {
			ret = append(ret, []Column{kvp.Key, kvp.Value})
		}
		return ret, nil
	}
	return p.processProjectionBatch(kvps, ctx)
}

func (p *ProjectionPlan) processProjectionBatch(chunk []KVPair, ctx *ExecuteCtx) ([][]Column, error) {
	var (
		nFields = len(p.Fields)
		ret     = make([][]Column, len(chunk))
		cols    = make([][]any, nFields)
		err     error
		have    bool
	)
	for i := 0; i < nFields; i++ {
		have = false
		if ctx != nil {
			fname := p.FieldNames[i]
			cols[i], have = ctx.GetChunkFieldFinalResult(fname)
		}
		if !have {
			cols[i], err = p.Fields[i].ExecuteBatch(chunk, ctx)
		} else {
			ctx.UpdateHit()
		}
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(chunk); i++ {
		row := make([]Column, nFields)
		for j := 0; j < nFields; j++ {
			row[j] = cols[j][i]
		}
		ret[i] = row
	}
	return ret, nil
}

func (p *ProjectionPlan) processProjection(kvp KVPair, ctx *ExecuteCtx) ([]Column, error) {
	nFields := len(p.Fields)
	ret := make([]Column, nFields)
	var (
		result any
		err    error
	)
	for i := 0; i < nFields; i++ {
		have := false
		if ctx != nil {
			fname := p.FieldNames[i]
			result, have = ctx.GetFieldResult(fname)
		}
		if !have {
			result, err = p.Fields[i].Execute(kvp, ctx)
		} else {
			ctx.UpdateHit()
		}
		if err != nil {
			return nil, err
		}
		switch value := result.(type) {
		case bool, []byte, string,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64,
			JSON, map[string]any, []any:
			ret[i] = value
		default:
			if value == nil {
				ret[i] = nil
				break
			}
			return nil, NewExecuteError(p.Fields[i].GetPos(), "Expression result type not support")
		}
	}
	return ret, nil
}

func (p *ProjectionPlan) String() string {
	fields := []string{}
	if p.AllFields {
		fields = append(fields, "*")
	} else {
		for _, f := range p.Fields {
			fields = append(fields, f.String())
		}
	}
	return fmt.Sprintf("ProjectionPlan{Fields = <%s>}", strings.Join(fields, ", "))
}

func (p *ProjectionPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}
