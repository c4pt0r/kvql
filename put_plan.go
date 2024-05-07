package kvql

import (
	"fmt"
	"strings"
)

type PutPlan struct {
	Storage  Storage
	KVPairs  []*PutKVPair
	executed bool
}

func (p *PutPlan) Init() error {
	p.executed = false
	return nil
}

func (p *PutPlan) Explain() []string {
	return []string{p.String()}
}

func (p *PutPlan) String() string {
	kvps := make([]string, len(p.KVPairs))
	for i, kvp := range p.KVPairs {
		kvps[i] = kvp.String()
	}
	return fmt.Sprintf("PutPlan{KVPairs = [%s]}", strings.Join(kvps, ", "))
}

func (p *PutPlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		return []Column{n}, err
	}
	return nil, nil
}

func (p *PutPlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		row := []Column{n}
		return [][]Column{row}, err
	}
	return nil, nil
}

func (p *PutPlan) FieldNameList() []string {
	return []string{"Rows"}
}

func (p *PutPlan) FieldTypeList() []Type {
	return []Type{TNUMBER}
}

func (p *PutPlan) processKVPair(ctx *ExecuteCtx, kvp *PutKVPair) ([]byte, []byte, error) {
	ekvp := NewKVPStr("", "")
	rkey, err := kvp.Key.Execute(ekvp, ctx)
	if err != nil {
		return nil, nil, err
	}
	key := []byte(toString(rkey))
	ekvp.Key = key
	rvalue, err := kvp.Value.Execute(ekvp, ctx)
	if err != nil {
		return nil, nil, err
	}
	value := []byte(toString(rvalue))
	return key, value, nil
}

func (p *PutPlan) execute(ctx *ExecuteCtx) (int, error) {
	nkvps := len(p.KVPairs)
	kvps := make([]KVPair, nkvps)
	for i, kvp := range p.KVPairs {
		key, value, err := p.processKVPair(ctx, kvp)
		if err != nil {
			return 0, err
		}
		kvps[i] = NewKVP(key, value)
	}

	if nkvps == 0 {
		return 0, nil
	} else if nkvps == 1 {
		err := p.Storage.Put(kvps[0].Key, kvps[0].Value)
		if err != nil {
			return 0, err
		}
		return 1, nil
	} else {
		err := p.Storage.BatchPut(kvps)
		if err != nil {
			return 0, err
		}
		return nkvps, nil
	}
}
