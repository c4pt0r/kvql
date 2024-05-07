package kvql

import (
	"fmt"
	"strings"
)

type RemovePlan struct {
	Storage  Storage
	Keys     []Expression
	executed bool
}

func (p *RemovePlan) Init() error {
	p.executed = false
	return nil
}

func (p *RemovePlan) Explain() []string {
	return []string{p.String()}
}

func (p *RemovePlan) String() string {
	keys := make([]string, len(p.Keys))
	for i, k := range p.Keys {
		keys[i] = k.String()
	}
	return fmt.Sprintf("RemovePlan{Keys = [%s]}", strings.Join(keys, ", "))
}

func (p *RemovePlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		return []Column{n}, err
	}
	return nil, nil
}

func (p *RemovePlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
		p.executed = true
		row := []Column{n}
		return [][]Column{row}, err
	}
	return nil, nil
}

func (p *RemovePlan) FieldNameList() []string {
	return []string{"Rows"}
}

func (p *RemovePlan) FieldTypeList() []Type {
	return []Type{TNUMBER}
}

func (p *RemovePlan) processKey(ekvp KVPair, ctx *ExecuteCtx, kexpr Expression) ([]byte, error) {
	rkey, err := kexpr.Execute(ekvp, ctx)
	if err != nil {
		return nil, err
	}
	key := []byte(toString(rkey))
	return key, nil
}

func (p *RemovePlan) execute(ctx *ExecuteCtx) (int, error) {
	nks := len(p.Keys)
	keys := make([][]byte, nks)
	ekvp := NewKVPStr("", "")
	for i, kexpr := range p.Keys {
		key, err := p.processKey(ekvp, ctx, kexpr)
		if err != nil {
			return 0, err
		}
		keys[i] = key
	}

	if nks == 0 {
		return 0, nil
	} else if nks == 1 {
		err := p.Storage.Delete(keys[0])
		if err != nil {
			return 0, err
		}
		return 1, nil
	} else {
		err := p.Storage.BatchDelete(keys)
		if err != nil {
			return 0, err
		}
		return nks, nil
	}
}
