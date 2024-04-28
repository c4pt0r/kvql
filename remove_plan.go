package kvql

import (
	"fmt"
	"strings"
)

type RemovePlan struct {
	Txn      Txn
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
		return []Column{n}, err
	}
	return nil, nil
}

func (p *RemovePlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	if !p.executed {
		n, err := p.execute(ctx)
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
	count := 0
	keys := make([][]byte, len(p.Keys))
	ekvp := NewKVPStr("", "")
	for i, kexpr := range p.Keys {
		key, err := p.processKey(ekvp, ctx, kexpr)
		if err != nil {
			return count, err
		}
		keys[i] = key
	}

	for _, key := range keys {
		err := p.Txn.Delete(key)
		if err != nil {
			return count, err
		}
		count += 1
	}
	p.executed = true
	return count, nil
}