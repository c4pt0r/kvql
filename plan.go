package kvql

import (
	"fmt"
	"os"
)

var (
	PlanBatchSize    = 32
	EnableFieldCache = true
)

func init() {
	if dfc := os.Getenv("DISABLE_FCACHE"); dfc == "1" {
		EnableFieldCache = false
	}
}

type ExecuteCtx struct {
	Hit                 int
	EnableCache         bool
	FieldCaches         map[string]any
	FieldChunkKeyCaches map[string][]any
	FieldChunkCaches    map[string][]any
}

func NewExecuteCtx() *ExecuteCtx {
	return &ExecuteCtx{
		Hit:                 0,
		EnableCache:         EnableFieldCache,
		FieldCaches:         make(map[string]any),
		FieldChunkKeyCaches: make(map[string][]any),
		FieldChunkCaches:    make(map[string][]any),
	}
}

func (c *ExecuteCtx) GetFieldResult(name string) (any, bool) {
	if !c.EnableCache {
		return nil, false
	}
	if val, have := c.FieldCaches[name]; have {
		return val, true
	}
	return nil, false
}

func (c *ExecuteCtx) SetFieldResult(name string, value any) {
	if !c.EnableCache {
		return
	}
	c.FieldCaches[name] = value
}

func (c *ExecuteCtx) GetChunkFieldResult(name string, key []byte) ([]any, bool) {
	if !c.EnableCache {
		return nil, false
	}
	ckey := fmt.Sprintf("%s-%s", name, string(key))
	if chunk, have := c.FieldChunkKeyCaches[ckey]; have {
		return chunk, true
	}
	return nil, false
}

func (c *ExecuteCtx) AppendChunkFieldResult(name string, chunk []any) {
	if !c.EnableCache {
		return
	}
	cdata, have := c.FieldChunkCaches[name]
	if have {
		cdata = append(cdata, chunk...)
		c.FieldChunkCaches[name] = cdata
	} else {
		cchunk := make([]any, len(chunk))
		copy(cchunk, chunk)
		c.FieldChunkCaches[name] = cchunk
	}
}

func (c *ExecuteCtx) SetChunkFieldResult(name string, key []byte, chunk []any) {
	if !c.EnableCache {
		return
	}
	ckey := fmt.Sprintf("%s-%s", name, string(key))
	if _, have := c.FieldChunkKeyCaches[ckey]; have {
		return
	}
	c.FieldChunkKeyCaches[ckey] = chunk
	c.AppendChunkFieldResult(name, chunk)
}

func (c *ExecuteCtx) GetChunkFieldFinalResult(name string) ([]any, bool) {
	if !c.EnableCache {
		return nil, false
	}
	val, have := c.FieldChunkCaches[name]
	return val, have
}

func (c *ExecuteCtx) UpdateHit() {
	c.Hit++
}

func (c *ExecuteCtx) Clear() {
	if !c.EnableCache {
		return
	}
	clear(c.FieldCaches)
	clear(c.FieldChunkCaches)
	clear(c.FieldChunkKeyCaches)
}

func (c *ExecuteCtx) AdjustChunkCache(chooseIdxes []int) {
	if !c.EnableCache {
		return
	}
	cidxes := make(map[int]struct{})
	for _, idx := range chooseIdxes {
		cidxes[idx] = struct{}{}
	}
	for k, v := range c.FieldChunkCaches {
		nv := make([]any, 0, len(chooseIdxes))
		for i, item := range v {
			if _, have := cidxes[i]; have {
				nv = append(nv, item)
			}
		}
		c.FieldChunkCaches[k] = nv
	}
}

type FinalPlan interface {
	String() string
	Explain() []string
	Init() error
	Next(ctx *ExecuteCtx) ([]Column, error)
	Batch(ctx *ExecuteCtx) ([][]Column, error)
	FieldNameList() []string
	FieldTypeList() []Type
}

type Plan interface {
	String() string
	Explain() []string
	Init() error
	Next(ctx *ExecuteCtx) (key []byte, value []byte, err error)
	Batch(ctx *ExecuteCtx) (rows []KVPair, err error)
}

var (
	_ Plan = (*FullScanPlan)(nil)
	_ Plan = (*EmptyResultPlan)(nil)
	_ Plan = (*RangeScanPlan)(nil)
	_ Plan = (*PrefixScanPlan)(nil)
	_ Plan = (*MultiGetPlan)(nil)

	_ FinalPlan = (*ProjectionPlan)(nil)
	_ FinalPlan = (*AggregatePlan)(nil)
	_ FinalPlan = (*FinalOrderPlan)(nil)
	_ FinalPlan = (*FinalLimitPlan)(nil)
	_ FinalPlan = (*PutPlan)(nil)
)

type Column any

type EmptyResultPlan struct {
	Txn Txn
}

func NewEmptyResultPlan(t Txn, f *FilterExec) Plan {
	return &EmptyResultPlan{
		Txn: t,
	}
}

func (p *EmptyResultPlan) Init() error {
	return nil
}

func (p *EmptyResultPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	return nil, nil, nil
}

func (p *EmptyResultPlan) String() string {
	return "EmptyResultPlan"
}

func (p *EmptyResultPlan) Explain() []string {
	return []string{p.String()}
}

func (p *EmptyResultPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	return nil, nil
}