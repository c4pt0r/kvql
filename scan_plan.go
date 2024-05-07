package kvql

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type FullScanPlan struct {
	Storage Storage
	Filter  *FilterExec
	iter    Cursor
}

func NewFullScanPlan(s Storage, f *FilterExec) Plan {
	return &FullScanPlan{
		Storage: s,
		Filter:  f,
	}
}

func (p *FullScanPlan) String() string {
	return fmt.Sprintf("FullScanPlan{Filter = '%s'}", p.Filter.Explain())
}

func (p *FullScanPlan) Explain() []string {
	return []string{p.String()}
}

func (p *FullScanPlan) Init() (err error) {
	p.iter, err = p.Storage.Cursor()
	if err != nil {
		return err
	}
	return p.iter.Seek([]byte{})
}

func (p *FullScanPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	for {
		key, val, err := p.iter.Next()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			break
		}
		ok, err := p.Filter.Filter(NewKVP(key, val), ctx)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *FullScanPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	var (
		ret         = make([]KVPair, 0, PlanBatchSize)
		filterBatch = make([]KVPair, 0, PlanBatchSize)
		count       = 0
		finish      = false
		chooseIdxes = make([]int, 0, 2*PlanBatchSize)
		bidx        = 0
	)
	for !finish {
		filterBatch = filterBatch[:0]
		for i := 0; i < PlanBatchSize; i++ {
			key, val, err := p.iter.Next()
			if err != nil {
				return nil, err
			}
			if key == nil {
				finish = true
				break
			}
			filterBatch = append(filterBatch, NewKVP(key, val))
		}
		if len(filterBatch) > 0 {
			matchs, err := p.Filter.FilterBatch(filterBatch, ctx)
			if err != nil {
				return nil, err
			}
			for i, m := range matchs {
				if m {
					ret = append(ret, filterBatch[i])
					chooseIdxes = append(chooseIdxes, bidx)
					count += 1
				}
				bidx += 1
			}
			if count >= PlanBatchSize {
				finish = true
			}
		}
	}
	ctx.AdjustChunkCache(chooseIdxes)
	return ret, nil
}

type PrefixScanPlan struct {
	Storage Storage
	Filter  *FilterExec
	Prefix  string
	iter    Cursor
}

func NewPrefixScanPlan(s Storage, f *FilterExec, p string) Plan {
	return &PrefixScanPlan{
		Storage: s,
		Filter:  f,
		Prefix:  p,
	}
}

func (p *PrefixScanPlan) Init() (err error) {
	p.iter, err = p.Storage.Cursor()
	if err != nil {
		return err
	}
	return p.iter.Seek([]byte(p.Prefix))
}

func (p *PrefixScanPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	pb := []byte(p.Prefix)
	for {
		key, val, err := p.iter.Next()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			break
		}

		// Key not have the prefix
		if !bytes.HasPrefix(key, pb) {
			break
		}

		// Filter with the expression
		ok, err := p.Filter.Filter(NewKVP(key, val), ctx)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *PrefixScanPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	var (
		ret         = make([]KVPair, 0, PlanBatchSize)
		filterBatch = make([]KVPair, 0, PlanBatchSize)
		count       = 0
		finish      = false
		pb          = []byte(p.Prefix)
		chooseIdxes = make([]int, 0, 2*PlanBatchSize)
		bidx        = 0
	)
	for !finish {
		filterBatch = filterBatch[:0]
		for i := 0; i < PlanBatchSize; i++ {
			key, val, err := p.iter.Next()
			if err != nil {
				return nil, err
			}
			if key == nil {
				finish = true
				break
			}
			// Key not have the prefix
			if !bytes.HasPrefix(key, pb) {
				finish = true
				break
			}
			filterBatch = append(filterBatch, NewKVP(key, val))
		}
		if len(filterBatch) > 0 {
			matchs, err := p.Filter.FilterBatch(filterBatch, ctx)
			if err != nil {
				return nil, err
			}
			for i, m := range matchs {
				if m {
					ret = append(ret, filterBatch[i])
					chooseIdxes = append(chooseIdxes, bidx)
					count += 1
				}
				bidx += 1
			}
			if count >= PlanBatchSize {
				finish = true
			}
		}
	}
	ctx.AdjustChunkCache(chooseIdxes)
	return ret, nil
}

func (p *PrefixScanPlan) String() string {
	return fmt.Sprintf("PrefixScanPlan{Prefix = '%s', Filter = '%s'}", p.Prefix, p.Filter.Explain())
}

func (p *PrefixScanPlan) Explain() []string {
	return []string{p.String()}
}

type RangeScanPlan struct {
	Storage Storage
	Filter  *FilterExec
	Start   []byte
	End     []byte
	iter    Cursor
}

func NewRangeScanPlan(s Storage, f *FilterExec, start []byte, end []byte) Plan {
	return &RangeScanPlan{
		Storage: s,
		Filter:  f,
		Start:   start,
		End:     end,
	}
}

func (p *RangeScanPlan) Init() (err error) {
	p.iter, err = p.Storage.Cursor()
	if err != nil {
		return err
	}
	if p.Start != nil {
		err = p.iter.Seek(p.Start)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *RangeScanPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	for {
		key, val, err := p.iter.Next()
		if err != nil {
			return nil, nil, err
		}
		if key == nil {
			break
		}

		// Key is greater than End
		if p.End != nil && bytes.Compare(key, p.End) > 0 {
			break
		}

		// Filter with the expression
		ok, err := p.Filter.Filter(NewKVP(key, val), ctx)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *RangeScanPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	var (
		ret         = make([]KVPair, 0, PlanBatchSize)
		filterBatch = make([]KVPair, 0, PlanBatchSize)
		count       = 0
		finish      = false
		chooseIdxes = make([]int, 0, 2*PlanBatchSize)
		bidx        = 0
	)
	for !finish {
		filterBatch = filterBatch[:0]
		for i := 0; i < PlanBatchSize; i++ {
			key, val, err := p.iter.Next()
			if err != nil {
				return nil, err
			}
			if key == nil {
				finish = true
				break
			}
			// Key is greater than End
			if p.End != nil && bytes.Compare(key, p.End) > 0 {
				finish = true
				break
			}
			filterBatch = append(filterBatch, NewKVP(key, val))
		}

		if len(filterBatch) > 0 {
			matchs, err := p.Filter.FilterBatch(filterBatch, ctx)
			if err != nil {
				return nil, err
			}
			for i, m := range matchs {
				if m {
					ret = append(ret, filterBatch[i])
					chooseIdxes = append(chooseIdxes, bidx)
					count += 1
				}
				bidx += 1
			}
			if count >= PlanBatchSize {
				finish = true
			}
		}
	}
	ctx.AdjustChunkCache(chooseIdxes)
	return ret, nil
}

func convertByteToString(val []byte) string {
	if val == nil {
		return "<nil>"
	}
	return string(val)
}

func (p *RangeScanPlan) String() string {
	return fmt.Sprintf("RangeScanPlan{Start = '%s', End = '%s', Filter = '%s'}", convertByteToString(p.Start), convertByteToString(p.End), p.Filter.Explain())
}

func (p *RangeScanPlan) Explain() []string {
	return []string{p.String()}
}

type MultiGetPlan struct {
	Storage Storage
	Filter  *FilterExec
	Keys    []string
	numKeys int
	idx     int
}

func NewMultiGetPlan(s Storage, f *FilterExec, keys []string) Plan {
	// We should sort keys to ensure order by erase works correctly
	sort.Strings(keys)
	return &MultiGetPlan{
		Storage: s,
		Filter:  f,
		Keys:    keys,
		idx:     0,
		numKeys: len(keys),
	}
}

func (p *MultiGetPlan) Init() error {
	return nil
}

func (p *MultiGetPlan) Next(ctx *ExecuteCtx) ([]byte, []byte, error) {
	for {
		if p.idx >= p.numKeys {
			break
		}
		key := []byte(p.Keys[p.idx])
		p.idx++
		val, err := p.Storage.Get(key)
		if err != nil {
			return nil, nil, err
		}
		if val == nil {
			// No Value
			continue
		}
		ok, err := p.Filter.Filter(NewKVP(key, val), ctx)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return key, val, nil
		}
	}
	return nil, nil, nil
}

func (p *MultiGetPlan) Batch(ctx *ExecuteCtx) ([]KVPair, error) {
	var (
		ret         = make([]KVPair, 0, PlanBatchSize)
		filterBatch = make([]KVPair, 0, PlanBatchSize)
		count       = 0
		finish      = false
		chooseIdxes = make([]int, 0, 2*PlanBatchSize)
		bidx        = 0
	)
	for !finish {
		filterBatch = filterBatch[:0]
		for i := 0; i < PlanBatchSize; i++ {
			if p.idx >= p.numKeys {
				finish = true
				break
			}
			key := []byte(p.Keys[p.idx])
			p.idx++
			val, err := p.Storage.Get(key)
			if err != nil {
				return nil, err
			}
			if val == nil {
				// No Value
				continue
			}
			filterBatch = append(filterBatch, NewKVP(key, val))
		}
		if len(filterBatch) > 0 {
			matchs, err := p.Filter.FilterBatch(filterBatch, ctx)
			if err != nil {
				return nil, err
			}
			for i, m := range matchs {
				if m {
					ret = append(ret, filterBatch[i])
					chooseIdxes = append(chooseIdxes, bidx)
					count += 1
				}
			}
		}
		if count >= PlanBatchSize {
			finish = true
		}
	}
	ctx.AdjustChunkCache(chooseIdxes)
	return ret, nil
}

func (p *MultiGetPlan) String() string {
	keys := strings.Join(p.Keys, ", ")
	return fmt.Sprintf("MultiGetPlan{Keys = <%s>, Filter = '%s'}", keys, p.Filter.Explain())
}

func (p *MultiGetPlan) Explain() []string {
	return []string{p.String()}
}
