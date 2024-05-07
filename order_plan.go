package kvql

import (
	"bytes"
	"container/heap"
	"fmt"
	"strconv"
	"strings"
)

type FinalOrderPlan struct {
	Storage    Storage
	Orders     []OrderField
	FieldNames []string
	FieldTypes []Type
	ChildPlan  FinalPlan
	pos        int
	total      int
	sorted     *orderColumnsRowHeap
	orderPos   []int
	orderTypes []Type
}

func (p *FinalOrderPlan) findOrderIdx(o OrderField) (int, error) {
	fname := o.Name
	for i, fn := range p.FieldNames {
		if fname == fn {
			return i, nil
		}
	}
	return 0, NewSyntaxError(o.Field.GetPos(), "Cannot find field: %s", fname)
}

func (p *FinalOrderPlan) Init() error {
	p.pos = 0
	p.total = 0
	p.orderPos = []int{}
	p.orderTypes = []Type{}
	for _, o := range p.Orders {
		idx, err := p.findOrderIdx(o)
		if err != nil {
			return err
		}
		p.orderPos = append(p.orderPos, idx)
		p.orderTypes = append(p.orderTypes, p.FieldTypes[idx])
	}
	p.sorted = &orderColumnsRowHeap{}
	heap.Init(p.sorted)
	return p.ChildPlan.Init()
}

func (p *FinalOrderPlan) FieldNameList() []string {
	return p.FieldNames
}

func (p *FinalOrderPlan) FieldTypeList() []Type {
	return p.FieldTypes
}

func (p *FinalOrderPlan) String() string {
	fields := []string{}
	for _, f := range p.Orders {
		orderStr := " ASC"
		if f.Order == DESC {
			orderStr = " DESC"
		}
		fields = append(fields, f.Name+orderStr)
	}
	return fmt.Sprintf("OrderPlan{Fields = <%s>}", strings.Join(fields, ", "))
}

func (p *FinalOrderPlan) Explain() []string {
	ret := []string{p.String()}
	for _, plan := range p.ChildPlan.Explain() {
		ret = append(ret, plan)
	}
	return ret
}

func (p *FinalOrderPlan) Next(ctx *ExecuteCtx) ([]Column, error) {
	if p.total == 0 {
		if err := p.prepare(ctx); err != nil {
			return nil, err
		}
	}
	if p.pos < p.total {
		rrow := heap.Pop(p.sorted)
		row := rrow.(*orderColumnsRow)
		p.pos++
		return row.cols, nil
	}
	return nil, nil
}

func (p *FinalOrderPlan) Batch(ctx *ExecuteCtx) ([][]Column, error) {
	if p.total == 0 {
		if err := p.prepareBatch(ctx); err != nil {
			return nil, err
		}
	}
	var (
		ret   = make([][]Column, 0, PlanBatchSize)
		count = 0
	)
	for p.pos < p.total {
		rrow := heap.Pop(p.sorted)
		row := rrow.(*orderColumnsRow)
		ret = append(ret, row.cols)
		p.pos++
		count++
		if count >= PlanBatchSize {
			break
		}
	}
	return ret, nil
}

func (p *FinalOrderPlan) prepare(ctx *ExecuteCtx) error {
	for {
		col, err := p.ChildPlan.Next(ctx)
		if err != nil {
			return err
		}
		if col == nil && err == nil {
			break
		}
		row := &orderColumnsRow{
			cols:       col,
			orders:     p.Orders,
			orderPos:   p.orderPos,
			orderTypes: p.orderTypes,
		}
		heap.Push(p.sorted, row)
		p.total++
	}
	return nil
}

func (p *FinalOrderPlan) prepareBatch(ctx *ExecuteCtx) error {
	for {
		rows, err := p.ChildPlan.Batch(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, cols := range rows {
			row := &orderColumnsRow{
				cols:       cols,
				orders:     p.Orders,
				orderPos:   p.orderPos,
				orderTypes: p.orderTypes,
			}
			heap.Push(p.sorted, row)
			p.total++
		}
	}
	return nil
}

type orderColumnsRow struct {
	cols       []Column
	orders     []OrderField
	orderPos   []int
	orderTypes []Type
}

func (l *orderColumnsRow) Less(r *orderColumnsRow) bool {
	for i, o := range l.orders {
		oidx := l.orderPos[i]
		desc := o.Order == DESC
		lval := l.cols[oidx]
		rval := r.cols[oidx]
		compare := l.compare(l.orderTypes[i], lval, rval, desc)
		if compare < 0 {
			return true
		} else if compare > 0 {
			return false
		}
	}
	return false
}

func (l *orderColumnsRow) compare(tp Type, lval, rval Column, reverse bool) int {
	switch tp {
	case TSTR:
		return l.compareBytes(lval, rval, reverse)
	case TNUMBER:
		return l.compareNumber(lval, rval, reverse)
	case TBOOL:
		return l.compareBool(lval, rval, reverse)
	default:
		return 0
	}
}

func (l *orderColumnsRow) compareBytes(lval, rval Column, reverse bool) int {
	var (
		lbval []byte
		rbval []byte
	)
	switch lval.(type) {
	case []byte:
		lbval = lval.([]byte)
		rbval = rval.([]byte)
	case string:
		lbval = []byte(lval.(string))
		rbval = []byte(rval.(string))
	default:
		return 0
	}
	if reverse {
		return 0 - bytes.Compare(lbval, rbval)
	}
	return bytes.Compare(lbval, rbval)
}

func (l *orderColumnsRow) compareBool(lval, rval Column, reverse bool) int {
	var (
		lbool bool
		rbool bool
	)
	switch lval.(type) {
	case bool:
		lbool = lval.(bool)
		rbool = rval.(bool)
	case string:
		lbool = lval.(string) == "true"
		rbool = rval.(string) == "true"
	case []byte:
		lbool = bytes.Equal(lval.([]byte), []byte("true"))
		rbool = bytes.Equal(rval.([]byte), []byte("true"))
	default:
		return 0
	}
	lint := 0
	rint := 0
	if lbool {
		lint = 1
	}
	if rbool {
		rint = 1
	}
	if lint == rint {
		return 0
	}
	if reverse {
		if lint > rint {
			return -1
		} else {
			return 1
		}
	}
	if lint < rint {
		return -1
	} else {
		return 1
	}
}

func (l *orderColumnsRow) compareNumber(lval, rval Column, reverse bool) int {
	var (
		lint, rint     int64
		lfloat, rfloat float64
		err            error
		isFloat        bool = false
	)
	switch lval.(type) {
	case int:
		lint = int64(lval.(int))
		rint = int64(rval.(int))
	case int16:
		lint = int64(lval.(int16))
		rint = int64(rval.(int16))
	case int32:
		lint = int64(lval.(int32))
		rint = int64(rval.(int32))
	case int64:
		lint = lval.(int64)
		rint = rval.(int64)
	case uint:
		lint = int64(lval.(uint))
		rint = int64(rval.(uint))
	case uint16:
		lint = int64(lval.(uint16))
		rint = int64(rval.(uint16))
	case uint32:
		lint = int64(lval.(uint32))
		rint = int64(rval.(uint32))
	case uint64:
		lint = int64(lval.(uint64))
		rint = int64(rval.(uint64))
	case float32:
		lfloat = float64(lval.(float32))
		rfloat = float64(rval.(float32))
		isFloat = true
	case float64:
		lfloat = lval.(float64)
		rfloat = rval.(float64)
		isFloat = true
	case []byte:
		if lint, err = strconv.ParseInt(string(lval.([]byte)), 10, 64); err == nil {
			if rint, err = strconv.ParseInt(string(rval.([]byte)), 10, 64); err == nil {
				return l.compareInt(lint, rint, reverse)
			}
		}
		if lfloat, err = strconv.ParseFloat(string(lval.([]byte)), 64); err == nil {
			if rfloat, err = strconv.ParseFloat(string(rval.([]byte)), 64); err == nil {
				return l.compareFloat(lfloat, rfloat, reverse)
			}
		}
		return 0
	case string:
		if lint, err = strconv.ParseInt(lval.(string), 10, 64); err == nil {
			if rint, err = strconv.ParseInt(rval.(string), 10, 64); err == nil {
				return l.compareInt(lint, rint, reverse)
			}
		}
		if lfloat, err = strconv.ParseFloat(lval.(string), 64); err == nil {
			if rfloat, err = strconv.ParseFloat(rval.(string), 64); err == nil {
				return l.compareFloat(lfloat, rfloat, reverse)
			}
		}
		return 0
	}

	if isFloat {
		return l.compareFloat(lfloat, rfloat, reverse)
	}
	return l.compareInt(lint, rint, reverse)
}

func (l *orderColumnsRow) compareInt(lval, rval int64, reverse bool) int {
	if lval == rval {
		return 0
	}
	if reverse {
		if lval > rval {
			return -1
		} else {
			return 1
		}
	}
	if lval < rval {
		return -1
	} else {
		return 1
	}
}

func (l *orderColumnsRow) compareFloat(lval, rval float64, reverse bool) int {
	if lval == rval {
		return 0
	}
	if reverse {
		if lval > rval {
			return -1
		} else {
			return 1
		}
	}
	if lval < rval {
		return -1
	} else {
		return 1
	}
}

type orderColumnsRowHeap []*orderColumnsRow

func (h orderColumnsRowHeap) Len() int {
	return len(h)
}

func (h orderColumnsRowHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *orderColumnsRowHeap) Push(x any) {
	*h = append(*h, x.(*orderColumnsRow))
}

func (h *orderColumnsRowHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h orderColumnsRowHeap) Less(i, j int) bool {
	l := h[i]
	r := h[j]
	return l.Less(r)
}
