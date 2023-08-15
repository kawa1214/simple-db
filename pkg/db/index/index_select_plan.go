package index

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type IndexSelectPlan struct {
	p   plan.Plan
	ii  *metadata.IndexInfo
	val *record.Constant
}

func NewIndexSelectPlan(p plan.Plan, ii *metadata.IndexInfo, val *record.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		p:   p,
		ii:  ii,
		val: val,
	}
}

func (isp *IndexSelectPlan) Open() query.Scan {
	ts, ok := isp.p.Open().(*record.TableScan)
	if !ok {
		panic("p is not a tableplan")
	}
	idx := Open(isp.ii)
	return NewIndexSelectScan(ts, idx, isp.val)
}

func (isp *IndexSelectPlan) BlocksAccessed() int {
	return BlocksAccessed(isp.ii) + isp.RecordsOutput()
}

func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.ii.RecordsOutput()
}

func (isp *IndexSelectPlan) DistinctValues(fldname string) int {
	return isp.ii.DistinctValues(fldname)
}

func (isp *IndexSelectPlan) Schema() *record.Schema {
	return isp.p.Schema()
}
