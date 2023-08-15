package index

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type IndexJoinPlan struct {
	p1, p2    plan.Plan
	ii        *metadata.IndexInfo
	joinfield string
	sch       *record.Schema
}

func NewIndexJoinPlan(p1, p2 plan.Plan, ii *metadata.IndexInfo, joinfield string) *IndexJoinPlan {
	ijp := &IndexJoinPlan{
		p1:        p1,
		p2:        p2,
		ii:        ii,
		joinfield: joinfield,
		sch:       record.NewSchema(),
	}
	ijp.sch.AddAll(p1.Schema())
	ijp.sch.AddAll(p2.Schema())
	return ijp
}

func (ijp *IndexJoinPlan) Open() query.Scan {
	s := ijp.p1.Open()
	ts, ok := ijp.p2.Open().(*record.TableScan)
	if !ok {
		panic("p2 is not a tableplan")
	}
	idx := NewHashIndexFromMetadata(ijp.ii)
	return NewIndexJoinScan(s, idx, ijp.joinfield, ts)
}

func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.p1.BlocksAccessed() +
		(ijp.p1.RecordsOutput() * BlocksAccessed(ijp.ii)) +
		ijp.RecordsOutput()
}

func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.p1.RecordsOutput() * ijp.ii.RecordsOutput()
}

func (ijp *IndexJoinPlan) DistinctValues(fldname string) int {
	if ijp.p1.Schema().HasField(fldname) {
		return ijp.p1.DistinctValues(fldname)
	}
	return ijp.p2.DistinctValues(fldname)
}

func (ijp *IndexJoinPlan) Schema() *record.Schema {
	return ijp.sch
}
