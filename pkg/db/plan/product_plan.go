package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type ProductPlan struct {
	p1, p2 Plan
	schema *record.Schema
}

func NewProductPlan(p1, p2 Plan) *ProductPlan {
	s := record.NewSchema()
	s.AddAll(p1.Schema())
	s.AddAll(p2.Schema())
	return &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: s,
	}
}

func (pp *ProductPlan) Open() query.Scan {
	s1 := pp.p1.Open()

	s2 := pp.p2.Open()

	return query.NewProductScan(s1, s2)
}

func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

func (pp *ProductPlan) DistinctValues(fldname string) int {
	if pp.p1.Schema().HasField(fldname) {
		return pp.p1.DistinctValues(fldname)
	}
	return pp.p2.DistinctValues(fldname)
}

func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
