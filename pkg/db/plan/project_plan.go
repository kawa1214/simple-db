package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

type ProjectPlan struct {
	p      Plan
	schema *record.Schema
}

func NewProjectPlan(p Plan, fieldlist []string) *ProjectPlan {
	s := record.NewSchema()
	for _, fldname := range fieldlist {
		s.Add(fldname, p.Schema())
	}
	return &ProjectPlan{
		p:      p,
		schema: s,
	}
}

func (pp *ProjectPlan) Open() query.Scan {
	s := pp.p.Open()
	return query.NewProjectScan(s, pp.schema.Fields)
}

func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.p.BlocksAccessed()
}

func (pp *ProjectPlan) RecordsOutput() int {
	return pp.p.RecordsOutput()
}

func (pp *ProjectPlan) DistinctValues(fldname string) int {
	return pp.p.DistinctValues(fldname)
}

func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
