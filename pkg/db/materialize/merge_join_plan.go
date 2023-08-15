package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// MergeJoinPlan is the plan class for the mergejoin operator.
type MergeJoinPlan struct {
	p1, p2   plan.Plan
	fldname1 string
	fldname2 string
	sch      *record.Schema
}

// NewMergeJoinPlan creates a mergejoin plan for the two specified queries.
// The RHS must be materialized after it is sorted, to deal with possible duplicates.
func NewMergeJoinPlan(tx *tx.Transaction, p1, p2 plan.Plan, fldname1, fldname2 string) *MergeJoinPlan {
	sortlist1 := []string{fldname1}
	p1 = NewSortPlan(tx, p1, sortlist1)

	sortlist2 := []string{fldname2}
	p2 = NewSortPlan(tx, p2, sortlist2)

	sch := record.NewSchema()
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())

	return &MergeJoinPlan{
		p1:       p1,
		p2:       p2,
		fldname1: fldname1,
		fldname2: fldname2,
		sch:      sch,
	}
}

// Open first sorts its two underlying scans on their join field.
// It then returns a mergejoin scan of the two sorted table scans.
func (m *MergeJoinPlan) Open() query.Scan {
	s1 := m.p1.Open()
	s2 := m.p2.Open().(*SortScan) // Type assertion
	return NewMergeJoinScan(s1, s2, m.fldname1, m.fldname2)
}

// BlocksAccessed returns the number of block accesses required to mergejoin the sorted tables.
func (m *MergeJoinPlan) BlocksAccessed() int {
	return m.p1.BlocksAccessed() + m.p2.BlocksAccessed()
}

// RecordsOutput returns the number of records in the join.
func (m *MergeJoinPlan) RecordsOutput() int {
	maxvals := max(m.p1.DistinctValues(m.fldname1), m.p2.DistinctValues(m.fldname2))
	return (m.p1.RecordsOutput() * m.p2.RecordsOutput()) / maxvals
}

// DistinctValues estimates the distinct number of field values in the join.
func (m *MergeJoinPlan) DistinctValues(fldname string) int {
	if m.p1.Schema().HasField(fldname) {
		return m.p1.DistinctValues(fldname)
	}
	return m.p2.DistinctValues(fldname)
}

// Schema returns the schema of the join.
func (m *MergeJoinPlan) Schema() *record.Schema {
	return m.sch
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
