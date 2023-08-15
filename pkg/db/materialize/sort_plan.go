package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// SortPlan is the plan class for the sort operator.
type SortPlan struct {
	tx   *tx.Transaction
	p    plan.Plan
	sch  *record.Schema
	comp *RecordComparator
}

// NewSortPlan creates a sort plan for the specified query.
func NewSortPlan(tx *tx.Transaction, p plan.Plan, sortfields []string) *SortPlan {
	return &SortPlan{
		tx:   tx,
		p:    p,
		sch:  p.Schema(),
		comp: NewRecordComparator(sortfields),
	}
}

// Open splits the source scan into runs and sorts them.
func (sp *SortPlan) Open() query.Scan {
	src := sp.p.Open()
	runs := sp.splitIntoRuns(src)
	src.Close()
	for len(runs) > 2 {
		runs = sp.doAMergeIteration(runs)
	}
	return NewSortScan(runs, sp.comp)
}

// BlocksAccessed returns the number of blocks in the sorted table.
func (sp *SortPlan) BlocksAccessed() int {
	mp := NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

// RecordsOutput returns the number of records in the sorted table.
func (sp *SortPlan) RecordsOutput() int {
	return sp.p.RecordsOutput()
}

// DistinctValues returns the number of distinct field values in the sorted table.
func (sp *SortPlan) DistinctValues(fldname string) int {
	return sp.p.DistinctValues(fldname)
}

// Schema returns the schema of the sorted table.
func (sp *SortPlan) Schema() *record.Schema {
	return sp.sch
}

func (sp *SortPlan) splitIntoRuns(src query.Scan) []*TempTable {
	var temps []*TempTable
	src.BeforeFirst()
	if !src.Next() {
		return temps
	}
	currentTemp := NewTempTable(sp.tx, sp.sch)
	temps = append(temps, currentTemp)
	currentScan := currentTemp.Open()
	for sp.copy(src, currentScan) {
		if sp.comp.Compare(src, currentScan) < 0 {
			currentScan.Close()
			currentTemp = NewTempTable(sp.tx, sp.sch)
			temps = append(temps, currentTemp)
			currentScan = currentTemp.Open()
		}
	}
	currentScan.Close()
	return temps
}

func (sp *SortPlan) doAMergeIteration(runs []*TempTable) []*TempTable {
	var result []*TempTable
	for len(runs) > 1 {
		p1 := runs[0]
		runs = runs[1:]
		p2 := runs[0]
		runs = runs[1:]
		result = append(result, sp.mergeTwoRuns(p1, p2))
	}
	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result
}

func (sp *SortPlan) mergeTwoRuns(p1, p2 *TempTable) *TempTable {
	src1 := p1.Open()
	src2 := p2.Open()
	result := NewTempTable(sp.tx, sp.sch)
	dest := result.Open()

	hasMore1 := src1.Next()
	hasMore2 := src2.Next()
	for hasMore1 && hasMore2 {
		if sp.comp.Compare(src1, src2) < 0 {
			hasMore1 = sp.copy(src1, dest)
		} else {
			hasMore2 = sp.copy(src2, dest)
		}
	}

	if hasMore1 {
		for hasMore1 {
			hasMore1 = sp.copy(src1, dest)
		}
	} else {
		for hasMore2 {
			hasMore2 = sp.copy(src2, dest)
		}
	}
	src1.Close()
	src2.Close()
	dest.Close()
	return result
}

func (sp *SortPlan) copy(src query.Scan, dest query.UpdateScan) bool {
	dest.Insert()
	for _, fldName := range sp.sch.Fields {
		dest.SetVal(fldName, src.GetVal(fldName))
	}
	return src.Next()
}
