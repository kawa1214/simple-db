package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// TablePlan corresponds to a table in the query plan.
type TablePlan struct {
	tblname string
	tx      *tx.Transaction
	layout  *record.Layout
	si      *metadata.StatInfo
}

// NewTablePlan creates a leaf node in the query tree corresponding to the specified table.
func NewTablePlan(tx *tx.Transaction, tblname string, md *metadata.MetadataMgr) *TablePlan {
	layout := md.GetLayout(tblname, tx)
	si := md.GetStatInfo(tblname, layout, tx)
	return &TablePlan{
		tblname: tblname,
		tx:      tx,
		layout:  layout,
		si:      si,
	}
}

// Open creates a table scan for this query.
func (tp *TablePlan) Open() query.Scan {
	return record.NewTableScan(tp.tx, tp.tblname, tp.layout)
}

// BlocksAccessed estimates the number of block accesses for the table.
func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

// RecordsOutput estimates the number of records in the table.
func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

// DistinctValues estimates the number of distinct field values in the table.
func (tp *TablePlan) DistinctValues(fldname string) int {
	return tp.si.DistinctValues(fldname)
}

// Schema determines the schema of the table.
func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}
