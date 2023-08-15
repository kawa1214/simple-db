package materialize

import (
	"sync"

	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// TempTable is a class that creates temporary tables.
type TempTable struct {
	tx      *tx.Transaction
	tblname string
	layout  *record.Layout
}

var (
	nextTableNum int
	mu           sync.Mutex
)

// NewTempTable allocates a name for a new temporary table having the specified schema.
func NewTempTable(tx *tx.Transaction, sch *record.Schema) *TempTable {
	tt := &TempTable{
		tx:      tx,
		tblname: nextTableName(),
		layout:  record.NewLayoutFromSchema(sch),
	}
	return tt
}

// Open opens a table scan for the temporary table.
func (tt *TempTable) Open() query.UpdateScan {
	return record.NewTableScan(tt.tx, tt.tblname, tt.layout)
}

// TableName returns the table name.
func (tt *TempTable) TableName() string {
	return tt.tblname
}

// GetLayout returns the table's metadata.
func (tt *TempTable) GetLayout() *record.Layout {
	return tt.layout
}

func nextTableName() string {
	mu.Lock()
	defer mu.Unlock()
	nextTableNum++
	return "temp" + string(rune(nextTableNum))
}
