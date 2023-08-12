package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// UpdatePlanner is the interface implemented by the planners
// for SQL insert, delete, and modify statements.
type UpdatePlanner interface {
	// ExecuteInsert executes the specified insert statement and
	// returns the number of affected records.
	ExecuteInsert(data parse.InsertData, tx *tx.Transaction) int

	// ExecuteDelete executes the specified delete statement and
	// returns the number of affected records.
	ExecuteDelete(data parse.DeleteData, tx *tx.Transaction) int

	// ExecuteModify executes the specified modify statement and
	// returns the number of affected records.
	ExecuteModify(data parse.ModifyData, tx *tx.Transaction) int

	// ExecuteCreateTable executes the specified create table statement and
	// returns the number of affected records.
	ExecuteCreateTable(data parse.CreateTableData, tx *tx.Transaction) int

	// ExecuteCreateView executes the specified create view statement and
	// returns the number of affected records.
	ExecuteCreateView(data parse.CreateViewData, tx *tx.Transaction) int

	// ExecuteCreateIndex executes the specified create index statement and
	// returns the number of affected records.
	ExecuteCreateIndex(data parse.CreateIndexData, tx *tx.Transaction) int
}

// InsertData, DeleteData, ModifyData, CreateTableData, CreateViewData, and CreateIndexData
// are placeholder types. You'll need to define these types based on your actual Go environment
// and the rest of your DBMS code.
