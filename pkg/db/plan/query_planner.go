package plan

import "github.com/kawa1214/simple-db/pkg/db/tx"

// QueryPlanner is the interface implemented by planners for the SQL select statement.
type QueryPlanner interface {
	// CreatePlan creates a plan for the parsed query.
	CreatePlan(data QueryData, tx *tx.Transaction) (Plan, error)
}
