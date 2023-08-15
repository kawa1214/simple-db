package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// AggregationFn is the interface implemented by aggregation functions.
// Aggregation functions are used by the groupby operator.
type AggregationFn interface {
	// ProcessFirst uses the current record of the specified scan
	// to be the first record in the group.
	ProcessFirst(s query.Scan)

	// ProcessNext uses the current record of the specified scan
	// to be the next record in the group.
	ProcessNext(s query.Scan)

	// FieldName returns the name of the new aggregation field.
	FieldName() string

	// Value returns the computed aggregation value.
	Value() *record.Constant
}
