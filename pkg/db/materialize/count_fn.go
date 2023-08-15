package materialize

import (
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
)

// CountFn implements the count aggregation function.
type CountFn struct {
	fldname string
	count   int
}

// NewCountFn creates a count aggregation function for the specified field.
func NewCountFn(fldname string) *CountFn {
	return &CountFn{fldname: fldname}
}

// ProcessFirst starts a new count.
// Since SimpleDB does not support null values,
// every record will be counted,
// regardless of the field.
// The current count is thus set to 1.
func (c *CountFn) ProcessFirst(s query.Scan) {
	c.count = 1
}

// ProcessNext increments the count since SimpleDB does not support null values.
func (c *CountFn) ProcessNext(s query.Scan) {
	c.count++
}

// FieldName returns the field's name, prepended by "countof".
func (c *CountFn) FieldName() string {
	return "countof" + c.fldname
}

// Value returns the current count.
func (c *CountFn) Value() record.Constant {
	return *record.NewIntConstant(c.count)
}
