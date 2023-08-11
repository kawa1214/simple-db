package parse

import (
	"strings"

	"github.com/kawa1214/simple-db/pkg/db/query"
)

// QueryData holds data for the SQL select statement.
type QueryData struct {
	fields []string
	tables []string
	pred   *query.Predicate
}

// NewQueryData creates a new QueryData instance.
func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

// Fields returns the fields mentioned in the select clause.
func (q *QueryData) Fields() []string {
	return q.fields
}

// Tables returns the tables mentioned in the from clause.
func (q *QueryData) Tables() []string {
	return q.tables
}

// Pred returns the predicate that describes which records should be in the output table.
func (q *QueryData) Pred() *query.Predicate {
	return q.pred
}

// String returns the string representation of the QueryData.
func (q *QueryData) String() string {
	result := "select "
	result += strings.Join(q.fields, ", ")
	result += " from "
	result += strings.Join(q.tables, ", ")

	predString := q.pred.String()
	if predString != "" {
		result += " where " + predString
	}
	return result
}
