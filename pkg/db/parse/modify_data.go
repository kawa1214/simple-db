package parse

import "github.com/kawa1214/simple-db/pkg/db/query"

// ModifyData is the data for the SQL "update" statement.
type ModifyData struct {
	tblname string
	fldname string
	newval  *query.Expression
	pred    *query.Predicate
}

// NewModifyData saves the table name, the modified field and its new value, and the predicate.
func NewModifyData(tblname, fldname string, newval *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		tblname: tblname,
		fldname: fldname,
		newval:  newval,
		pred:    pred,
	}
}

// TableName returns the name of the affected table.
func (m *ModifyData) TableName() string {
	return m.tblname
}

// TargetField returns the field whose values will be modified.
func (m *ModifyData) TargetField() string {
	return m.fldname
}

// NewValue returns an expression. Evaluating this expression for a record produces the value that will be stored in the record's target field.
func (m *ModifyData) NewValue() *query.Expression {
	return m.newval
}

// Pred returns the predicate that describes which records should be modified.
func (m *ModifyData) Pred() *query.Predicate {
	return m.pred
}
