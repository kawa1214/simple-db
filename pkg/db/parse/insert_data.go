package parse

import "github.com/kawa1214/simple-db/pkg/db/record"

// InsertData is the data for the SQL "insert" statement.
type InsertData struct {
	tblname string
	flds    []string
	vals    []*record.Constant
}

// NewInsertData saves the table name and the field and value lists.
func NewInsertData(tblname string, flds []string, vals []*record.Constant) *InsertData {
	return &InsertData{
		tblname: tblname,
		flds:    flds,
		vals:    vals,
	}
}

// TableName returns the name of the affected table.
func (i *InsertData) TableName() string {
	return i.tblname
}

// Fields returns a list of fields for which values will be specified in the new record.
func (i *InsertData) Fields() []string {
	return i.flds
}

// Vals returns a list of values for the specified fields. There is a one-one correspondence between this list of values and the list of fields.
func (i *InsertData) Vals() []*record.Constant {
	return i.vals
}
