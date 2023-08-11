package parse

import "github.com/kawa1214/simple-db/pkg/db/record"

// CreateTableData is the data for the SQL "create table" statement.
type CreateTableData struct {
	tblname string
	sch     *record.Schema
}

// NewCreateTableData saves the table name and schema.
func NewCreateTableData(tblname string, sch *record.Schema) *CreateTableData {
	return &CreateTableData{
		tblname: tblname,
		sch:     sch,
	}
}

// TableName returns the name of the new table.
func (c *CreateTableData) TableName() string {
	return c.tblname
}

// NewSchema returns the schema of the new table.
func (c *CreateTableData) NewSchema() *record.Schema {
	return c.sch
}
