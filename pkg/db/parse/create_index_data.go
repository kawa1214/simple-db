package parse

// CreateIndexData is the parser for the "create index" statement.
type CreateIndexData struct {
	idxname, tblname, fldname string
}

// NewCreateIndexData saves the table and field names of the specified index.
func NewCreateIndexData(idxname, tblname, fldname string) *CreateIndexData {
	return &CreateIndexData{
		idxname: idxname,
		tblname: tblname,
		fldname: fldname,
	}
}

// IndexName returns the name of the index.
func (c *CreateIndexData) IndexName() string {
	return c.idxname
}

// TableName returns the name of the indexed table.
func (c *CreateIndexData) TableName() string {
	return c.tblname
}

// FieldName returns the name of the indexed field.
func (c *CreateIndexData) FieldName() string {
	return c.fldname
}
