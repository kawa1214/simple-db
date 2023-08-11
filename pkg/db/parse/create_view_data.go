package parse

// CreateViewData is the data for the SQL "create view" statement.
type CreateViewData struct {
	viewname string
	qrydata  *QueryData
}

// NewCreateViewData saves the view name and its definition.
func NewCreateViewData(viewname string, qrydata *QueryData) *CreateViewData {
	return &CreateViewData{
		viewname: viewname,
		qrydata:  qrydata,
	}
}

// ViewName returns the name of the new view.
func (c *CreateViewData) ViewName() string {
	return c.viewname
}

// ViewDef returns the definition of the new view.
func (c *CreateViewData) ViewDef() string {
	return c.qrydata.String()
}
