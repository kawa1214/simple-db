package parse

import "github.com/kawa1214/simple-db/pkg/db/query"

// DeleteData is the data for the SQL "delete" statement.
type DeleteData struct {
	tblname string
	pred    *query.Predicate
}

// NewDeleteData saves the table name and predicate.
func NewDeleteData(tblname string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		tblname: tblname,
		pred:    pred,
	}
}

// TableName returns the name of the affected table.
func (d *DeleteData) TableName() string {
	return d.tblname
}

// Pred returns the predicate that describes which records should be deleted.
func (d *DeleteData) Pred() *query.Predicate {
	return d.pred
}
