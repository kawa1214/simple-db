package query

import "github.com/kawa1214/simple-db/pkg/db/record"

type UpdateScan interface {
	Scan // Embed the Scan interface

	// SetVal modifies the field value of the current record using a Constant
	SetVal(fldname string, val *record.Constant)

	// SetInt modifies the field value of the current record with an integer value
	SetInt(fldname string, val int)

	// SetString modifies the field value of the current record with a string value
	SetString(fldname string, val string)

	// Insert inserts a new record somewhere in the scan
	Insert()

	// Delete deletes the current record from the scan
	Delete()

	// GetRid returns the ID of the current record
	GetRid() *record.RID

	// MoveToRid positions the scan so that the current record has the specified ID
	MoveToRid(rid *record.RID)
}
