package metadata

import (
	"strings"

	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

// IndexMgr is the index manager.
type IndexMgr struct {
	layout  *record.Layout
	tblmgr  *TableMgr
	statmgr *StatMgr
}

// NewIndexMgr creates the index manager. If the database is new, the "idxcat" table is created.
func NewIndexMgr(isnew bool, tblmgr *TableMgr, statmgr *StatMgr, tx *tx.Transaction) *IndexMgr {
	if isnew {
		sch := record.NewSchema()
		sch.AddStringField("indexname", MAX_NAME)
		sch.AddStringField("tablename", MAX_NAME)
		sch.AddStringField("fieldname", MAX_NAME)
		tblmgr.CreateTable("idxcat", sch, tx)
	}
	return &IndexMgr{
		layout:  tblmgr.GetLayout("idxcat", tx),
		tblmgr:  tblmgr,
		statmgr: statmgr,
	}
}

// CreateIndex creates an index of the specified type for the specified field.
func (im *IndexMgr) CreateIndex(idxname, tblname, fldname string, tx *tx.Transaction) {
	ts := record.NewTableScan(tx, "idxcat", im.layout)
	ts.Insert()
	ts.SetString("indexname", idxname)
	ts.SetString("tablename", tblname)
	ts.SetString("fieldname", fldname)
	ts.Close()
}

// GetIndexInfo returns a map containing the index info for all indexes on the specified table.
func (im *IndexMgr) GetIndexInfo(tblname string, tx *tx.Transaction) map[string]*IndexInfo {
	result := make(map[string]*IndexInfo)
	ts := record.NewTableScan(tx, "idxcat", im.layout)
	for ts.Next() {
		if strings.EqualFold(ts.GetString("tablename"), tblname) {
			idxname := ts.GetString("indexname")
			fldname := ts.GetString("fieldname")
			tblLayout := im.tblmgr.GetLayout(tblname, tx)
			tblsi := im.statmgr.GetStatInfo(tblname, tblLayout, tx)
			ii := NewIndexInfo(idxname, fldname, tblLayout.Schema(), tx, tblsi)
			result[fldname] = ii
		}
	}
	ts.Close()
	return result
}
