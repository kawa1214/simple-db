package metadata

import (
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type MetadataMgr struct {
	tblmgr  *TableMgr
	viewmgr *ViewMgr
	statmgr *StatMgr
	idxmgr  *IndexMgr
}

func NewMetadataMgr(isnew bool, tx *tx.Transaction) *MetadataMgr {
	tm := NewTableMgr(isnew, tx)
	sm := NewStatMgr(tm, tx)
	m := &MetadataMgr{
		tblmgr:  tm,
		viewmgr: NewViewMgr(isnew, tm, tx),
		statmgr: sm,
		idxmgr:  NewIndexMgr(isnew, tm, sm, tx),
	}
	return m
}

func (m *MetadataMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) {
	m.tblmgr.CreateTable(tblname, sch, tx)
}

func (m *MetadataMgr) GetLayout(tblname string, tx *tx.Transaction) *record.Layout {
	return m.tblmgr.GetLayout(tblname, tx)
}

func (m *MetadataMgr) CreateView(viewname string, viewdef string, tx *tx.Transaction) {
	m.viewmgr.CreateView(viewname, viewdef, tx)
}

func (m *MetadataMgr) GetViewDef(viewname string, tx *tx.Transaction) string {
	return m.viewmgr.GetViewDef(viewname, tx)
}

func (m *MetadataMgr) CreateIndex(idxname string, tblname string, fldname string, tx *tx.Transaction) {
	m.idxmgr.CreateIndex(idxname, tblname, fldname, tx)
}

func (m *MetadataMgr) GetIndexInfo(tblname string, tx *tx.Transaction) map[string]*IndexInfo {
	return m.idxmgr.GetIndexInfo(tblname, tx)
}

func (m *MetadataMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	return m.statmgr.GetStatInfo(tblname, layout, tx)
}
