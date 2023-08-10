package metadata

import (
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

const MAX_VIEWDEF = 100

type ViewMgr struct {
	tblMgr *TableMgr
}

func NewViewMgr(isNew bool, tblMgr *TableMgr, tx *tx.Transaction) *ViewMgr {
	vm := &ViewMgr{tblMgr: tblMgr}
	if isNew {
		sch := record.NewSchema()
		sch.AddStringField("viewname", MAX_NAME)
		sch.AddStringField("viewdef", MAX_VIEWDEF)
		tblMgr.CreateTable("viewcat", sch, tx)
	}
	return vm
}

func (vm *ViewMgr) CreateView(vname, vdef string, tx *tx.Transaction) {
	layout := vm.tblMgr.GetLayout("viewcat", tx)
	ts := record.NewTableScan(tx, "viewcat", layout)
	ts.Insert()
	ts.SetString("viewname", vname)
	ts.SetString("viewdef", vdef)
	ts.Close()
}

func (vm *ViewMgr) GetViewDef(vname string, tx *tx.Transaction) string {
	var result string
	layout := vm.tblMgr.GetLayout("viewcat", tx)
	ts := record.NewTableScan(tx, "viewcat", layout)
	for ts.Next() {
		if ts.GetString("viewname") == vname {
			result = ts.GetString("viewdef")
			break
		}
	}
	ts.Close()
	return result
}
