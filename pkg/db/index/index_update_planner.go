package index

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type IndexUpdatePlanner struct {
	mdm *metadata.MetadataMgr
}

func NewIndexUpdatePlanner(mdm *metadata.MetadataMgr) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{mdm: mdm}
}

func (iup *IndexUpdatePlanner) ExecuteInsert(data parse.InsertData, tx *tx.Transaction) int {
	tblname := data.TableName()
	p := plan.NewTablePlan(tx, tblname, iup.mdm)

	// first, insert the record
	s := p.Open().(query.UpdateScan)
	s.Insert()
	rid := s.GetRid()

	// then modify each field, inserting an index record if appropriate
	indexes := iup.mdm.GetIndexInfo(tblname, tx)
	for i, fldname := range data.Fields() {
		val := data.Vals()[i]
		s.SetVal(fldname, val)

		if ii, ok := indexes[fldname]; ok {
			idx := Open(ii)
			idx.Insert(val, rid)
			idx.Close()
		}
	}
	s.Close()
	return 1
}
