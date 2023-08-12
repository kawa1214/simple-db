package plan

import (
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/parse"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type BasicUpdatePlanner struct {
	mdm *metadata.MetadataMgr
}

func NewBasicUpdatePlanner(mdm *metadata.MetadataMgr) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

func (bup *BasicUpdatePlanner) ExecuteDelete(data parse.DeleteData, tx *tx.Transaction) int {
	p := NewTablePlan(tx, data.TableName(), bup.mdm)
	sp := NewSelectPlan(p, *data.Pred())
	us := sp.Open().(query.UpdateScan)

	count := 0
	for us.Next() {
		us.Delete()
		count++
	}
	us.Close()
	return count
}

func (bup *BasicUpdatePlanner) ExecuteModify(data parse.ModifyData, tx *tx.Transaction) int {
	p := NewTablePlan(tx, data.TableName(), bup.mdm)
	sp := NewSelectPlan(p, *data.Pred())
	us, ok := sp.Open().(query.UpdateScan)
	if !ok {
		panic("failed to cast to UpdateScan")
	}
	count := 0
	for us.Next() {
		val := data.NewValue().Evaluate(us)
		us.SetVal(data.TargetField(), val)
		count++
	}
	us.Close()
	return count
}

func (bup *BasicUpdatePlanner) ExecuteInsert(data parse.InsertData, tx *tx.Transaction) int {
	p := NewTablePlan(tx, data.TableName(), bup.mdm)
	us, ok := p.Open().(query.UpdateScan)
	if !ok {
		panic("failed to cast to UpdateScan")
	}
	us.Insert()
	i := 0
	for _, fldname := range data.Fields() {
		val := data.Vals()[i]
		us.SetVal(fldname, val)
		i++
	}
	us.Close()
	return 1
}

func (bup *BasicUpdatePlanner) ExecuteCreateTable(data parse.CreateTableData, tx *tx.Transaction) int {
	bup.mdm.CreateTable(data.TableName(), data.NewSchema(), tx)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateView(data parse.CreateViewData, tx *tx.Transaction) int {
	bup.mdm.CreateView(data.ViewName(), data.ViewDef(), tx)
	return 0
}

func (bup *BasicUpdatePlanner) ExecuteCreateIndex(data parse.CreateIndexData, tx *tx.Transaction) int {
	bup.mdm.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), tx)
	return 0
}
