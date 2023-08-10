package metadata

import (
	"sync"

	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
)

type TableMgr struct {
	tcatLayout, fcatLayout *record.Layout
	mu                     sync.Mutex
}

// MaxName is the maximum character length a tablename or fieldname can have.
const MAX_NAME = 16

// NewTableMgr creates a new catalog manager for the database system.
func NewTableMgr(isNew bool, tx *tx.Transaction) *TableMgr {
	tm := &TableMgr{}

	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	tm.tcatLayout = record.NewLayoutFromSchema(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tm.fcatLayout = record.NewLayoutFromSchema(fcatSchema)

	if isNew {
		tm.CreateTable("tblcat", tcatSchema, tx)
		tm.CreateTable("fldcat", fcatSchema, tx)
	}
	return tm
}

// CreateTable creates a new table with the given name and schema.
func (tm *TableMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	layout := record.NewLayoutFromSchema(sch)

	// insert one record into tblcat
	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	tcat.Insert()
	tcat.SetString("tblname", tblname)
	tcat.SetInt("slotsize", layout.SlotSize())
	tcat.Close()

	// insert a record into fldcat for each field
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for _, fldname := range sch.Fields {
		schType, err := sch.Type(fldname)
		if err != nil {
			panic(err)
		}
		schLen, err := sch.Length(fldname)
		if err != nil {
			panic(err)
		}

		fcat.Insert()
		fcat.SetString("tblname", tblname)
		fcat.SetString("fldname", fldname)
		fcat.SetInt("type", schType)
		fcat.SetInt("length", schLen)
		fcat.SetInt("offset", layout.Offset(fldname))
	}
	fcat.Close()
}

// GetLayout retrieves the layout of the specified table from the catalog.
func (tm *TableMgr) GetLayout(tblname string, tx *tx.Transaction) *record.Layout {
	size := -1

	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		if tcat.GetString("tblname") == tblname {
			size = tcat.GetInt("slotsize")
			break
		}
	}
	tcat.Close()

	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("tblname") == tblname {
			fldname := fcat.GetString("fldname")
			fldtype := fcat.GetInt("type")
			fldlen := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			offsets[fldname] = offset
			sch.AddField(fldname, fldtype, fldlen)
		}
	}
	fcat.Close()
	return record.NewLayout(sch, offsets, size)
}
