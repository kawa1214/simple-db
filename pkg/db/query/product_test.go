package query

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestProduct(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp"
	fm := file.NewFileMgr(dir, 800)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)
	// db := simpledb.NewSimpleDB("producttest")

	// tx := db.NewTx()

	// sch1 := simpledb.NewSchema()
	sch1 := record.NewSchema()
	mdm1 := metadata.NewMetadataMgr(true, tx)
	mdm1.CreateTable("MyTable", sch1, tx)
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := record.NewLayoutFromSchema(sch1)
	ts1 := record.NewTableScan(tx, "T1", layout1)

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := record.NewLayoutFromSchema(sch2)
	ts2 := record.NewTableScan(tx, "T2", layout2)

	ts1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d records into T1.\n", n)
	for i := 0; i < n; i++ {
		ts1.Insert()
		ts1.SetInt("A", i)
		ts1.SetString("B", "aaa"+fmt.Sprint(i))
	}
	ts1.Close()

	ts2.BeforeFirst()
	t.Logf("Inserting %d records into T2.\n", n)
	for i := 0; i < n; i++ {
		ts2.Insert()
		ts2.SetInt("C", n-i-1)
		ts2.SetString("D", "bbb"+fmt.Sprint(n-i-1))
	}
	ts2.Close()

	s1 := record.NewTableScan(tx, "T1", layout1)
	s2 := record.NewTableScan(tx, "T2", layout2)
	s3 := NewProductScan(s1, s2)
	for s3.Next() {
		val := s3.GetString("B")
		t.Logf("B: %s\n", val)
	}
	s3.Close()
	tx.Commit()

	t.Error()
}
