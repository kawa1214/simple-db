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

func TestScan2(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/scan2"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)
	tx := tx.NewTransaction(fm, lm, bm)
	_ = metadata.NewMetadataMgr(true, tx)

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout1 := record.NewLayoutFromSchema(sch1)
	us1 := record.NewTableScan(tx, "T1", layout1)
	us1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d records into T1.\n", n)
	for i := 0; i < n; i++ {
		us1.Insert()
		us1.SetInt("A", i)
		us1.SetString("B", "bbb"+fmt.Sprint(i))
	}
	us1.Close()

	sch2 := record.NewSchema()
	sch2.AddIntField("C")
	sch2.AddStringField("D", 9)
	layout2 := record.NewLayoutFromSchema(sch2)
	us2 := record.NewTableScan(tx, "T2", layout2)
	us2.BeforeFirst()
	t.Logf("Inserting %d records into T2.\n", n)
	for i := 0; i < n; i++ {
		us2.Insert()
		us2.SetInt("C", n-i-1)
		us2.SetString("D", "ddd"+fmt.Sprint(n-i-1))
	}
	us2.Close()

	s1 := record.NewTableScan(tx, "T1", layout1)
	s2 := record.NewTableScan(tx, "T2", layout2)
	s3 := NewProductScan(s1, s2)
	term := NewTerm(*NewFieldExpression("A"), *NewFieldExpression("C"))
	pred := NewPredicate(term)
	t.Log("The predicate is", pred)
	s4 := NewSelectScan(s3, *pred)

	fields := []string{"B", "D"}
	s5 := NewProjectScan(s4, fields)
	for s5.Next() {
		bVal, err := s5.GetString("B")
		if err != nil {
			t.Error(err)
		}
		dVal, err := s5.GetString("D")
		if err != nil {
			t.Error(err)
		}
		t.Logf("%s %s\n", bVal, dVal)
	}
	s5.Close()
	tx.Commit()

	t.Error()
}
