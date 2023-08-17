package query

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestScan1(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/scan1"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	_ = metadata.NewMetadataMgr(true, tx)
	tx.Commit()

	sch1 := record.NewSchema()
	sch1.AddIntField("A")
	sch1.AddStringField("B", 9)
	layout := record.NewLayoutFromSchema(sch1)
	s1 := record.NewTableScan(tx, "T", layout)

	s1.BeforeFirst()
	n := 200
	t.Logf("Inserting %d random records.\n", n)
	for i := 0; i < n; i++ {
		s1.Insert()
		k := int(rand.Float64() * 50)
		s1.SetInt("A", k)
		s1.SetString("B", "rec"+fmt.Sprint(k))
	}
	s1.Close()

	s2 := record.NewTableScan(tx, "T", layout)
	c := record.NewIntConstant(10)
	term := NewTerm(*NewFieldExpression("A"), *NewConstantExpression(c))
	pred := NewPredicate(term)
	t.Logf("The predicate is %v", pred)
	s3 := NewSelectScan(s2, *pred)
	fields := []string{"B"}
	s4 := NewProjectScan(s3, fields)
	for s4.Next() {
		t.Log(s4.GetString("B"))
	}
	s4.Close()
	tx.Commit()

	// t.Error()
}
