package plan

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/query"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestSingleTablePlanTest(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)

	// the STUDENT node
	p1 := NewTablePlan(tx, "student", mdm)

	// the Select node for "major = 10"
	term := query.NewTerm(*query.NewFieldExpression("majorid"), *query.NewConstantExpression(record.NewIntConstant(10)))
	pred := query.NewPredicate(term)
	p2 := NewSelectPlan(p1, *pred)

	// the Select node for "gradyear = 2020"
	t2 := query.NewTerm(*query.NewFieldExpression("gradyear"), *query.NewConstantExpression(record.NewIntConstant(2020)))
	pred2 := query.NewPredicate(t2)
	p3 := NewSelectPlan(p2, *pred2)

	// the Project node
	c := []string{"sname", "majorid", "gradyear"}
	p4 := NewProjectPlan(p3, c)

	// Look at R(p) and B(p) for each plan p.
	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)

	// Change p2 to be p2, p3, or p4 to see the other scans in action.
	// Changing p2 to p4 will throw an exception because SID is not in the projection list.
	s := p2.Open()

	for s.Next() {
		fmt.Println(s.GetInt("sid"), s.GetString("sname"), s.GetInt("majorid"), s.GetInt("gradyear"))
	}
	s.Close()
}

func printStats(n int, p Plan) {
	fmt.Printf("Here are the stats for plan p%d\n", n)
	fmt.Printf("\tR(p%d): %d\n", n, p.RecordsOutput())
	fmt.Printf("\tB(p%d): %d\n\n", n, p.BlocksAccessed())
}
