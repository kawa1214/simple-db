package plan

// func TestSingleTablePlanTest(t *testing.T) {
// 	db := simpledb.NewSimpleDB("studentdb")
// 	mdm := db.MdMgr()
// 	tx := db.NewTx()

// 	// the STUDENT node
// 	p1 := simpledb.NewTablePlan(tx, "student", mdm)

// 	// the Select node for "major = 10"
// 	t := simpledb.NewTerm(simpledb.NewExpression("majorid"), simpledb.NewExpression(simpledb.NewConstant(10)))
// 	pred := simpledb.NewPredicate(t)
// 	p2 := simpledb.NewSelectPlan(p1, pred)

// 	// the Select node for "gradyear = 2020"
// 	t2 := simpledb.NewTerm(simpledb.NewExpression("gradyear"), simpledb.NewExpression(simpledb.NewConstant(2020)))
// 	pred2 := simpledb.NewPredicate(t2)
// 	p3 := simpledb.NewSelectPlan(p2, pred2)

// 	// the Project node
// 	c := []string{"sname", "majorid", "gradyear"}
// 	p4 := simpledb.NewProjectPlan(p3, c)

// 	// Look at R(p) and B(p) for each plan p.
// 	printStats(1, p1)
// 	printStats(2, p2)
// 	printStats(3, p3)
// 	printStats(4, p4)

// 	// Change p2 to be p2, p3, or p4 to see the other scans in action.
// 	// Changing p2 to p4 will throw an exception because SID is not in the projection list.
// 	s, err := p2.Open()
// 	if err != nil {
// 		panic(err)
// 	}
// 	for s.Next() {
// 		fmt.Println(s.GetInt("sid"), s.GetString("sname"), s.GetInt("majorid"), s.GetInt("gradyear"))
// 	}
// 	s.Close()
// }

// func printStats(n int, p simpledb.Plan) {
// 	fmt.Printf("Here are the stats for plan p%d\n", n)
// 	fmt.Printf("\tR(p%d): %d\n", n, p.RecordsOutput())
// 	fmt.Printf("\tB(p%d): %d\n\n", n, p.BlocksAccessed())
// }
