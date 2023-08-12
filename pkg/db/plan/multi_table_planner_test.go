package plan

// func TestMultiTablePlanner(t *testing.T) {
// 	db := simpledb.NewSimpleDB("studentdb")
// 	mdm := db.MdMgr()
// 	tx := db.NewTx()

// 	// the STUDENT node
// 	p1 := NewTablePlan(tx, "student", mdm)

// 	// the DEPT node
// 	p2 := NewTablePlan(tx, "dept", mdm)

// 	// the Product node for student x dept
// 	p3 := NewProductPlan(p1, p2)

// 	// the Select node for "majorid = did"
// 	t := NewTerm(NewExpression("majorid"), NewExpression("did"))
// 	pred := NewPredicate(t)
// 	p4 := NewSelectPlan(p3, pred)

// 	// Look at R(p) and B(p) for each plan p.
// 	printStats(1, p1)
// 	printStats(2, p2)
// 	printStats(3, p3)
// 	printStats(4, p4)

// 	// Change p3 to be p4 to see the select scan in action.
// 	s, err := p3.Open()
// 	if err != nil {
// 		panic(err)
// 	}
// 	for s.Next() {
// 		fmt.Println(s.GetString("sname"), s.GetString("dname"))
// 	}
// 	s.Close()
// }

// func printStats(n int, p simpledb.Plan) {
// 	fmt.Println("Here are the stats for plan p", n)
// 	fmt.Println("\tR(p", n, "): ", p.RecordsOutput())
// 	fmt.Println("\tB(p", n, "): ", p.BlocksAccessed())
// 	fmt.Println()
// }
