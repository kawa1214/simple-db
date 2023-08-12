package query

type PlanInfo interface {
	DistinctValues(fldname string) int
}
