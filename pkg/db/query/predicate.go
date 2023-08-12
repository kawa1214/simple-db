package query

import (
	"strings"

	"github.com/kawa1214/simple-db/pkg/db/record"
)

// Predicate is a Boolean combination of terms.
type Predicate struct {
	terms []*Term
}

const MAX_TERMS = 10

// NewPredicate creates an empty predicate, corresponding to "true".
func NewPredicate(term *Term) *Predicate {

	pr := &Predicate{
		terms: make([]*Term, 0, MAX_TERMS),
	}

	if term != nil {
		// pr.terms[0] = term
		pr.terms = append(pr.terms, term)
	}

	return pr
}

// NewPredicateWithTerm creates a predicate containing a single term.
func NewPredicateWithTerm(t *Term) *Predicate {
	return &Predicate{terms: []*Term{t}}
}

// ConjoinWith modifies the predicate to be the conjunction of itself and the specified predicate.
func (p *Predicate) ConjoinWith(pred *Predicate) {
	p.terms = append(p.terms, pred.terms...)
}

// IsSatisfied returns true if the predicate evaluates to true with respect to the specified scan.
func (p *Predicate) IsSatisfied(s Scan) bool {
	for _, t := range p.terms {
		if !t.IsSatisfied(s) {
			return false
		}
	}
	return true
}

// ReductionFactor calculates the extent to which selecting on the predicate reduces the number of records output by a query.
func (p *Predicate) ReductionFactor(plan PlanInfo) int {
	factor := 1
	for _, t := range p.terms {
		factor *= t.ReductionFactor(plan)
	}
	return factor
}

// SelectSubPred returns the subpredicate that applies to the specified schema.
func (p *Predicate) SelectSubPred(sch *record.Schema) *Predicate {
	result := NewPredicate(nil)
	for _, t := range p.terms {
		if t.AppliesTo(sch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// JoinSubPred returns the subpredicate consisting of terms that apply to the union of the two specified schemas, but not to either schema separately.
func (p *Predicate) JoinSubPred(sch1, sch2 *record.Schema) *Predicate {
	result := NewPredicate(nil)
	newsch := record.NewSchema()
	newsch.AddAll(sch1)
	newsch.AddAll(sch2)
	for _, t := range p.terms {
		if !t.AppliesTo(sch1) && !t.AppliesTo(sch2) && t.AppliesTo(newsch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// EquatesWithConstant determines if there is a term of the form "F=c" where F is the specified field and c is some constant.
func (p *Predicate) EquatesWithConstant(fldname string) *record.Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fldname)
		if c != nil {
			return c
		}
	}
	return nil
}

// EquatesWithField determines if there is a term of the form "F1=F2" where F1 is the specified field and F2 is another field.
func (p *Predicate) EquatesWithField(fldname string) string {
	for _, t := range p.terms {
		s := t.EquatesWithField(fldname)
		if s != "" {
			return s
		}
	}
	return ""
}

func (p *Predicate) String() string {
	var terms []string
	for _, t := range p.terms {
		terms = append(terms, t.String())
	}
	return strings.Join(terms, " and ")
}
