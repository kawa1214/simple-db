package query

import "github.com/kawa1214/simple-db/pkg/db/record"

// Expression corresponds to SQL expressions.
type Expression struct {
	val     *record.Constant
	fldname *string
}

// NewConstantExpression creates a new expression with a constant value.
func NewConstantExpression(val *record.Constant) *Expression {
	return &Expression{val: val}
}

// NewFieldExpression creates a new expression with a field name.
func NewFieldExpression(fldname string) *Expression {
	return &Expression{fldname: &fldname}
}

// Evaluate evaluates the expression with respect to the current record of the specified scan.
func (e *Expression) Evaluate(s Scan) *record.Constant {
	if e.val != nil {
		return e.val
	}
	return s.GetVal(*e.fldname)
}

// IsFieldName returns true if the expression is a field reference.
func (e *Expression) IsFieldName() bool {
	return e.fldname != nil
}

// AsConstant returns the constant corresponding to a constant expression, or nil if the expression does not denote a constant.
func (e *Expression) AsConstant() *record.Constant {
	return e.val
}

// AsFieldName returns the field name corresponding to a constant expression, or an empty string if the expression does not denote a field.
func (e *Expression) AsFieldName() string {
	if e.fldname != nil {
		return *e.fldname
	}
	return ""
}

// AppliesTo determines if all of the fields mentioned in this expression are contained in the specified schema.
func (e *Expression) AppliesTo(sch *record.Schema) bool {
	if e.val != nil {
		return true
	}
	return sch.HasField(*e.fldname)
}

// ToString returns the string representation of the expression.
func (e *Expression) ToString() string {
	if e.val != nil {
		return e.val.ToString()
	}
	return *e.fldname
}
