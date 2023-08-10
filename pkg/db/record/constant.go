package record

type Constant struct {
	Val interface{}
}

func NewConstant(val interface{}) *Constant {
	return &Constant{Val: val}
}

func (c *Constant) AsInt() int {
	return c.Val.(int)
}

func (c *Constant) AsString() string {
	return c.Val.(string)
}
