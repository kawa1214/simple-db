package query

// // Constant denotes values stored in the database.
// type Constant struct {
// 	ival *int
// 	sval *string
// }

// // NewIntConstant creates a new constant with an integer value.
// func NewIntConstant(ival int) *Constant {
// 	return &Constant{ival: &ival}
// }

// // NewStringConstant creates a new constant with a string value.
// func NewStringConstant(sval string) *Constant {
// 	return &Constant{sval: &sval}
// }

// // AsInt returns the integer value of the constant.
// func (c *Constant) AsInt() int {
// 	if c.ival != nil {
// 		return *c.ival
// 	}
// 	return 0 // or panic/error if you want to handle it strictly
// }

// // AsString returns the string value of the constant.
// func (c *Constant) AsString() string {
// 	if c.sval != nil {
// 		return *c.sval
// 	}
// 	return "" // or panic/error if you want to handle it strictly
// }

// // Equals checks if two constants are equal.
// func (c *Constant) Equals(other *Constant) bool {
// 	if c.ival != nil && other.ival != nil {
// 		return *c.ival == *other.ival
// 	}
// 	if c.sval != nil && other.sval != nil {
// 		return *c.sval == *other.sval
// 	}
// 	return false
// }

// // CompareTo compares two constants.
// func (c *Constant) CompareTo(other *Constant) int {
// 	if c.ival != nil && other.ival != nil {
// 		if *c.ival == *other.ival {
// 			return 0
// 		} else if *c.ival < *other.ival {
// 			return -1
// 		} else {
// 			return 1
// 		}
// 	}

// 	if c.sval != nil && other.sval != nil {
// 		if *c.sval == *other.sval {
// 			return 0
// 		} else if *c.sval < *other.sval {
// 			return -1
// 		} else {
// 			return 1
// 		}
// 	}

// 	return 0 // or panic/error if you want to handle it strictly
// }

// // HashCode returns the hash code of the constant.
// func (c *Constant) HashCode() int {
// 	if c.ival != nil {
// 		return *c.ival // This is a simplistic hash for an integer, consider using a better hash function
// 	}
// 	if c.sval != nil {
// 		return len(*c.sval) // This is a simplistic hash for a string, consider using a better hash function
// 	}
// 	return 0
// }

// // ToString returns the string representation of the constant.
// func (c *Constant) ToString() string {
// 	if c.ival != nil {
// 		return fmt.Sprint(*c.ival)
// 	}
// 	if c.sval != nil {
// 		return *c.sval
// 	}
// 	return ""
// }
