package multibuffer

import "math"

// BufferNeeds contains static methods,
// which estimate the optimal number of buffers
// to allocate for a scan.
type BufferNeeds struct{}

// BestRoot considers the various roots
// of the specified output size (in blocks),
// and returns the highest root that is less than
// the number of available buffers.
func (bn *BufferNeeds) BestRoot(available int, size int) int {
	avail := available - 2 // reserve a couple
	if avail <= 1 {
		return 1
	}
	k := int(^uint(0) >> 1) // equivalent to Integer.MAX_VALUE in Java
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(math.Pow(float64(size), 1/i)))
	}
	return k
}

// BestFactor considers the various factors
// of the specified output size (in blocks),
// and returns the highest factor that is less than
// the number of available buffers.
func (bn *BufferNeeds) BestFactor(available int, size int) int {
	avail := available - 2 // reserve a couple
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(float64(size) / i))
	}
	return k
}
