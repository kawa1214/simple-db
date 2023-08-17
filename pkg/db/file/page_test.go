package file

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestNewPage(t *testing.T) {
	p := NewPage(400)
	if p.bb.Len() != 400 {
		t.Errorf("p.bb.Len() should be 400")
	}
	if p.charset != DEFAULT_CHARSET {
		t.Errorf("p.charset should be US-ASCII")
	}

}

func TestNewPageFromBytes(t *testing.T) {
	p := NewPageFromBytes([]byte("hello"))
	if p.bb.Len() != 5 {
		t.Errorf("p.bb.Len() should be 5")
	}
	if p.charset != DEFAULT_CHARSET {
		t.Errorf("p.charset should be US-ASCII")
	}
	if p.bb.String() != "hello" {
		t.Errorf("p.bb.String() should be hello")
	}
}

func TestGetInt(t *testing.T) {
	testCases := []struct {
		name   string
		val    int
		offset int
		want   func(t *testing.T, p *Page)
	}{
		{
			name:   "offset 0",
			val:    1,
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.GetInt(0) != 1 {
					t.Errorf("p.GetInt(0) should be 1, but %d", p.GetInt(0))
				}
			},
		},
		{
			name:   "offset 4",
			val:    1,
			offset: 4,
			want: func(t *testing.T, p *Page) {
				if p.GetInt(4) != 1 {
					t.Errorf("p.GetInt(4) should be 1, but %d", p.GetInt(4))
				}
			},
		},
		{
			name:   "max",
			val:    int(math.MaxUint32),
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.GetInt(0) != int(math.MaxUint32) {
					t.Errorf("p.GetInt(0) should be %d, but %d", int(math.MaxUint32), p.GetInt(0))
				}
			},
		},
		{
			name:   "max+1",
			val:    int(math.MaxUint32) + 1,
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.GetInt(0) != 0 {
					t.Errorf("p.GetInt(0) should be %d, but %d", 0, p.GetInt(0))
				}
			},
		},
		{
			name:   "min",
			val:    int(math.MinInt32),
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.GetInt(0) != int(math.MinInt32)*-1 {
					t.Errorf("p.GetInt(0) should be %d, but %d", int(math.MinInt32)*-1, p.GetInt(0))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, 4)
			binary.BigEndian.PutUint32(data, uint32(tc.val))
			p := NewPage(400)
			copy(p.bb.Bytes()[tc.offset:], data)

			tc.want(t, p)
		})
	}

}

func TestSetInt(t *testing.T) {
	testCases := []struct {
		name   string
		val    int
		offset int
		want   func(t *testing.T, p *Page)
	}{
		{
			name:   "offset 0",
			val:    1,
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.bb.Bytes()[0] != 0 || p.bb.Bytes()[1] != 0 || p.bb.Bytes()[2] != 0 || p.bb.Bytes()[3] != 1 {
					t.Errorf("p.bb.Bytes()[0:4] should be [0,0,0,1], but %v", p.bb.Bytes()[0:4])
				}
			},
		},
		{
			name:   "offset 4",
			val:    1,
			offset: 4,
			want: func(t *testing.T, p *Page) {
				if p.bb.Bytes()[4] != 0 || p.bb.Bytes()[5] != 0 || p.bb.Bytes()[6] != 0 || p.bb.Bytes()[7] != 1 {
					t.Errorf("p.bb.Bytes()[0:4] should be [0,0,0,1], but %v", p.bb.Bytes()[4:4+4])
				}
			},
		},
		{
			name:   "max",
			val:    int(math.MaxUint32),
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.bb.Bytes()[0] != 255 || p.bb.Bytes()[1] != 255 || p.bb.Bytes()[2] != 255 || p.bb.Bytes()[3] != 255 {
					t.Errorf("p.bb.Bytes()[0:4] should be [255,255,255,255], but %v", p.bb.Bytes()[0:4])
				}
			},
		},
		{
			name:   "max+1",
			val:    int(math.MaxUint32) + 1,
			offset: 0,
			want: func(t *testing.T, p *Page) {
				if p.bb.Bytes()[0] != 0 || p.bb.Bytes()[1] != 0 || p.bb.Bytes()[2] != 0 || p.bb.Bytes()[3] != 0 {
					t.Errorf("p.bb.Bytes()[0:4] should be [0,0,0,0], but %v", p.bb.Bytes()[0:4])
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPage(400)
			p.SetInt(tc.offset, tc.val)

			tc.want(t, p)
		})
	}
}

func TestGetBytes(t *testing.T) {
	p := NewPage(400)
	copy(p.bb.Bytes()[0:], []byte{0, 0, 0, 4, 1, 2, 3, 4})
	if string(p.GetBytes(0)) != string([]byte{1, 2, 3, 4}) {
		t.Errorf("p.GetBytes(0) should be [1,2,3,4], but %v", p.GetBytes(0))
	}
}

func TestSetBytes(t *testing.T) {
	p := NewPage(400)
	p.SetBytes(0, []byte{1, 2, 3, 4})
	if string(p.bb.Bytes()[0:8]) != string([]byte{0, 0, 0, 4, 1, 2, 3, 4}) {
		t.Errorf("p.bb.Bytes()[0:] should be [0,0,0,4,1,2,3,4], but %v", p.bb.Bytes()[0:8])
	}
}

func TestGetString(t *testing.T) {
	str := "hello"

	p := NewPage(400)
	bytes := []byte(str)
	l := len(bytes)
	copy(p.bb.Bytes()[0:], []byte{0, 0, 0, byte(l)})
	copy(p.bb.Bytes()[4:], bytes)
	if p.GetString(0) != "hello" {
		t.Errorf("p.GetString(0) should be hello, but %v", p.GetString(0))
	}
}

func TestSetString(t *testing.T) {
	str := "hello"

	p := NewPage(400)
	l := len([]byte(str))
	p.SetString(0, str)
	if string(p.bb.Bytes()[4:4+l]) != str {
		t.Errorf("p.bb.Bytes()[0:] should be hello, but %v", p.bb.Bytes()[4:4+l])
	}
}

func TestMaxLength(t *testing.T) {
	str := "hello"
	l := len(str)
	if MaxLength(l) != l*4+4 {
		t.Errorf("p.MaxLength() should be 20, but %d", MaxLength(l))
	}
}
