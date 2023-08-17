package file

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

type Page struct {
	bb      *bytes.Buffer
	charset string
}

const DEFAULT_CHARSET = "US-ASCII"

func NewPage(blocksize int) *Page {
	return &Page{
		bb:      bytes.NewBuffer(make([]byte, blocksize)),
		charset: DEFAULT_CHARSET,
	}
}

func NewPageFromBytes(b []byte) *Page {
	return &Page{
		bb:      bytes.NewBuffer(b),
		charset: DEFAULT_CHARSET,
	}
}

func (p *Page) GetInt(offset int) int {
	data := p.bb.Bytes()[offset : offset+4]
	val := binary.BigEndian.Uint32(data)
	return int(val)
}

func (p *Page) SetInt(offset int, n int) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(n))
	copy(p.bb.Bytes()[offset:], data)
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	return p.bb.Bytes()[offset+4 : offset+4+length]
}

func (p *Page) SetBytes(offset int, b []byte) {
	p.SetInt(offset, len(b))
	copy(p.bb.Bytes()[offset+4:], b)
}

func (p *Page) GetString(offset int) string {
	b := p.GetBytes(offset)
	return string(b)
}

func (p *Page) SetString(offset int, s string) {
	b := []byte(s)
	p.SetBytes(offset, b)
}

func MaxLength(strlen int) int {
	bytesPerChar := utf8.UTFMax
	return 4 + strlen*bytesPerChar
}

func (p *Page) Contents() *bytes.Buffer {
	return p.bb
}
