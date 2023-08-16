package file

import "testing"

func TestEqual(t *testing.T) {
	b1 := NewBlockID("a", 1)
	b2 := NewBlockID("a", 1)
	if !b1.Equal(b2) {
		t.Errorf("b1 and b2 should be equal")
	}
	b3 := NewBlockID("a", 2)
	if b1.Equal(b3) {
		t.Errorf("b1 and b3 should not be equal")
	}
	b4 := NewBlockID("b", 1)
	if b1.Equal(b4) {
		t.Errorf("b1 and b4 should not be equal")
	}
}

func TestString(t *testing.T) {
	b := NewBlockID("a", 1)
	if b.String() != "[file a, block 1]" {
		t.Errorf("b.String() should be [file a, block 1]")
	}
}

func TestNew(t *testing.T) {
	b := NewBlockID("a", 1)
	if b.filename != "a" {
		t.Errorf("b.FileName() should be a")
	}
	if b.blknum != 1 {
		t.Errorf("b.Number() should be 1")
	}
}

func TestFileName(t *testing.T) {
	b := NewBlockID("a", 1)
	if b.FileName() != "a" {
		t.Errorf("b.FileName() should be a")
	}
}

func TestNumber(t *testing.T) {
	b := NewBlockID("a", 1)
	if b.Number() != 1 {
		t.Errorf("b.Number() should be 1")
	}
}
