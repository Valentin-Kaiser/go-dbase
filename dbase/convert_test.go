package dbase

import (
	"testing"
	"time"
)

func TestYMD2JD(t *testing.T) {
	want := 2453738
	have := YMD2JD(2006, 1, 2)
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}
}

func TestJD2YMD(t *testing.T) {
	want := []int{2006, 1, 2}
	y, m, d := JD2YMD(2453738)
	if want[0] != y || want[1] != m || want[2] != d {
		t.Errorf("Want %v-%v-%v, have %v-%v-%v", want[0], want[1], want[2], y, m, d)
	}
}

func TestJDToNumber(t *testing.T) {
	want := 2453738
	have, err := JDToNumber("2006-1-2")
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}
}

func TestJDToDate(t *testing.T) {
	want := time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
	have, err := JDToDate(2453738)
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}
}

func TestWin1250Converter_Decode(t *testing.T) {
	dec := new(Win1250Converter)
	in := []byte{0xC4, 0xF5}
	b, err := dec.Decode(in)
	if err != nil {
		t.Fatalf("error in decode: %s", err)
	}
	want := "Äő"
	if string(b) != want {
		t.Errorf("Want %s, have %s", want, string(b))
	}
}

func TestWin1250Converter_Encode(t *testing.T) {
	c := new(Win1250Converter)
	in := []byte("Äő")
	want := []byte{0xC4, 0xF5}

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("error in encode: %s", err)
	}

	if string(want) != string(b) {
		t.Errorf("Want %s, have %s", want, string(b))
	}
}
