package dbase

import (
	"testing"
)

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
