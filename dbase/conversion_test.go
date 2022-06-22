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

func TestParseDate(t *testing.T) {

}

func TestParseDateTime(t *testing.T) {

}

func TestParseNumericInt(t *testing.T) {

}

func TestParseFloat(t *testing.T) {

}

func TestToUTF8String(t *testing.T) {

}
