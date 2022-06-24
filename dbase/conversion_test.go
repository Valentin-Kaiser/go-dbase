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
	want := time.Date(2020, 6, 7, 0, 0, 0, 0, time.UTC)
	have, err := dbf.parseDate([]byte("20200607"))
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}

	_, err = dbf.parseDate([]byte("2607"))
	if err == nil {
		t.Errorf("Wanted error %v", err)
	}
}

func TestParseDateTime(t *testing.T) {
	dbf = &DBF{}
	want := time.Date(2022, 6, 20, 12, 0, 0, 0, time.UTC)
	have, err := dbf.parseDateTime([]byte{0x67, 0x88, 0x25, 0x00, 0x00, 0x2e, 0x93, 0x02})
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}

	_, err = dbf.parseDate([]byte{})
	if err == nil {
		t.Errorf("Wanted error %v", err)
	}
}

func TestParseNumericInt(t *testing.T) {
	dbf = &DBF{}
	want := int64(65536)
	have, err := dbf.parseNumericInt([]byte("65536"))
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}

	_, err = dbf.parseNumericInt([]byte("abcd"))
	if err == nil {
		t.Errorf("Wanted error %v", err)
	}
}

func TestParseFloat(t *testing.T) {
	dbf = &DBF{}
	want := float64(65536.1024)
	have, err := dbf.parseFloat([]byte("65536.1024"))
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}

	_, err = dbf.parseFloat([]byte("abcd"))
	if err == nil {
		t.Errorf("Wanted error %v", err)
	}
}

func TestToUTF8String(t *testing.T) {
	dbf = &DBF{
		convert: &Win1250Converter{},
	}

	want := "Äő"
	have, err := dbf.toUTF8String([]byte{0xC4, 0xF5})
	if err != nil {
		t.Errorf("Want %v, have %v with error %v", want, have, err.Error())
	}
	if want != have {
		t.Errorf("Want %v, have %v", want, have)
	}
}
