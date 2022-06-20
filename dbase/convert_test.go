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

func TestToFloat64(t *testing.T) {
	if ToFloat64(123.456) != float64(123.456) {
		t.Errorf("Want %f, have %f", float64(123.456), ToFloat64(123.456))
	}
	if ToFloat64("123.456") != float64(0) {
		t.Errorf("Want %f, have %f", 0.0, ToFloat64(123.456))
	}
}

func TestToInt64(t *testing.T) {
	if ToInt64(int64(123456)) != int64(123456) {
		t.Errorf("Want %d, have %d", int64(123456), ToInt64(123456))
	}
	if ToInt64("123.456") != int64(0) {
		t.Errorf("Want %d, have %d", 0, ToInt64(123456))
	}
}

func TestToString(t *testing.T) {
	if ToString("Hêllo!") != "Hêllo!" {
		t.Errorf("Want %q, have %q", "Hêllo!", ToString("Hêllo!"))
	}
	if ToString(123.456) != "" {
		t.Errorf("Want %q, have %q", "", ToString(123.456))
	}
}

func TestToTrimmedString(t *testing.T) {
	if ToTrimmedString("Hêllo!      ") != "Hêllo!" {
		t.Errorf("Want %q, have %q", "Hêllo!", ToTrimmedString("Hêllo!    "))
	}
	if ToTrimmedString(123.456) != "" {
		t.Errorf("Want %q, have %q", "", ToTrimmedString(123.456))
	}
}

func TestToTime(t *testing.T) {
	now := time.Now()
	if ToTime(now).Equal(now) == false {
		t.Errorf("Want %v, have %v", now, ToTime(now))
	}
	if ToTime("123.456").IsZero() == false {
		t.Errorf("Want %v, have %v", time.Time{}, ToTime("123.456"))
	}
}

func TestToBool(t *testing.T) {
	if ToBool(true) == false {
		t.Error("Want true")
	}
	if ToBool(33) != false {
		t.Error("Want false")
	}
}
