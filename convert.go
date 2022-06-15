package dbase

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// convert year, month and day to a julian day number
// julian day number -> days since 01-01-4712 BC
func YMD2JD(y, m, d int) int {
	return d - 32075 +
		1461*(y+4800+(m-14)/12)/4 +
		367*(m-2-(m-14)/12*12)/12 -
		3*((y+4900+(m-14)/12)/100)/4
}

// convert julian day number to year, month and day
// julian day number -> days since 01-01-4712 BC
func JD2YMD(date int) (int, int, int) {
	l := date + 68569
	n := 4 * l / 146097
	l = l - (146097*n+3)/4
	y := 4000 * (l + 1) / 1461001
	l = l - 1461*y/4 + 31
	m := 80 * l / 2447
	d := l - 2447*m/80
	l = m / 11
	m = m + 2 - 12*l
	y = 100*(n-49) + y + l
	return y, m, d
}

// convert year, month and day delimited by hyphen to a julian day number
// julian day number -> days since 01-01-4712 BC
func JDToNumber(date string) (int, error) {
	dateString := strings.FieldsFunc(date, unicode.IsPunct)
	if len(dateString) != 3 {
		return 0, errors.New("expected 3 punctuation delimited fields.")
	}
	y, ei := strconv.Atoi(dateString[0])
	m, ej := strconv.Atoi(dateString[1])
	d, ek := strconv.Atoi(dateString[2])
	if ei != nil || ej != nil || ek != nil {
		return 0, errors.New("expected 3 integers")
	}
	return YMD2JD(y, m, d), nil
}

// convert julian day number to golang time.Time
// julian day number -> days since 01-01-4712 BC
func JDToDate(number int) (time.Time, error) {
	y, m, d := JD2YMD(number)
	ys := strconv.Itoa(y)
	ms := strconv.Itoa(m)
	ds := strconv.Itoa(d)

	if m < 10 {
		ms = "0" + ms
	}

	if d < 10 {
		ds = "0" + ds
	}

	return time.Parse("2006-01-02", ys+"-"+ms+"-"+ds)
}

/**
 *	################################################################
 *	#		casting helper functions for field values
 *	################################################################
 */

// ToString always returns a string
func ToString(in interface{}) string {
	if str, ok := in.(string); ok {
		return str
	}
	return ""
}

// ToTrimmedString always returns a string with spaces trimmed
func ToTrimmedString(in interface{}) string {
	if str, ok := in.(string); ok {
		return strings.TrimSpace(str)
	}
	return ""
}

// ToInt64 always returns an int64
func ToInt64(in interface{}) int64 {
	if i, ok := in.(int64); ok {
		return i
	}
	return 0
}

// ToFloat64 always returns a float64
func ToFloat64(in interface{}) float64 {
	if f, ok := in.(float64); ok {
		return f
	}
	return 0.0
}

// ToTime always returns a time.Time
func ToTime(in interface{}) time.Time {
	if t, ok := in.(time.Time); ok {
		return t
	}
	return time.Time{}
}

// ToBool always returns a boolean
func ToBool(in interface{}) bool {
	if b, ok := in.(bool); ok {
		return b
	}
	return false
}
