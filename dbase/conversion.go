package dbase

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
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
 *	#				Internal Column data type helper
 *	################################################################
 */

func (dbf *DBF) parseDate(raw []byte) (time.Time, error) {
	if string(raw) == strings.Repeat(" ", 8) {
		return time.Time{}, nil
	}
	return time.Parse("20060102", string(raw))
}

func (dbf *DBF) parseDateTime(raw []byte) (time.Time, error) {
	if len(raw) != 8 {
		return time.Time{}, fmt.Errorf("dbase-reader-parse-date-time-1:FAILED:%v", ERROR_INVALID.AsError())
	}
	julDat := int(binary.LittleEndian.Uint32(raw[:4]))
	mSec := int(binary.LittleEndian.Uint32(raw[4:]))

	// Determine year, month, day
	y, m, d := JD2YMD(julDat)
	if y < 0 || y > 9999 {
		return time.Time{}, nil
	}

	// Calculate whole seconds and use the remainder as nanosecond resolution
	nSec := mSec / 1000
	mSec = mSec - (nSec * 1000)

	// Create time using ymd and nanosecond timestamp
	return time.Date(y, time.Month(m), d, 0, 0, nSec, mSec*int(time.Millisecond), time.UTC), nil
}

// parses a string as byte array to int64
func (dbf *DBF) parseNumericInt(raw []byte) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return int64(0), nil
	}
	return strconv.ParseInt(trimmed, 10, 64)
}

// parses a string as byte array to float64
func (dbf *DBF) parseFloat(raw []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return float64(0.0), nil
	}
	return strconv.ParseFloat(strings.TrimSpace(string(trimmed)), 64)
}

// toUTF8String converts a byte slice to a UTF8 string using the converter
func (dbf *DBF) toUTF8String(raw []byte) (string, error) {
	utf8, err := dbf.convert.Decode(raw)
	if err != nil {
		return string(raw), fmt.Errorf("dbase-reader-to-utf8-string-1:FAILED:%v", err)
	}
	return string(utf8), nil
}
