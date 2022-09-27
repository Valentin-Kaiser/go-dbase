package dbase

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Convert year, month and day to a julian day number
// Julian day number -> days since 01-01-4712 BC
func YMD2JD(y, m, d int) int {
	return d - 32075 +
		1461*(y+4800+(m-14)/12)/4 +
		367*(m-2-(m-14)/12*12)/12 -
		3*((y+4900+(m-14)/12)/100)/4
}

// Convert julian day number to year, month and day
// Julian day number -> days since 01-01-4712 BC
func JD2YMD(date int) (int, int, int) {
	l := date + 68569
	n := 4 * l / 146097
	l -= (146097*n + 3) / 4
	y := 4000 * (l + 1) / 1461001
	l = l - 1461*y/4 + 31
	m := 80 * l / 2447
	d := l - 2447*m/80
	l = m / 11
	m = m + 2 - 12*l
	y = 100*(n-49) + y + l
	return y, m, d
}

// Convert julian day number to golang time.Time
// Julian day number -> days since 01-01-4712 BC
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
	t, err := time.Parse("2006-01-02", ys+"-"+ms+"-"+ds)
	if err != nil {
		return t, fmt.Errorf("dbase-conversion-jdtodate-1:FAILED:%w", err)
	}
	return t, nil
}

/**
 *	################################################################
 *	#				Internal Column data type helper
 *	################################################################
 */

// parseDate parses a date string from a byte slice and returns a time.Time
func (dbf *DBF) parseDate(raw []byte) (time.Time, error) {
	if string(raw) == strings.Repeat(" ", 8) {
		return time.Time{}, nil
	}
	t, err := time.Parse("20060102", string(raw))
	if err != nil {
		return t, fmt.Errorf("dbase-interpreter-parsedate-1:FAILED:%w", err)
	}
	return t, nil
}

// parseDateTIme parses a date and time string from a byte slice and returns a time.Time
func (dbf *DBF) parseDateTime(raw []byte) (time.Time, error) {
	if len(raw) != 8 {
		return time.Time{}, fmt.Errorf("dbase-conversion-parsedate-1:FAILED:%v", InvalidPosition)
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
	mSec -= (nSec * 1000)
	// Create time using ymd and nanosecond timestamp
	return time.Date(y, time.Month(m), d, 0, 0, nSec, mSec*int(time.Millisecond), time.UTC), nil
}

// parseNumericInt parses a string as byte array to int64
func (dbf *DBF) parseNumericInt(raw []byte) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return int64(0), nil
	}
	i, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return i, fmt.Errorf("dbase-conversion-parseint-1:FAILED:%w", err)
	}
	return i, nil
}

// parseFloat parses a string as byte array to float64
func (dbf *DBF) parseFloat(raw []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return float64(0), nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
	if err != nil {
		return f, fmt.Errorf("dbase-conversion-parsefloat-1:FAILED:%w", err)
	}
	return f, nil
}

// toUTF8String converts a byte slice to a UTF8 string using the converter
func (dbf *DBF) toUTF8String(raw []byte) (string, error) {
	utf8, err := dbf.convert.Decode(raw)
	if err != nil {
		return string(raw), fmt.Errorf("dbase-conversion-toutf8string-1:FAILED:%w", err)
	}
	return string(utf8), nil
}
