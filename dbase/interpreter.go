package dbase

// At this moment not all FoxPro column types are supported.
// When reading column values, the value returned by this package is always `interface{}`.
//
// The supported column types with their return Go types are:
//
//	Column Type >> Column Type Name >> Golang type
//
//	B  >>  Double  >>  float64
//	C  >>  Character  >>  string
//	D  >>  Date  >>  time.Time
//	F  >>  Float  >>  float64
//	I  >>  Integer  >>  int32
//	L  >>  Logical  >>  bool
//	M  >>  Memo   >>  string
//	M  >>  Memo (Binary)  >>  []byte
//	N  >>  Numeric (0 decimals)  >>  int64
//	N  >>  Numeric (with decimals)  >>  float64
//	T  >>  DateTime  >>  time.Time
//	Y  >>  Currency  >>  float64
//
// This module contains the functions to convert a dbase database entry as byte array into a row struct
// with the columns converted into the corresponding data types.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Converts raw column data to the correct type for the given column
// For C and M columns a charset conversion is done
// For M columns the data is read from the memo file
func (dbf *DBF) dataToValue(raw []byte, column *Column) (interface{}, error) {
	// Not all column types have been implemented because we don't use them in our DBFs
	// Extend this function if needed
	if len(raw) != int(column.Length) {
		return nil, fmt.Errorf("dbase-interpreter-datatovalue-1:FAILED:invalid length %v Bytes != %v Bytes", len(raw), column.Length)
	}
	switch column.Type() {
	case "M":
		// M values contain the address in the FPT file from where to read data
		memo, isText, err := dbf.parseMemo(raw)
		if isText {
			if err != nil {
				return string(memo), fmt.Errorf("dbase-interpreter-datatovalue-2:FAILED:%w", err)
			}
			return string(memo), nil
		}
		return memo, nil
	case "C":
		// C values are stored as strings, the returned string is not trimmed
		str, err := toUTF8String(raw, dbf.converter)
		if err != nil {
			return str, fmt.Errorf("dbase-interpreter-datatovalue-4:FAILED:%w", err)
		}
		return str, nil
	case "I":
		// I values are stored as numeric values
		return int32(binary.LittleEndian.Uint32(raw)), nil
	case "B":
		// B (double) values are stored as numeric values
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
	case "D":
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		date, err := parseDate(raw)
		if err != nil {
			return date, fmt.Errorf("dbase-interpreter-datatovalue-5:FAILED:%w", err)
		}
		return date, nil
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		dateTime, err := parseDateTime(raw)
		if err != nil {
			return dateTime, fmt.Errorf("dbase-interpreter-datatovalue-6:FAILED:%w", err)
		}
		return dateTime, nil
	case "L":
		// L values are stored as strings T or F, we only check for T, the rest is false...
		return string(raw) == "T", nil
	case "V":
		// V values just return the raw value
		return raw, nil
	case "Y":
		// Y values are currency values stored as ints with 4 decimal places
		return float64(binary.LittleEndian.Uint64(raw)) / 10000, nil
	case "N":
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		if column.Decimals == 0 {
			i, err := parseNumericInt(raw)
			if err != nil {
				return i, fmt.Errorf("dbase-interpreter-datatovalue-7:FAILED:%w", err)
			}
			return i, nil
		}
		fallthrough // same as "F"
	case "F":
		// F values are stored as string values
		f, err := parseFloat(raw)
		if err != nil {
			return f, fmt.Errorf("dbase-interpreter-datatovalue-8:FAILED:%w", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("dbase-interpreter-datatovalue-9:FAILED:Unsupported column data type: %s", column.Type())
	}
}

// Converts column data to the byte representation
// For M values the data has to be written to the memo file
func (dbf *DBF) valueToData(field *Field) ([]byte, error) {
	switch field.Type() {
	case "M":
		// M (string) values are stored in the FPT (memo) file and the address is stored in the DBF
		memo := make([]byte, 0)
		txt := false
		s, sok := field.value.(string)
		if sok {
			memo = []byte(s)
			txt = true
		}
		m, ok := field.value.([]byte)
		if ok {
			memo = m
			txt = false
		}
		if !ok && !sok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-1:FAILED:Invalid type for memo field: %T", field.value)
		}
		// Write the memo to the memo file
		address, err := dbf.writeMemo(memo, txt, len(memo))
		if err != nil {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-2:FAILED:%w", err)
		}
		return address, nil
	case "C":
		// C values are stored as strings, the returned string is not trimmed
		c, ok := field.value.(string)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-1:FAILED:invalid data type %T, expected string on column field: %v", field.value, field.Name())
		}
		raw := make([]byte, field.column.Length)
		bin, err := fromUtf8String([]byte(c), dbf.converter)
		if err != nil {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-2:FAILED:%w", err)
		}
		bin = appendSpaces(bin, int(field.column.Length))
		copy(raw, bin)
		if len(raw) > int(field.column.Length) {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-3:FAILED:invalid length %v Bytes > %v Bytes", len(raw), field.column.Length)
		}
		return raw, nil
	case "I":
		// I values (int32)
		i, ok := field.value.(int32)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-4:FAILED:invalid data type %T, expected int32", field.value)
		}
		raw := make([]byte, field.column.Length)
		bin, err := toBinary(i)
		if err != nil {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-5:FAILED:%w", err)
		}
		copy(raw, bin)
		if len(raw) != int(field.column.Length) {
			return nil, fmt.Errorf("dbase-interpreter-datatovalue-6:FAILED:invalid length %v Bytes != %v Bytes", len(raw), field.column.Length)
		}
		return raw, nil
	case "Y":
		// Y (currency)
		fallthrough // same as "B"
	case "F":
		// F (Float)
		fallthrough // same as "B"
	case "B":
		// B (double)
		b, ok := field.value.(float64)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-7:FAILED:invalid data type %T, expected float64", field.value)
		}
		var bin []byte
		if b == float64(int64(b)) {
			// if the value is an integer, store as integer
			bin = []byte(fmt.Sprintf("%d", int64(b)))
		} else {
			// if the value is a float, store as float
			expression := fmt.Sprintf("%%.%df", field.column.Decimals)
			bin = []byte(fmt.Sprintf(expression, field.value))
		}
		return prependSpaces(bin, int(field.column.Length)), nil
	case "D":
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		d, ok := field.value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-10:FAILED:invalid data type %T, expected time.Time", field.value)
		}
		raw := make([]byte, field.column.Length)
		bin := []byte(d.Format("20060102"))
		copy(raw, bin)
		if len(raw) != int(field.column.Length) {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-12:FAILED:invalid length %v Bytes != %v Bytes", len(raw), field.column.Length)
		}
		return raw, nil
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		t, ok := field.value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-13:FAILED:invalid data type %T, expected time.Time", field.value)
		}
		raw := make([]byte, 8)
		i := YMD2JD(t.Year(), int(t.Month()), t.Day())
		date, err := toBinary(uint64(i))
		if err != nil {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-14:FAILED:%w", err)
		}
		copy(raw[:4], date)
		millis := t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000
		time, err := toBinary(uint64(millis))
		if err != nil {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-15:FAILED:%w", err)
		}
		copy(raw[4:], time)
		if len(raw) != int(field.column.Length) {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-16:FAILED:invalid length %v Bytes != %v Bytes", len(raw), field.column.Length)
		}
		return raw, nil
	case "L":
		// L (bool) values are stored as strings T or F, we only check for T, the rest is false...
		l, ok := field.value.(bool)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-17:FAILED:invalid data type %T, expected bool", field.value)
		}
		raw := []byte("F")
		if l {
			return []byte("T"), nil
		}
		return raw, nil
	case "V":
		// V values just return the raw value
		raw, ok := field.value.([]byte)
		if !ok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-18:FAILED:invalid data type %T, expected []byte", field.value)
		}
		return raw, nil
	case "N":
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		bin := make([]byte, 0)
		f, fok := field.value.(float64)
		if fok {
			if f == float64(int64(f)) {
				// if the value is an integer, store as integer
				bin = []byte(fmt.Sprintf("%d", int64(f)))
			} else {
				// if the value is a float, store as float
				expression := fmt.Sprintf("%%.%df", field.column.Decimals)
				bin = []byte(fmt.Sprintf(expression, field.value))
			}
		}
		_, iok := field.value.(int64)
		if iok {
			bin = []byte(fmt.Sprintf("%d", field.value))
		}

		if !iok && !fok {
			return nil, fmt.Errorf("dbase-interpreter-valuetodata-19:FAILED:invalid data type %T, expected int64 or float64", field.value)
		}
		return prependSpaces(bin, int(field.column.Length)), nil
	default:
		return nil, fmt.Errorf("dbase-interpreter-valuetodata-22:FAILED:Unsupported column data type: %s", field.column.Type())
	}
}

func toBinary(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil, fmt.Errorf("dbase-interpreter-tobinary-1:FAILED:%w", err)
	}
	return buf.Bytes(), nil
}
