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
package dbase

import (
	"encoding/binary"
	"fmt"
	"math"
	"syscall"
)

// Converts raw column data to the correct type for the given column
// For C and M columns a charset conversion is done
// For M columns the data is read from the memo file
func (dbf *DBF) DataToValue(raw []byte, column *Column) (interface{}, error) {
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
		str, err := dbf.toUTF8String(raw)
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
		date, err := dbf.parseDate(raw)
		if err != nil {
			return date, fmt.Errorf("dbase-interpreter-datatovalue-5:FAILED:%w", err)
		}
		return date, nil
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		dateTime, err := dbf.parseDateTime(raw)
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
			i, err := dbf.parseNumericInt(raw)
			if err != nil {
				return i, fmt.Errorf("dbase-interpreter-datatovalue-7:FAILED:%w", err)
			}
			return i, nil
		}
		fallthrough // same as "F"
	case "F":
		// F values are stored as string values
		f, err := dbf.parseFloat(raw)
		if err != nil {
			return f, fmt.Errorf("dbase-interpreter-datatovalue-8:FAILED:%w", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("dbase-interpreter-datatovalue-9:FAILED:Unsupported column data type: %s", column.Type())
	}
}

// Returns if the row at internal row pointer is deleted
func (dbf *DBF) Deleted() (bool, error) {
	if dbf.table.rowPointer >= dbf.header.RowsCount {
		return false, fmt.Errorf("dbase-interpreter-deleted-1:FAILED:%v", EOF)
	}
	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.header.FirstRow)+(int64(dbf.table.rowPointer)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-2:FAILED:%w", err)
	}
	buf := make([]byte, 1)
	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-3:FAILED:%w", err)
	}
	if read != 1 {
		return false, fmt.Errorf("dbase-interpreter-deleted-4:FAILED:%v", Incomplete)
	}
	return buf[0] == Deleted, nil
}
