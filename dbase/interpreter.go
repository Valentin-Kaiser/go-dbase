package dbase

import (
	"encoding/binary"
	"fmt"
	"math"
	"syscall"
)

// Converts raw field data to the correct type for the given field
// For C and M fields a charset conversion is done
// For M fields the data is read from the memo file
func (dbf *DBF) FieldToValue(raw []byte, fieldPosition int) (interface{}, error) {
	// Not all field types have been implemented because we don't use them in our DBFs
	// Extend this function if needed
	if fieldPosition < 0 || len(dbf.table.fields) < fieldPosition {
		return nil, fmt.Errorf("dbase-reader-field-to-value-1:FAILED:%v", ERROR_INVALID.AsError())
	}

	switch dbf.table.fields[fieldPosition].FieldType() {
	case "M":
		// M values contain the address in the FPT file from where to read data
		memo, isText, err := dbf.parseMemo(raw)
		if isText {
			if err != nil {
				return string(memo), fmt.Errorf("dbase-reader-field-to-value-2:FAILED:%v", err)
			}
			return string(memo), nil
		}
		return memo, nil
	case "C":
		// C values are stored as strings, the returned string is not trimmed
		str, err := dbf.toUTF8String(raw)
		if err != nil {
			return str, fmt.Errorf("dbase-reader-field-to-value-4:FAILED:%v", err)
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
			return date, fmt.Errorf("dbase-reader-field-to-value-5:FAILED:%v", err)
		}
		return date, nil
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		dateTime, err := dbf.parseDateTime(raw)
		if err != nil {
			return dateTime, fmt.Errorf("dbase-reader-field-to-value-6:FAILED:%v", err)
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
		return float64(float64(binary.LittleEndian.Uint64(raw)) / 10000), nil
	case "N":
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		if dbf.table.fields[fieldPosition].Decimals == 0 {
			i, err := dbf.parseNumericInt(raw)
			if err != nil {
				return i, fmt.Errorf("dbase-reader-field-to-value-7:FAILED:%v", err)
			}
			return i, nil
		}
		fallthrough // same as "F"
	case "F":
		// F values are stored as string values
		f, err := dbf.parseFloat(raw)
		if err != nil {
			return f, fmt.Errorf("dbase-reader-field-to-value-8:FAILED:%v", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("dbase-reader-field-to-value-9:FAILED:Unsupported fieldtype: %s", dbf.table.fields[fieldPosition].FieldType())
	}
}

// Returns if the record at recordPosition is deleted
func (dbf *DBF) DeletedAt(recordPosition uint32) (bool, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return false, fmt.Errorf("dbase-reader-deleted-at-1:FAILED:%v", ERROR_EOF.AsError())
	}

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)), 0)
	if err != nil {
		return false, fmt.Errorf("dbase-reader-deleted-at-2:FAILED:%v", err)
	}

	buf := make([]byte, 1)
	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return false, fmt.Errorf("dbase-reader-deleted-at-3:FAILED:%v", err)
	}
	if read != 1 {
		return false, fmt.Errorf("dbase-reader-deleted-at-4:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf[0] == 0x2A, nil
}

// Returns if the record at the internal record pos is deleted
func (dbf *DBF) Deleted() (bool, error) {
	return dbf.DeletedAt(dbf.table.recordPointer)
}

// Converts raw record data to a Record struct
// If the data points to a memo (FPT) file this file is also read
func (dbf *DBF) bytesToRecord(data []byte) (*Record, error) {
	rec := &Record{}
	rec.DBF = dbf

	// a record should start with te delete flag, a space (0x20) or * (0x2A)
	rec.Deleted = data[0] == 0x2A
	if !rec.Deleted && data[0] != 0x20 {
		return nil, fmt.Errorf("dbase-reader-bytes-to-record-1:FAILED:invalid record data, no delete flag found at beginning of record")
	}

	rec.Data = make([]interface{}, dbf.FieldsCount())

	offset := uint16(1) // deleted flag already read
	for i := 0; i < len(rec.Data); i++ {
		fieldinfo := dbf.table.fields[i]
		val, err := dbf.FieldToValue(data[offset:offset+uint16(fieldinfo.Length)], i)
		if err != nil {
			return rec, fmt.Errorf("dbase-reader-bytes-to-record-2:FAILED:%v", err)
		}
		rec.Data[i] = val

		offset += uint16(fieldinfo.Length)
	}

	return rec, nil
}
