package dbase

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
// This package contains the functions to convert a dbase database entry as byte array into a row struct
// with the columns converted into the corresponding data types.
func (dbf *DBF) dataToValue(raw []byte, column *Column) (interface{}, error) {
	// Not all column types have been implemented because we don't use them in our DBFs
	// Extend this function if needed
	if len(raw) != int(column.Length) {
		return nil, newError("dbase-interpreter-datatovalue-1", fmt.Errorf("invalid length %v Bytes != %v Bytes at column field: %v", len(raw), column.Length, column.Name()))
	}
	switch column.DataType {
	case Memo:
		// M values contain the address in the FPT file from where to read data
		return dbf.parseMValue(raw, column)
	case Character:
		// C values are stored as strings, the returned string is not trimmed
		return dbf.parseCValue(raw, column)
	case Integer:
		// I values are stored as numeric values
		return dbf.parseIValue(raw)
	case Double:
		// B (double) values are stored as numeric values
		return dbf.parseBValue(raw)
	case Date:
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		return dbf.parseDValue(raw, column)
	case DateTime:
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		return dbf.parseTValue(raw, column)
	case Logical:
		// L values are stored as strings T or F, we only check for T, the rest is false...
		return dbf.parseLValue(raw)
	case Currency:
		// Y values are currency values stored as ints with 4 decimal places
		return dbf.parseYValue(raw)
	case Numeric:
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		return dbf.parseNValue(raw, column)
	case Float:
		// F values are stored as string values
		return dbf.parseFValue(raw, column)
	case Varbinary:
		// Q values just return the raw value
		fallthrough
	case Varchar:
		// V values just return the raw value
		fallthrough
	case Blob:
		// W values just return the raw value
		fallthrough
	case Picture:
		// P values just return the raw value
		fallthrough
	case General:
		// G values just return the raw value
		return dbf.parseRawValue(raw, column)
	default:
		return nil, newError("dbase-interpreter-datatovalue-2", fmt.Errorf("unsupported column data type: %s", string(column.DataType)))
	}
}

// Converts column data to the byte representation
// For M values the data has to be written to the memo file
func (dbf *DBF) valueToByteRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
	switch field.column.DataType {
	case Memo:
		return dbf.getMRepresentation(field)
	case Character:
		// C values are stored as strings, the returned string is not trimmed
		return dbf.getCRepresentation(field, skipSpacing)
	case Integer:
		// I values (int32)
		return dbf.getIRepresentation(field)
	case Currency:
		// Y (currency)
		return dbf.getYRepresentation(field)
	case Float:
		// F (Float)
		return dbf.getFRepresentation(field, skipSpacing)
	case Double:
		// B (double)
		return dbf.getBRepresentation(field)
	case Date:
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		return dbf.getDRepresentation(field)
	case DateTime:
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		return dbf.getTRepresentation(field)
	case Logical:
		// L (bool) values are stored as strings T or F, we only check for T, the rest is false...
		return dbf.getLRepresentation(field)
	case Numeric:
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		return dbf.getNRepresentation(field, skipSpacing)
	case Varbinary:
		// Q values just return the raw value
		fallthrough
	case Varchar:
		// V values just return the raw value
		fallthrough
	case Blob:
		// W values just return the raw value
		fallthrough
	case Picture:
		// P values just return the raw value
		fallthrough
	case General:
		// G values just return the raw value
		return dbf.getRawRepresentation(field)
	default:
		return nil, newError("dbase-interpreter-valuetobyterepresentation-1", fmt.Errorf("unsupported column data type: %s at column field: %v", field.Type(), field.Name()))
	}
}

// Returns the value from the memo file as string or []byte
func (dbf *DBF) parseMValue(raw []byte, column *Column) (interface{}, error) {
	// M values contain the address in the FPT file from where to read data
	memo, isText, err := dbf.parseMemo(raw)
	if err != nil {
		return nil, newError("dbase-interpreter-parsemvalue-1", fmt.Errorf("parsing memo failed at column field: %v failed with error: %w", column.Name(), err))
	}
	if isText {
		return string(memo), nil
	}
	return memo, nil
}

// Saves the value to the memo file and returns the address in the FPT file
func (dbf *DBF) getMRepresentation(field *Field) ([]byte, error) {
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
		return nil, newError("dbase-interpreter-parsemrepresentation-1", fmt.Errorf("invalid type for memo field: %T", field.value))
	}
	// Write the memo to the memo file
	address, err := dbf.writeMemo(memo, txt, len(memo))
	if err != nil {
		return nil, newError("dbase-interpreter-getmrepresentation-2", fmt.Errorf("writing to memo file at column field: %v failed with error: %w", field.Name(), err))
	}
	return address, nil
}

// Returns the value as string
func (dbf *DBF) parseCValue(raw []byte, column *Column) (interface{}, error) {
	// C values are stored as strings, the returned string is not trimmed
	str, err := toUTF8String(raw, dbf.config.Converter)
	if err != nil {
		return str, newError("dbase-interpreter-parsecvalue-1", fmt.Errorf("parsing to utf8 string failed at column field: %v failed with error: %w", column.Name(), err))
	}
	return str, nil
}

// Returns the string value as byte representation
func (dbf *DBF) getCRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
	// C values are stored as strings, the returned string is not trimmed
	c, ok := field.value.(string)
	if !ok {
		return nil, newError("dbase-interpreter-getcrepresentation-1", fmt.Errorf("invalid data type %T, expected string on column field: %v", field.value, field.Name()))
	}
	raw := make([]byte, field.column.Length)
	bin, err := fromUtf8String([]byte(c), dbf.config.Converter)
	if err != nil {
		return nil, newError("dbase-interpreter-getcrepresentation-2", fmt.Errorf("parsing from utf8 string at column field: %v failed with error %w", field.Name(), err))
	}
	if skipSpacing {
		return bin, nil
	}
	bin = appendSpaces(bin, int(field.column.Length))
	copy(raw, bin)
	if len(raw) > int(field.column.Length) {
		return nil, newError("dbase-interpreter-getcrepresentation-3", fmt.Errorf("invalid length %v bytes > %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as int32
func (dbf *DBF) parseIValue(raw []byte) (interface{}, error) {
	return int32(binary.LittleEndian.Uint32(raw)), nil
}

// Returns the int32 value as byte representation
func (dbf *DBF) getIRepresentation(field *Field) ([]byte, error) {
	// I values (int32)
	i, ok := field.value.(int32)
	if !ok {
		f, ok := field.value.(float64)
		if !ok {
			return nil, newError("dbase-interpreter-getirepresentation-1", fmt.Errorf("invalid data type %T, expected int32 at column field: %v", field.value, field.Name()))
		}
		// check for lower and uppper bounds
		if f > 0 && f <= math.MaxInt32 {
			i = int32(f)
		}
	}
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(i)
	if err != nil {
		return nil, newError("dbase-interpreter-getirepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getirepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as float64
func (dbf *DBF) parseYValue(raw []byte) (interface{}, error) {
	return float64(int64(binary.LittleEndian.Uint64(raw))) / 10000, nil
}

// Returns the float64 value as byte representation
func (dbf *DBF) getYRepresentation(field *Field) ([]byte, error) {
	f, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getyrepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
	}
	// Cast to int64 and multiply by 10000
	i := int64(f * 10000)
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(i)
	if err != nil {
		return nil, newError("dbase-interpreter-getyrepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getyrepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as float64
func (dbf *DBF) parseFValue(raw []byte, column *Column) (interface{}, error) {
	f, err := parseFloat(raw)
	if err != nil {
		return f, newError("dbase-interpreter-parsefvalue-1", fmt.Errorf("parsing float at column field: %v failed with error: %w", column.Name(), err))
	}
	return f, nil
}

// Returns the float64 value as byte representation
func (dbf *DBF) getFRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
	b, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getfrepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
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
	if skipSpacing {
		return bin, nil
	}
	return prependSpaces(bin, int(field.column.Length)), nil
}

// Returns the value as float64
func (dbf *DBF) parseBValue(raw []byte) (interface{}, error) {
	return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
}

// Returns the float64 value as byte representation
func (dbf *DBF) getBRepresentation(field *Field) ([]byte, error) {
	b, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getbrepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
	}
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(b)
	if err != nil {
		return nil, newError("dbase-interpreter-getbrepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getbrepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as time.Time
func (dbf *DBF) parseDValue(raw []byte, column *Column) (interface{}, error) {
	// D values are stored as string in format YYYYMMDD, convert to time.Time
	date, err := parseDate(raw)
	if err != nil {
		return date, newError("dbase-interpreter-parsedvalue-1", fmt.Errorf("parsing to date at column field: %v failed with error: %w", column.Name(), err))
	}
	return date, nil
}

// Get the time.Time value as byte representation
func (dbf *DBF) getDRepresentation(field *Field) ([]byte, error) {
	d, ok := field.value.(time.Time)
	if !ok {
		s, ok := field.value.(string)
		if !ok {
			return nil, newError("dbase-interpreter-getdrepresentation-1", fmt.Errorf("invalid data type %T, expected time.Time at column field: %v", field.value, field.Name()))
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, newError("dbase-interpreter-getdrepresentation-2", fmt.Errorf("parsing time failed at column field: %v failed with error: %w", field.Name(), err))
		}
		d = t
	}
	raw := make([]byte, field.column.Length)
	bin := []byte(d.Format("20060102"))
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getdrepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as time.Time
func (dbf *DBF) parseTValue(raw []byte, column *Column) (interface{}, error) {
	dateTime, err := parseDateTime(raw)
	if err != nil {
		return dateTime, newError("dbase-interpreter-parsetvalue-1", fmt.Errorf("parsing date time at column field: %v failed with error: %w", column.Name(), err))
	}
	return dateTime, nil
}

// Get the time.Time value as byte representation consisting of 4 bytes for julian date and 4 bytes for time
func (dbf *DBF) getTRepresentation(field *Field) ([]byte, error) {
	t, ok := field.value.(time.Time)
	if !ok {
		s, ok := field.value.(string)
		if !ok {
			return nil, newError("dbase-interpreter-gettrepresentation-1", fmt.Errorf("invalid data type %T, expected time.Time at column field: %v", field.value, field.Name()))
		}
		parsedTime, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, newError("dbase-interpreter-gettrepresentation-2", fmt.Errorf("parsing time failed at column field: %v failed with error: %w", field.Name(), err))
		}
		t = parsedTime
	}
	raw := make([]byte, 8)
	i := YMD2JD(t.Year(), int(t.Month()), t.Day())
	date, err := toBinary(uint64(i))
	if err != nil {
		return nil, newError("dbase-interpreter-gettrepresentation-3", fmt.Errorf("time conversion at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw[:4], date)
	millis := t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000
	time, err := toBinary(uint64(millis))
	if err != nil {
		return nil, newError("dbase-interpreter-gettrepresentation-4", fmt.Errorf("binary conversion at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw[4:], time)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-gettrepresentation-5", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Return the value (T or F) as bool
func (dbf *DBF) parseLValue(raw []byte) (interface{}, error) {
	return string(raw) == "T", nil
}

// Get the bool value as byte representation (T or F)
func (dbf *DBF) getLRepresentation(field *Field) ([]byte, error) {
	l, ok := field.value.(bool)
	if !ok {
		return nil, newError("dbase-interpreter-getlrepresentation-1", fmt.Errorf("invalid data type %T, expected bool at column field: %v", field.value, field.Name()))
	}
	raw := []byte("F")
	if l {
		return []byte("T"), nil
	}
	return raw, nil
}

// Get the raw value as byte representation
func (dbf *DBF) parseRawValue(raw []byte, column *Column) (interface{}, error) {
	return raw, nil
}

// Get the raw value as byte representation (only type check for []byte is performed)
func (dbf *DBF) getRawRepresentation(field *Field) ([]byte, error) {
	raw, ok := field.value.([]byte)
	if !ok {
		return nil, newError("dbase-interpreter-getrawrepresentation-1", fmt.Errorf("invalid data type %T, expected []byte at column field: %v", field.value, field.Name()))
	}
	return raw, nil
}

// Returns the value as integer or float64
func (dbf *DBF) parseNValue(raw []byte, column *Column) (interface{}, error) {
	if column.Decimals == 0 {
		i, err := parseNumericInt(raw)
		if err != nil {
			return i, newError("dbase-interpreter-parsenvalue-1", fmt.Errorf("parsing numeric int at column field: %v failed with error: %w", column.Name(), err))
		}
		return i, nil
	}

	return dbf.parseFValue(raw, column)
}

// Get the integer or float64 value as byte representation
func (dbf *DBF) getNRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
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
		return nil, newError("dbase-interpreter-parsenrepresentation-1", fmt.Errorf("invalid data type %T, expected int64 or float64 at column field: %v", field.value, field.Name()))
	}
	if skipSpacing {
		return bin, nil
	}
	return prependSpaces(bin, int(field.column.Length)), nil
}

// Convert data to binary representation
func toBinary(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil, newError("dbase-interpreter-tobinary-1", err)
	}
	return buf.Bytes(), nil
}
