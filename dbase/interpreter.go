package dbase

import (
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
// | Column Type | Column Type Name | Golang type |
// | ----------- | ---------------- | ----------- |
// | B | Double | float64 |
// | C | Character | string |
// | D | Date | time.Time |
// | F | Float | float64 |
// | I | Integer | int32 |
// | L | Logical | bool |
// | M | Memo | string |
// | M | Memo (Binary) | []byte |
// | N | Numeric (0 decimals) | int64 |
// | N | Numeric (with decimals) | float64 |
// | T | DateTime | time.Time |
// | Y | Currency | float64 |
//
// Not all available column types have been implemented because we don't use them in our DBFs
func (file *File) Interpret(raw []byte, column *Column) (interface{}, error) {
	var funcs = map[DataType]func([]byte, *Column) (interface{}, error){
		// M values contain the address in the FPT file from where to read data
		Memo: file.parseMemo,
		// C values are stored as strings, the returned string is not trimmed
		Character: file.parseCharacter,
		// I values are stored as numeric values
		Integer: file.parseInteger,
		// Y values are currency values stored as ints with 4 decimal places
		Currency: file.parseCurrency,
		// F values are stored as string values
		Float: file.parseFloat,
		// B (double) values are stored as numeric values
		Double: file.parseDouble,
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		Date: file.parseDate,
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		DateTime: file.parseDateTime,
		// L values are stored as strings T or F, we only check for T, the rest is false...
		Logical: file.parseLogical,
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		Numeric: file.parseNumeric,
		// V and Q values just return the raw value
		Varchar:   file.parseVarchar,
		Varbinary: file.parseVarbinary,
		// W, P and G values just return the raw value
		Blob:    file.parseRaw,
		Picture: file.parseRaw,
		General: file.parseRaw,
	}

	if len(raw) != int(column.Length) {
		return nil, newErrorf("dbase-interpreter-datatovalue-1", "invalid length %v Bytes != %v Bytes at column field: %v", len(raw), column.Length, column.Name())
	}

	f, ok := funcs[DataType(column.DataType)]
	if !ok {
		return nil, newErrorf("dbase-interpreter-datatovalue-2", "unsupported column data type: %s at column field: %v", DataType(column.DataType), column.Name())
	}

	return f(raw, column)
}

// Represent converts column data to the byte representation of the columns data type
// For M values the data is written to the memo file and the address is returned
func (file *File) Represent(field *Field, padding bool) ([]byte, error) {
	var funcs = map[DataType]func(*Field, bool) ([]byte, error){
		// M values contain the address in the FPT file from where to read data
		Memo: file.getMemoRepresentation,
		// C values are stored as strings, the returned string is not trimmed
		Character: file.getCharacterRepresentation,
		// I values (int32)
		Integer: file.getIntegerRepresentation,
		// Y (currency)
		Currency: file.getCurrencyRepresentation,
		// F (Float)
		Float: file.getFloatRepresentation,
		// B (double)
		Double: file.getDoubleRepresentation,
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		Date: file.getDateRepresentation,
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		DateTime: file.getDateTimeRepresentation,
		// L (bool) values are stored as strings T or F, we only check for T, the rest is false...
		Logical: file.getLogicalRepresentation,
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		Numeric: file.getNumericRepresentation,
		// V and Q values just return the raw value
		Varchar:   file.getVarcharRepresentation,
		Varbinary: file.getVarbinaryRepresentation,
		// W, P and G values just return the raw value
		Blob:    file.getRawRepresentation,
		Picture: file.getRawRepresentation,
		General: file.getRawRepresentation,
	}

	// if value is nil, return empty byte array
	if field.GetValue() == nil {
		return make([]byte, field.column.Length), nil
	}

	f, ok := funcs[DataType(field.column.DataType)]
	if !ok {
		return nil, newErrorf("dbase-interpreter-valuetodata-1", "unsupported column data type: %s at column field: %v", DataType(field.column.DataType), field.Name())
	}

	return f(field, padding)
}

// Returns the value from the memo file as string or []byte
func (file *File) parseMemo(raw []byte, column *Column) (interface{}, error) {
	// M values contain the address in the FPT file from where to read data
	memo, isText, err := file.ReadMemo(raw)
	if err != nil {
		return nil, newError("dbase-interpreter-parsememo-1", fmt.Errorf("parsing memo failed at column field: %v failed with error: %w", column.Name(), err))
	}
	if isText {
		return string(memo), nil
	}
	return memo, nil
}

// Saves the value to the memo file and returns the address in the FPT file
func (file *File) getMemoRepresentation(field *Field, _ bool) ([]byte, error) {
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
		return nil, newError("dbase-interpreter-getmemorepresentation-1", fmt.Errorf("invalid type for memo field: %T", field.value))
	}
	// Write the memo to the memo file
	address, err := file.WriteMemo(memo, txt, len(memo))
	if err != nil {
		return nil, newError("dbase-interpreter-getmrepresentation-2", fmt.Errorf("writing to memo file at column field: %v failed with error: %w", field.Name(), err))
	}
	return address, nil
}

// Returns the value as string
func (file *File) parseCharacter(raw []byte, column *Column) (interface{}, error) {
	// C values are stored as strings, the returned string is not trimmed
	str, err := toUTF8String(raw, file.config.Converter)
	if err != nil {
		return str, newError("dbase-interpreter-parsecharacter-1", fmt.Errorf("parsing to utf8 string failed at column field: %v failed with error: %w", column.Name(), err))
	}
	return str, nil
}

// Returns the string value as byte representation
func (file *File) getCharacterRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
	// C values are stored as strings, the returned string is not trimmed
	c, ok := field.value.(string)
	if !ok {
		return nil, newError("dbase-interpreter-getcharacterrepresentation-1", fmt.Errorf("invalid data type %T, expected string on column field: %v", field.value, field.Name()))
	}
	raw := make([]byte, field.column.Length)
	bin, err := fromUtf8String([]byte(c), file.config.Converter)
	if err != nil {
		return nil, newError("dbase-interpreter-getcharacterrepresentation-2", fmt.Errorf("parsing from utf8 string at column field: %v failed with error %w", field.Name(), err))
	}
	if skipSpacing {
		return bin, nil
	}
	bin = appendSpaces(bin, int(field.column.Length))
	copy(raw, bin)
	if len(raw) > int(field.column.Length) {
		return nil, newError("dbase-interpreter-getcharacterrepresentation-3", fmt.Errorf("invalid length %v bytes > %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as int32
func (file *File) parseInteger(raw []byte, _ *Column) (interface{}, error) {
	return int32(binary.LittleEndian.Uint32(raw)), nil
}

// Returns the int32 value as byte representation
func (file *File) getIntegerRepresentation(field *Field, _ bool) ([]byte, error) {
	// I values (int32)
	i, ok := field.value.(int32)
	if !ok {
		f, ok := field.value.(float64)
		if !ok {
			return nil, newError("dbase-interpreter-getintegerrepresentation-1", fmt.Errorf("invalid data type %T, expected int32 at column field: %v", field.value, field.Name()))
		}
		// check for lower and uppper bounds
		if f > 0 && f <= math.MaxInt32 {
			i = int32(f)
		}
	}
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(i)
	if err != nil {
		return nil, newError("dbase-interpreter-getintegerrepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getintegerrepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as float64
func (file *File) parseCurrency(raw []byte, _ *Column) (interface{}, error) {
	return float64(int64(binary.LittleEndian.Uint64(raw))) / 10000, nil
}

// Returns the float64 value as byte representation
func (file *File) getCurrencyRepresentation(field *Field, _ bool) ([]byte, error) {
	f, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getcurrencyrepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
	}
	// Cast to int64 and multiply by 10000
	i := int64(f * 10000)
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(i)
	if err != nil {
		return nil, newError("dbase-interpreter-getcurrencyrepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getcurrencyrepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as float64
func (file *File) parseFloat(raw []byte, column *Column) (interface{}, error) {
	f, err := parseFloat(raw)
	if err != nil {
		return f, newError("dbase-interpreter-parsefloat-1", fmt.Errorf("parsing float at column field: %v failed with error: %w", column.Name(), err))
	}
	return f, nil
}

// Returns the float64 value as byte representation
func (file *File) getFloatRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
	b, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getfloatrepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
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
func (file *File) parseDouble(raw []byte, _ *Column) (interface{}, error) {
	return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
}

// Returns the float64 value as byte representation
func (file *File) getDoubleRepresentation(field *Field, _ bool) ([]byte, error) {
	b, ok := field.value.(float64)
	if !ok {
		return nil, newError("dbase-interpreter-getdoublerepresentation-1", fmt.Errorf("invalid data type %T, expected float64 at column field: %v", field.value, field.Name()))
	}
	raw := make([]byte, field.column.Length)
	bin, err := toBinary(b)
	if err != nil {
		return nil, newError("dbase-interpreter-getdoublerepresentation-2", fmt.Errorf("converting to binary at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getdoublerepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as time.Time
func (file *File) parseDate(raw []byte, column *Column) (interface{}, error) {
	// D values are stored as string in format YYYYMMDD, convert to time.Time
	date, err := parseDate(raw)
	if err != nil {
		return date, newError("dbase-interpreter-parsedatevalue-1", fmt.Errorf("parsing to date at column field: %v failed with error: %w", column.Name(), err))
	}
	return date, nil
}

// Get the time.Time value as byte representation
func (file *File) getDateRepresentation(field *Field, _ bool) ([]byte, error) {
	d, ok := field.value.(time.Time)
	if !ok {
		s, ok := field.value.(string)
		if !ok {
			return nil, newError("dbase-interpreter-getdaterepresentation-1", fmt.Errorf("invalid data type %T, expected time.Time at column field: %v", field.value, field.Name()))
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, newError("dbase-interpreter-getdaterepresentation-2", fmt.Errorf("parsing time failed at column field: %v failed with error: %w", field.Name(), err))
		}
		d = t
	}
	raw := make([]byte, field.column.Length)
	bin := []byte(d.Format("20060102"))
	copy(raw, bin)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getdaterepresentation-3", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Returns the value as time.Time
func (file *File) parseDateTime(raw []byte, _ *Column) (interface{}, error) {
	return parseDateTime(raw), nil
}

// Get the time.Time value as byte representation consisting of 4 bytes for julian date and 4 bytes for time
func (file *File) getDateTimeRepresentation(field *Field, _ bool) ([]byte, error) {
	t, ok := field.value.(time.Time)
	if !ok {
		s, ok := field.value.(string)
		if !ok {
			return nil, newError("dbase-interpreter-getdatetimerepresentation-1", fmt.Errorf("invalid data type %T, expected time.Time at column field: %v", field.value, field.Name()))
		}
		parsedTime, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, newError("dbase-interpreter-getdatetimerepresentation-2", fmt.Errorf("parsing time failed at column field: %v failed with error: %w", field.Name(), err))
		}
		t = parsedTime
	}
	raw := make([]byte, 8)
	i := julianDate(t.Year(), int(t.Month()), t.Day())
	date, err := toBinary(uint64(i))
	if err != nil {
		return nil, newError("dbase-interpreter-getdatetimerepresentation-3", fmt.Errorf("time conversion at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw[:4], date)
	millis := t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000
	time, err := toBinary(uint64(millis))
	if err != nil {
		return nil, newError("dbase-interpreter-getdatetimerepresentation-4", fmt.Errorf("binary conversion at column field: %v failed with error: %w", field.Name(), err))
	}
	copy(raw[4:], time)
	if len(raw) != int(field.column.Length) {
		return nil, newError("dbase-interpreter-getdatetimerepresentation-5", fmt.Errorf("invalid length %v bytes != %v bytes at column field: %v", len(raw), field.column.Length, field.Name()))
	}
	return raw, nil
}

// Return the value (T or F) as bool
func (file *File) parseLogical(raw []byte, _ *Column) (interface{}, error) {
	return string(raw) == "T", nil
}

// Get the bool value as byte representation (T or F)
func (file *File) getLogicalRepresentation(field *Field, _ bool) ([]byte, error) {
	l, ok := field.value.(bool)
	if !ok {
		return nil, newError("dbase-interpreter-getlogicalrepresentation-1", fmt.Errorf("invalid data type %T, expected bool at column field: %v", field.value, field.Name()))
	}
	raw := []byte("F")
	if l {
		return []byte("T"), nil
	}
	return raw, nil
}

// Get the raw value as byte representation
func (file *File) parseRaw(raw []byte, column *Column) (interface{}, error) {
	return raw, nil
}

// Get the raw value as byte representation (only type check for []byte is performed)
func (file *File) getRawRepresentation(field *Field, _ bool) ([]byte, error) {
	// If string is passed, convert to []byte
	if s, ok := field.value.(string); ok {
		return []byte(s), nil
	}
	raw, ok := field.value.([]byte)
	if !ok {
		return nil, newError("dbase-interpreter-getrawrepresentation-1", fmt.Errorf("invalid data type %T, expected []byte at column field: %v", field.value, field.Name()))
	}
	return raw, nil
}

// Returns the value as integer or float64
func (file *File) parseNumeric(raw []byte, column *Column) (interface{}, error) {
	if column.Decimals == 0 {
		i, err := parseNumericInt(raw)
		if err != nil {
			return i, newError("dbase-interpreter-parsenumeric-1", fmt.Errorf("parsing numeric int at column field: %v failed with error: %w", column.Name(), err))
		}
		return i, nil
	}

	return file.parseFloat(raw, column)
}

// Get the integer or float64 value as byte representation
func (file *File) getNumericRepresentation(field *Field, skipSpacing bool) ([]byte, error) {
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
		return nil, newError("dbase-interpreter-getnumericrepresentation-1", fmt.Errorf("invalid data type %T, expected int64 or float64 at column field: %v", field.value, field.Name()))
	}
	if skipSpacing {
		return bin, nil
	}
	return prependSpaces(bin, int(field.column.Length)), nil
}

func (file *File) parseVarchar(raw []byte, column *Column) (interface{}, error) {
	varlen, null, err := file.ReadNullFlag(uint64(file.table.rowPointer), column)
	if err != nil {
		return nil, newError("dbase-interpreter-parsevarchar-1", fmt.Errorf("reading null flag at column field: %v failed with error: %w", column.Name(), err))
	}
	if null {
		return []byte{}, nil
	}
	if varlen {
		length := raw[len(raw)-1]
		raw = raw[:length]
	}
	return string(raw), nil
}

func (file *File) getVarcharRepresentation(field *Field, _ bool) ([]byte, error) {
	s, ok := field.value.(string)
	if ok {
		return []byte(s), nil
	}
	m, ok := field.value.([]byte)
	if ok {
		return m, nil
	}
	return nil, newError("dbase-interpreter-getvarcharrepresentation-1", fmt.Errorf("invalid data type %T, expected string at column field: %v", field.value, field.Name()))
}

func (file *File) parseVarbinary(raw []byte, column *Column) (interface{}, error) {
	varlen, null, err := file.ReadNullFlag(uint64(file.table.rowPointer), column)
	if err != nil {
		return nil, newError("dbase-interpreter-parsevarbinary-1", fmt.Errorf("reading null flag at column field: %v failed with error: %w", column.Name(), err))
	}
	if null {
		return []byte{}, nil
	}
	if varlen {
		length := raw[len(raw)-1]
		raw = raw[:length]
	}
	return raw, nil
}

func (file *File) getVarbinaryRepresentation(field *Field, _ bool) ([]byte, error) {
	raw, ok := field.value.([]byte)
	if !ok {
		return nil, newError("dbase-interpreter-getvarbinaryrepresentation-1", fmt.Errorf("invalid data type %T, expected []byte at column field: %v", field.value, field.Name()))
	}
	return raw, nil
}
