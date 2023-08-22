package dbase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Table is a struct containing the table columns, modifications and the row pointer
type Table struct {
	name       string          // Name of the table
	columns    []*Column       // Columns defined in this table
	mods       []*Modification // Modification to change values or name of fields
	rowPointer uint32          // Internal row pointer, can be moved
}

// Row is a struct containing the row Position, deleted flag and data fields
type Row struct {
	handle     *File    // Pointer to the DBF object this row belongs to
	Position   uint32   // Position of the row in the file
	ByteOffset int64    // Byte offset of the row in the file
	Deleted    bool     // Deleted flag
	fields     []*Field // Fields in this row
}

// Column is a struct containing the column information
type Column struct {
	FieldName [11]byte // Column name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
	DataType  byte     // Column type
	Position  uint32   // Displacement of column in row
	Length    uint8    // Length of column (in bytes)
	Decimals  uint8    // Number of decimal places
	Flag      byte     // Column flag
	Next      uint32   // Value of autoincrement Next value
	Step      uint16   // Value of autoincrement Step value
	Reserved  [7]byte  // Reserved
}

// Field is a row data field
type Field struct {
	column *Column     // Pointer to the column this field belongs to
	value  interface{} // Value of the field
}

// nullFlagPosition calculates position of this column in the null flag
func (table *Table) nullFlagPosition(column *Column) int {
	bitCount := 0
	for _, c := range table.columns {
		if c.DataType != byte(Varchar) && c.DataType != byte(Varbinary) || c == column {
			break
		}
		bitCount++
		if c.Flag == byte(NullableFlag) || c.Flag == byte(NullableFlag|BinaryFlag) {
			bitCount++
		}
	}

	return bitCount
}

// Returns all values of a row as a slice of interface{}
func (row *Row) Values() []interface{} {
	values := make([]interface{}, 0)
	for _, field := range row.fields {
		if field != nil {
			values = append(values, field.value)
		}
	}
	return values
}

// Returns the value of a row at the given position
func (row *Row) Value(pos int) interface{} {
	return row.fields[pos].value
}

// Returns the value of a row at the given column name
func (row *Row) ValueByName(name string) (interface{}, error) {
	pos := row.handle.ColumnPosByName(name)
	if pos < 0 {
		return nil, newError("dbase-table-valuebyname-1", fmt.Errorf("column %v not found", name))
	}
	return row.Value(pos), nil
}

// Returns the value of a row at the given column name
// MustValueByName panics if the value is not found
func (row *Row) MustValueByName(name string) interface{} {
	val, err := row.ValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as a string
// If the value is neither a string nor a byte slice, an error is returned
func (row *Row) StringValueByName(name string) (string, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return "", newError("dbase-table-stringvaluebyname-1", err)
	}
	if val != nil {
		str, ok := val.(string)
		if ok {
			return str, nil
		}
		bslice, ok := val.([]byte)
		if ok {
			return string(sanitizeString(bslice)), nil
		}
		return "", newError("dbase-table-stringvaluebyname-2", fmt.Errorf("value is not a string"))
	}
	return "", nil
}

// Returns the value of a row at the given column name as a string
// MustStringValueByName panics if the value is not found or not a string
func (row *Row) MustStringValueByName(name string) string {
	val, err := row.StringValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as an int64
// If the value is not castable to an int64, an error is returned
func (row *Row) IntValueByName(name string) (int64, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return 0, newError("dbase-table-intvaluebyname-1", err)
	}
	if val != nil {
		val = cast(val, reflect.TypeOf(int64(0)))
		intVal, ok := val.(int64)
		if ok {
			return intVal, nil
		}
		return 0, newError("dbase-table-intvaluebyname-2", fmt.Errorf("value is not an int"))
	}
	return 0, nil
}

// Returns the value of a row at the given column name as an int32
// MustIntValueByName panics if the value is not found or not an int32
func (row *Row) MustIntValueByName(name string) int64 {
	val, err := row.IntValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as a float64
// If the value is not castable to a float64, an error is returned
func (row *Row) FloatValueByName(name string) (float64, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return 0, newError("dbase-table-floatvaluebyname-1", err)
	}
	if val != nil {
		val = cast(val, reflect.TypeOf(float64(0)))
		floatVal, ok := val.(float64)
		if ok {
			return floatVal, nil
		}
		return 0, newError("dbase-table-floatvaluebyname-2", fmt.Errorf("value is not a float"))
	}
	return 0, nil
}

// Returns the value of a row at the given column name as a float64
// MustFloatValueByName panics if the value is not found or not a float64
func (row *Row) MustFloatValueByName(name string) float64 {
	val, err := row.FloatValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as a bool
// If the value is not a bool, an error is returned
func (row *Row) BoolValueByName(name string) (bool, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return false, newError("dbase-table-boolvaluebyname-1", err)
	}
	if val != nil {
		boolVal, ok := val.(bool)
		if ok {
			return boolVal, nil
		}
		return false, newError("dbase-table-boolvaluebyname-2", fmt.Errorf("value is not a bool"))
	}

	return false, nil
}

// Returns the value of a row at the given column name as a bool
// MustBoolValueByName panics if the value is not found or not a bool
func (row *Row) MustBoolValueByName(name string) bool {
	val, err := row.BoolValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as a time.Time
// If the value is not a time.Time, an error is returned
func (row *Row) TimeValueByName(name string) (time.Time, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return time.Time{}, newError("dbase-table-timevaluebyname-1", err)
	}
	if val != nil {
		val = cast(val, reflect.TypeOf(time.Time{}))
		timeVal, ok := val.(time.Time)
		if ok {
			return timeVal, nil
		}
		return time.Time{}, newError("dbase-table-timevaluebyname-2", fmt.Errorf("value is not a time"))
	}
	return time.Time{}, nil
}

// Returns the value of a row at the given column name as a time.Time
// MustTimeValueByName panics if the value is not found or not a time.Time
func (row *Row) MustTimeValueByName(name string) time.Time {
	val, err := row.TimeValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns the value of a row at the given column name as a []byte
// If the value is not a []byte, []uint8 or string, an error is returned
func (row *Row) BytesValueByName(name string) ([]byte, error) {
	val, err := row.ValueByName(name)
	if err != nil {
		return nil, newError("dbase-table-bytesvaluebyname-1", err)
	}
	if val != nil {
		val = cast(val, reflect.TypeOf([]byte{}))
		bytesVal, ok := val.([]byte)
		if ok {
			return bytesVal, nil
		}
		uint8Val, ok := val.([]uint8)
		if ok {
			return uint8Val, nil
		}
		strVal, ok := val.(string)
		if ok {
			return []byte(strVal), nil
		}
		return nil, newError("dbase-table-bytesvaluebyname-2", fmt.Errorf("value is not a byte slice"))
	}
	return nil, nil
}

// Returns the value of a row at the given column name as a []byte
// MustBytesValueByName panics if the value is not found or not a []byte
func (row *Row) MustBytesValueByName(name string) []byte {
	val, err := row.BytesValueByName(name)
	if err != nil {
		panic(err)
	}
	return val
}

// Returns all fields of the current row
func (row *Row) Fields() []*Field {
	return row.fields
}

// Returns the field of a row by position or nil if not found
func (row *Row) Field(pos int) *Field {
	if pos < 0 || pos >= len(row.fields) {
		return nil
	}
	return row.fields[pos]
}

// Returns the field of a row by name or nil if not found
func (row *Row) FieldByName(name string) *Field {
	return row.Field(row.handle.ColumnPosByName(name))
}

// Converts the row back to raw dbase data
func (row *Row) ToBytes() ([]byte, error) {
	debugf("Converting row %v to row data (%d bytes)...", row.Position, row.handle.header.RowLength)
	data := make([]byte, row.handle.header.RowLength)
	// a row should start with te delete flag, a space ACTIVE(0x20) or DELETED(0x2A)
	if row.Deleted {
		data[0] = byte(Deleted)
	} else {
		data[0] = byte(Active)
	}
	// deleted flag already read
	offset := uint16(1)
	varPos := 0
	nullFlag := make([]byte, 1)
	for _, field := range row.fields {
		val, err := row.handle.Represent(field, false)
		if err != nil {
			return nil, newError("dbase-table-rowtobytes-1", err)
		}
		// Get null and length if variable length field
		if field.column.DataType == byte(Varbinary) || field.column.DataType == byte(Varchar) {
			length := len(val)
			// Not null and not full size
			if length < int(field.column.Length) && length > 0 {
				debugf("Variable length field %v is not null and not full size (%v < %v)", field.column.Name(), length, field.column.Length)
				// Set last byte as length
				buf := make([]byte, field.column.Length)
				copy(buf, val)
				buf[field.column.Length-1] = byte(length)
				val = buf
				// Set full size flag
				byteIndex := varPos / 8
				bitIndex := varPos % 8
				nullFlag[byteIndex] = setNthBit(nullFlag[byteIndex], bitIndex)
			} else if length == 0 { // Null
				debugf("Variable length field %v is null", field.column.Name())
				// Set null flag
				byteIndex := varPos / 8
				bitIndex := varPos % 8
				nullFlag[byteIndex] = setNthBit(nullFlag[byteIndex], bitIndex+1)
			}
			// Increase variable field in nullFlag position, increase by one for length and another one for null flag
			varPos++
			if field.column.Flag == byte(NullableFlag) || field.column.Flag == byte(NullableFlag|BinaryFlag) {
				varPos++
			}
		}
		copy(data[offset:offset+uint16(field.column.Length)], val)
		offset += uint16(field.column.Length)
	}
	// Append null flag column at the end of the row
	if row.handle.nullFlagColumn != nil {
		debugf("Appending null flag column at the end of the row => %b", nullFlag)
		copy(data[offset:offset+uint16(row.handle.nullFlagColumn.Length)], nullFlag)
	}
	return data, nil
}

// Returns a complete row as a map.
func (row *Row) ToMap() (map[string]interface{}, error) {
	debugf("Converting row %v to map...", row.Position)
	out := make(map[string]interface{})
	var err error
	for i, field := range row.fields {
		val := field.GetValue()
		if i >= 0 && i < len(row.handle.table.mods) && row.handle.table.mods[i] != nil {
			mod := row.handle.table.mods[i]
			if mod.TrimSpaces {
				if str, ok := val.(string); ok {
					val = strings.TrimSpace(str)
				}

				if bslice, ok := val.([]byte); ok {
					val = sanitizeString(bslice)
				}
			}
			if mod.Convert != nil {
				debugf("Converting field %v due to modification", field.Name())
				val, err = mod.Convert(val)
				if err != nil {
					return nil, newError("dbase-table-tomap-1", err)
				}
			}
			if len(mod.ExternalKey) != 0 {
				debugf("Resolving external key %v for field %v due to modification", mod.ExternalKey, field.Name())
				out[mod.ExternalKey] = val
				continue
			}
		}
		out[field.Name()] = val
	}
	return out, nil
}

// Returns a complete row as a JSON object.
func (row *Row) ToJSON() ([]byte, error) {
	debugf("Converting row %v to JSON...", row.Position)
	m, err := row.ToMap()
	if err != nil {
		return nil, newError("dbase-table-tojson-1", err)
	}
	j, err := json.Marshal(m)
	if err != nil {
		return j, newError("dbase-table-tojson-2", err)
	}
	return j, nil
}

// Converts a row to a struct.
// The struct must have the same field names as the columns in the table or the dbase tag must be set.
// dbase tags can be used to name the field. For example: `dbase:"<table_name>.<field_name>"` or `dbase:"<field_name>"`
func (row *Row) ToStruct(v interface{}) error {
	rt := reflect.TypeOf(v)
	if rt.Kind() != reflect.Ptr {
		return newError("dbase-table-struct-1", fmt.Errorf("expected pointer, got %v", rt.Kind()))
	}
	debugf("Converting row %v to struct...", row.Position)
	m, err := row.ToMap()
	if err != nil {
		return newError("dbase-table-struct-2", err)
	}
	tags := getStructTags(v)
	for tag := range tags {
		if strings.Contains(tag, ".") {
			// Split the tag by the dot to get the TableName and ColumnName
			parts := strings.Split(tag, ".")
			if len(parts) != 2 {
				continue
			}
			// Check if the TableName matches the current table
			if parts[0] != row.handle.table.name {
				delete(tags, tag)
				continue
			}

			// Apply the ColumnName as the new tag
			tags[parts[1]] = tags[tag]
			delete(tags, tag)
		}
	}

	for k, val := range m {
		err := setStructField(tags, v, k, val)
		if err != nil {
			return newError("dbase-table-tostruct-2", err)
		}
	}
	return nil
}

// Returns the name of the column as a trimmed string (max length 10)
func (c *Column) Name() string {
	return string(bytes.TrimRight(c.FieldName[:], "\x00"))
}

// Returns the type of the column as string (length 1)
func (c *Column) Type() string {
	return string(c.DataType)
}

func (c *Column) Reflect() (reflect.Type, error) {
	return DataType(c.DataType).Reflect()
}

// SetValue allows to change the field value
func (field *Field) SetValue(value interface{}) error {
	if field == nil {
		return newError("dbase-table-setvalue-1", fmt.Errorf("field is not defined by table"))
	}
	field.value = value
	return nil
}

// Value returns the field value
func (field Field) GetValue() interface{} {
	return field.value
}

// Name returns the field name
func (field Field) Name() string {
	return field.column.Name()
}

// Type returns the field type
func (field Field) Type() DataType {
	return DataType(field.column.DataType)
}

// Column returns the field column definition
func (field Field) Column() *Column {
	return field.column
}

// Create a new DBF file with the specified version, configuration and columns
// Please only use this for development and testing purposes and dont build new applications with it
func NewTable(version FileVersion, config *Config, columns []*Column, memoBlockSize uint16, io IO) (*File, error) {
	if len(columns) == 0 || config.Converter == nil {
		return nil, newError("dbase-table-newtable-1", errors.New("invalid parameters"))
	}
	file := &File{
		config: config,
		io:     io,
		header: &Header{
			FileType:  byte(version),
			Year:      uint8(time.Now().Year() - 2000),
			Month:     uint8(time.Now().Month()),
			Day:       uint8(time.Now().Day()),
			FirstRow:  296 + uint16(len(columns))*32,
			RowLength: 1,
			CodePage:  config.Converter.CodePage(),
		},
		table: &Table{
			columns: make([]*Column, 0),
		},
		dbaseMutex: &sync.Mutex{},
		memoMutex:  &sync.Mutex{},
	}
	debugf("Creating new DBF file: %v - type: %v - year: %v - month: %v - day: %v - first row: %v - row length: %v - code page: %v - columns: %v", config.Filename, file.header.FileType, file.header.Year, file.header.Month, file.header.Day, file.header.FirstRow, file.header.RowLength, file.header.CodePage, len(columns))
	// Determines how many bytes are needed for the _NullFlag field if needed
	nullFlagLength := 0
	// Check if we need a memo file
	memoField := false
	for _, column := range columns {
		if column.DataType == byte(Memo) {
			memoField = true
			file.header.TableFlags = byte(MemoFlag)
		}
		if column.DataType == byte(Varchar) || column.DataType == byte(Varbinary) {
			nullFlagLength++
			if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
				nullFlagLength++
			}
		}
		// Set the column position in the row
		column.Position = uint32(file.header.RowLength)
		// Add the column length to the row length
		file.header.RowLength += uint16(column.Length)
		// Add columns to the table
		file.table.columns = append(file.table.columns, column)
	}
	// If there are memo fields, add the memo header
	if memoField {
		file.memoHeader = &MemoHeader{
			NextFree:  0,
			Unused:    [2]byte{0x00, 0x00},
			BlockSize: memoBlockSize,
		}
		debugf("Initializing related memo file header - block size: %v", file.memoHeader.BlockSize)
	}
	// If there are nullable or variable length fields, add the null flag column
	if nullFlagLength > 0 {
		length := nullFlagLength / 8
		if nullFlagLength%8 > 0 {
			length++
		}
		file.nullFlagColumn = &Column{
			FieldName: nullFlagColumn,
			DataType:  0x30,
			Position:  uint32(file.header.RowLength),
			Length:    uint8(length),
			Decimals:  0,
			Flag:      byte(HiddenFlag + NullableFlag),
			Next:      0x00,
			Step:      0x00,
			Reserved:  [7]byte{},
		}
		file.header.FirstRow += 32
		file.header.RowLength += uint16(length)
		debugf("Initializing null flag column - length: %v", length)
	}

	err := file.Init()
	if err != nil {
		return nil, newError("dbase-table-newtable-1", err)
	}

	return file, nil
}

// Create a new column with the specified name, data type, length, decimals and nullable flag
// The length is only used for character, varbinary, varchar, numeric and float data types
func NewColumn(name string, dataType DataType, length uint8, decimals uint8, nullable bool) (*Column, error) {
	if len(name) == 0 || len(name) > 10 {
		return nil, newError("dbase-table-newcolumn-1", errors.New("column name must be between 1 and 10 characters long"))
	}
	column := &Column{
		FieldName: [11]byte{},
		DataType:  byte(dataType),
		Position:  uint32(0),
		Decimals:  decimals,
		Length:    uint8(0),
		Flag:      0x00,
		Next:      uint32(0),
		Step:      uint16(0),
		Reserved:  [7]byte{},
	}
	copy(column.FieldName[:], []byte(strings.ToUpper(name))[:11])
	debugf("Creating new column: %v - type: %v - length: %v - decimals: %v - nullable: %v - position: %v - flag: %v", name, dataType, length, decimals, nullable, column.Position, column.Flag)
	// Set the appropriate flag for nullable fields
	if nullable {
		column.Flag = byte(NullableFlag)
	}
	// Check for data type to specify the length
	switch dataType {
	case Varbinary:
		column.Flag |= byte(BinaryFlag)
		fallthrough
	case Varchar, Character:
		if length == 0 || length > 254 {
			return nil, newError("dbase-table-newcolumn-2", errors.New("character, varbinary and varchar values can only be between 1 to 254 characters long"))
		}
		column.Length = length
	case Numeric, Float:
		if length == 0 || length > 20 {
			return nil, newError("dbase-table-newcolumn-3", errors.New("numeric and float values can only be between 1 to 20 characters long"))
		}
		column.Length = length
	case Logical:
		column.Length = 1
	case Integer, Memo:
		column.Length = 4
	case Currency, Date, DateTime, Double:
		column.Length = 8
	default:
		return nil, newError("dbase-table-newcolumn-4", fmt.Errorf("invalid data type %v specified", dataType))
	}
	return column, nil
}

// Writes the row to the file at the row pointer position
func (row *Row) Write() error {
	return row.handle.WriteRow(row)
}

// Increment increases set the value of the auto increment Column to the Next value
// Also increases the Next value by the amount of Step
// Rewrites the columns header
func (row *Row) Increment() error {
	for _, field := range row.fields {
		if field.column.Flag == byte(AutoincrementFlag) {
			field.value = int32(field.column.Next)
			field.column.Next += uint32(field.column.Step)
			debugf("Incrementing autoincrement field %s to %v (Step: %v)", field.column.Name(), field.value, field.column.Step)
		}
	}
	err := row.handle.WriteColumns()
	if err != nil {
		return newError("dbase-table-row-increment-1", err)
	}
	return nil
}

// Appends the row as a new entry to the file
func (row *Row) Add() error {
	row.Position = row.handle.header.RowsCount + 1
	return row.Write()
}
