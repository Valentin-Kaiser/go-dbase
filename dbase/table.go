package dbase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Containing DBF header information like dBase FileType, last change and rows count.
// https://docs.microsoft.com/en-us/previous-versions/visualstudio/foxpro/st4a0s68(v=vs.80)#table-header-record-structure
type Header struct {
	FileType   byte     // File type flag
	Year       uint8    // Last update year (0-99)
	Month      uint8    // Last update month
	Day        uint8    // Last update day
	RowsCount  uint32   // Number of rows in file
	FirstRow   uint16   // Position of first data row
	RowLength  uint16   // Length of one data row, including delete flag
	Reserved   [16]byte // Reserved
	TableFlags byte     // Table flags
	CodePage   byte     // Code page mark
}

// The raw header of the Memo file.
type MemoHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

type Database struct {
	file   *File
	tables map[string]*File
}

// Table is a struct containing the table columns, modifications and the row pointer
type Table struct {
	columns    []*Column       // Columns defined in this table
	mods       []*Modification // Modification to change values or name of fields
	rowPointer uint32          // Internal row pointer, can be moved
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

// Row is a struct containing the row Position, deleted flag and data fields
type Row struct {
	handle     *File    // Pointer to the DBF object this row belongs to
	Position   uint32   // Position of the row in the file
	ByteOffset int64    // Byte offset of the row in the file
	Deleted    bool     // Deleted flag
	fields     []*Field // Fields in this row
}

// Field is a row data field
type Field struct {
	column *Column     // Pointer to the column this field belongs to
	value  interface{} // Value of the field
}

// Modification allows to change the column name or value type
type Modification struct {
	TrimSpaces  bool                                   // Trim spaces from string values
	Convert     func(interface{}) (interface{}, error) // Conversion function to convert the value
	ExternalKey string                                 // External key to use for the column
}

/**
 *  ###############################################################
 *  #                   Create new DBF file
 *  ###############################################################
 */

// Open a database and all related tables
func OpenDatabase(config *Config) (*Database, error) {
	if config == nil {
		return nil, newError("dbase-io-opendatabase-1", fmt.Errorf("missing config"))
	}
	if len(strings.TrimSpace(config.Filename)) == 0 {
		return nil, newError("dbase-io-opendatabase-2", fmt.Errorf("missing filename"))
	}
	if strings.ToUpper(filepath.Ext(config.Filename)) != string(DBC) {
		return nil, newError("dbase-io-opendatabase-3", fmt.Errorf("invalid file name: %v", config.Filename))
	}
	debugf("Opening database: %v", config.Filename)
	databaseTable, err := OpenTable(config)
	if err != nil {
		return nil, newError("dbase-io-opendatabase-4", fmt.Errorf("opening database table failed with error: %w", err))
	}
	// Search by all records where object type is table
	typeField, err := databaseTable.NewFieldByName("OBJECTTYPE", "Table")
	if err != nil {
		return nil, newError("dbase-io-opendatabase-5", fmt.Errorf("creating type field failed with error: %w", err))
	}
	rows, err := databaseTable.Search(typeField, true)
	if err != nil {
		return nil, newError("dbase-io-opendatabase-6", fmt.Errorf("searching for type field failed with error: %w", err))
	}
	// Try to load the table files
	tables := make(map[string]*File)
	for _, row := range rows {
		objectName, err := row.ValueByName("OBJECTNAME")
		if err != nil {
			return nil, newError("dbase-io-opendatabase-7", fmt.Errorf("getting table name failed with error: %w", err))
		}
		tableName, ok := objectName.(string)
		if !ok {
			return nil, newError("dbase-io-opendatabase-8", fmt.Errorf("table name is not a string"))
		}
		tableName = strings.Trim(tableName, " ")
		if tableName == "" {
			continue
		}
		debugf("Found table: %v in database", tableName)
		// Replace underscores with spaces
		tablePath := path.Join(filepath.Dir(config.Filename), strings.ReplaceAll(tableName, "_", " ")+string(DBF))
		tableConfig := &Config{
			Filename:          tablePath,
			Converter:         config.Converter,
			Exclusive:         config.Exclusive,
			Untested:          config.Untested,
			TrimSpaces:        config.TrimSpaces,
			WriteLock:         config.WriteLock,
			ValidateCodePage:  config.ValidateCodePage,
			InterpretCodePage: config.InterpretCodePage,
		}
		// Load the table
		table, err := OpenTable(tableConfig)
		if err != nil {
			return nil, newError("dbase-io-opendatabase-9", fmt.Errorf("opening table failed with error: %w", err))
		}
		tables[tableName] = table
	}
	return &Database{file: databaseTable, tables: tables}, nil
}

// Close the database file and all related tables
func (db *Database) Close() error {
	for _, table := range db.tables {
		if err := table.Close(); err != nil {
			return newError("dbase-io-close-1", err)
		}
	}
	return db.file.Close()
}

// Create a new DBF file
func New(version FileVersion, config *Config, columns []*Column, memoBlockSize uint16) (*File, error) {
	if len(columns) == 0 {
		return nil, errors.New("no columns defined")
	}
	if config.Converter == nil {
		return nil, errors.New("no converter defined")
	}
	file := &File{
		config: config,
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
			if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
				nullFlagLength += 2
			} else {
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
			FieldName: [11]byte{0x5F, 0x4E, 0x75, 0x6C, 0x6C, 0x46, 0x6C, 0x61, 0x67, 0x73},
			DataType:  0x30,
			Position:  uint32(file.header.RowLength),
			Length:    uint8(length),
			Decimals:  0,
			Flag:      0x05,
			Next:      0x00,
			Step:      0x00,
			Reserved:  [7]byte{},
		}
		file.header.FirstRow += 32
		file.header.RowLength += uint16(length)
		debugf("Initializing null flag column - length: %v", length)
	}
	// Create the files
	file, err := create(file)
	if err != nil {
		return nil, err
	}
	// Write the headers
	err = file.writeHeader()
	if err != nil {
		return nil, err
	}
	// Write the columns
	err = file.writeColumns()
	if err != nil {
		return nil, err
	}
	// Write the memo header
	if file.memoHeader != nil {
		err = file.writeMemoHeader()
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

// Create a new table column with the given name, type and length
func NewColumn(name string, dataType DataType, length uint8, decimals uint8, nullable bool) (*Column, error) {
	if len(name) == 0 {
		return nil, errors.New("no column name defined")
	}
	if len(name) > 10 {
		return nil, newError("dbase-table-newcolumn-1", errors.New("column name can only be 10 characters long"))
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
	case Character:
		if length > 254 {
			return nil, newError("dbase-table-newcolumn-2", errors.New("character length can only be 254 bytes long"))
		}
		if length == 0 {
			return nil, newError("dbase-table-newcolumn-3", errors.New("character length can not be 0"))
		}
		column.Length = length
	case Varbinary:
		if length > 254 {
			return nil, newError("dbase-table-newcolumn-5", errors.New("varbinary length can only be 254 bytes long"))
		}
		if length == 0 {
			return nil, newError("dbase-table-newcolumn-6", errors.New("varbinary length can not be 0"))
		}
		column.Length = length
		column.Flag |= byte(BinaryFlag)
	case Varchar:
		if length > 254 {
			return nil, newError("dbase-table-newcolumn-6", errors.New("varchar length can only be 254 bytes long"))
		}
		if length == 0 {
			return nil, newError("dbase-table-newcolumn-7", errors.New("varchar length can not be 0"))
		}
		column.Length = length
	case Numeric:
		if length > 20 {
			return nil, newError("dbase-table-newcolumn-3", errors.New("numeric length can only be 20 bytes long"))
		}
		if length == 0 {
			return nil, newError("dbase-table-newcolumn-4", errors.New("numeric length can not be 0"))
		}
		column.Length = length
	case Float:
		if length > 20 {
			return nil, newError("dbase-table-newcolumn-4", errors.New("float length can only be 20 bytes long"))
		}
		if length == 0 {
			return nil, newError("dbase-table-newcolumn-5", errors.New("float length can not be 0"))
		}
		column.Length = length
	case Logical:
		column.Length = 1
	case Integer, Memo:
		column.Length = 4
	case Currency, Date, DateTime, Double:
		column.Length = 8
	default:
		return nil, newError("dbase-table-newcolumn-2", errors.New("invalid data type"))
	}
	return column, nil
}

/**
 *	################################################################
 *	#					dBase header helpers
 *	################################################################
 */

// Parses the year, month and day to time.Time.
// Note: the year is stored in 2 digits, so we assume the year is between 2000 and 2099.
func (h *Header) Modified() time.Time {
	return time.Date(2000+int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.Local)
}

// Returns the calculated number of columns from the header info alone (without the need to read the columninfo from the header).
// This is the fastest way to determine the number of rows in the file.
// Note: when Open is used the columns have already been parsed so it is better to call DBF.ColumnsCount() in that case.
func (h *Header) ColumnsCount() uint16 {
	return (h.FirstRow - 296) / 32
}

// Returns the amount of records in the table
func (h *Header) RecordsCount() uint32 {
	return h.RowsCount
}

// Returns the calculated file size based on the header info
func (h *Header) FileSize() int64 {
	return 296 + int64(h.ColumnsCount()*32) + int64(h.RowsCount*uint32(h.RowLength))
}

/**
 *	################################################################
 *	#						DBF helper
 *	################################################################
 */

// Returns if the internal row pointer is at end of file
func (file *File) EOF() bool {
	return file.table.rowPointer >= file.header.RowsCount
}

// Returns if the internal row pointer is before first row
func (file *File) BOF() bool {
	return file.table.rowPointer == 0
}

// Returns the current row pointer position
func (file *File) Pointer() uint32 {
	return file.table.rowPointer
}

// Returns the dBase database file header struct for inspecting
func (file *File) Header() *Header {
	return file.header
}

// returns the number of rows
func (file *File) RowsCount() uint32 {
	return file.header.RowsCount
}

// Returns all columns
func (file *File) Columns() []*Column {
	return file.table.columns
}

// Returns the requested column
func (file *File) Column(pos int) *Column {
	if pos < 0 || pos >= len(file.table.columns) {
		return nil
	}
	return file.table.columns[pos]
}

// Returns the number of columns
func (file *File) ColumnsCount() uint16 {
	return uint16(len(file.table.columns))
}

// Returns a slice of all the column names
func (file *File) ColumnNames() []string {
	num := len(file.table.columns)
	names := make([]string, num)
	for i := 0; i < num; i++ {
		names[i] = file.table.columns[i].Name()
	}
	return names
}

// Returns the column position of a column by name or -1 if not found.
func (file *File) ColumnPosByName(colname string) int {
	for i := 0; i < len(file.table.columns); i++ {
		if file.table.columns[i].Name() == colname {
			return i
		}
	}
	return -1
}

// Returns the column position of a column or -1 if not found.
func (file *File) ColumnPos(column *Column) int {
	for i := 0; i < len(file.table.columns); i++ {
		if file.table.columns[i] == column {
			return i
		}
	}
	return -1
}

/**
 *	################################################################
 *	#						Database helper
 *	################################################################
 */

// Returns all table of the database
func (db *Database) Tables() map[string]*File {
	return db.tables
}

// Returns the names of every table in the database
func (db *Database) Names() []string {
	names := make([]string, 0)
	for name := range db.tables {
		names = append(names, name)
	}
	return names
}

// Returns the complete database schema
func (db *Database) Schema() map[string][]*Column {
	schema := make(map[string][]*Column)
	for name, table := range db.tables {
		schema[name] = table.Columns()
	}
	return schema
}

/**
 *	################################################################
 *	#						Modifications
 *	################################################################
 */

// SetColumnModification sets a modification for a column
func (file *File) SetColumnModification(position int, mod *Modification) {
	// Skip if position is out of range
	if position < 0 || position >= len(file.table.columns) {
		return
	}
	debugf("Modification set for column %d", position)
	file.table.mods[position] = mod
}

func (file *File) SetColumnModificationByName(name string, mod *Modification) error {
	position := file.ColumnPosByName(name)
	if position < 0 {
		return newError("dbase-table-setcolumnmodificationbyname-1", fmt.Errorf("Column '%s' not found", name))
	}
	file.SetColumnModification(position, mod)
	return nil
}

// Returns the column modification for a column at the given position
func (file *File) GetColumnModification(position int) *Modification {
	return file.table.mods[position]
}

/**
 *	################################################################
 *	#						ColumnHeader helper
 *	################################################################
 */

// Returns the name of the column as a trimmed string (max length 10)
func (c *Column) Name() string {
	return string(bytes.TrimRight(c.FieldName[:], "\x00"))
}

// Returns the type of the column as string (length 1)
func (c *Column) Type() string {
	return string(c.DataType)
}

/**
 *	################################################################
 *	#						Rows
 *	################################################################
 */

// Returns all rows as a slice
func (file *File) Rows(skipInvalid bool, skipDeleted bool) ([]*Row, error) {
	rows := make([]*Row, 0)
	for !file.EOF() {
		// This reads the complete row
		row, err := file.Row()
		if err != nil && !skipInvalid {
			return nil, newError("dbase-table-rows-1", err)
		}
		// Increment the row pointer
		file.Skip(1)
		// skip deleted rows
		if row.Deleted && skipDeleted {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Returns the requested row at file.rowPointer.
func (file *File) Row() (*Row, error) {
	data, err := file.readRow(file.table.rowPointer)
	if err != nil {
		return nil, newError("dbase-table-row-1", err)
	}
	return file.BytesToRow(data)
}

// Returns a new Row struct with the same column structure as the dbf and the next row pointer
func (file *File) NewRow() *Row {
	row := &Row{
		handle:   file,
		Position: file.header.RowsCount + 1,
		Deleted:  false,
		fields:   make([]*Field, 0),
	}
	for _, column := range file.table.columns {
		row.fields = append(row.fields, &Field{
			column: column,
			value:  nil,
		})
	}
	debugf("Initiliazing new at position %d", row.Position)
	return row
}

// Creates a new field with the given value and column
func (file *File) NewField(pos int, value interface{}) (*Field, error) {
	column := file.Column(pos)
	if column == nil {
		return nil, newError("dbase-table-newfield-1", fmt.Errorf("column at position %v not found", pos))
	}
	return &Field{column: column, value: value}, nil
}

// Creates a new field with the given value and column specified by name
func (file *File) NewFieldByName(name string, value interface{}) (*Field, error) {
	pos := file.ColumnPosByName(name)
	if pos < 0 {
		return nil, newError("dbase-table-newfieldbyname-1", fmt.Errorf("column '%s' not found", name))
	}
	return file.NewField(pos, value)
}

// Writes the row to the file at the row pointer position
func (row *Row) Write() error {
	return row.writeRow()
}

// Increments the pointer s row to the end of the file
func (row *Row) Add() error {
	row.Position = row.handle.header.RowsCount + 1
	return row.Write()
}

// Returns all values of a row as a slice of interface{}
func (row *Row) Values() []interface{} {
	values := make([]interface{}, 0)
	for _, field := range row.fields {
		values = append(values, field.value)
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

/**
 *	################################################################
 *	#						Conversions
 *	################################################################
 */

// Converts raw row data to a Row struct
// If the data points to a memo (FPT) file this file is also read
func (file *File) BytesToRow(data []byte) (*Row, error) {
	debugf("Converting row data (%d bytes) to row struct...", len(data))
	rec := &Row{}
	rec.Position = file.table.rowPointer
	rec.handle = file
	rec.fields = make([]*Field, file.ColumnsCount())
	if len(data) < int(file.header.RowLength) {
		return nil, newError("dbase-table-bytestorow-1", fmt.Errorf("invalid row data size %v Bytes < %v Bytes", len(data), int(file.header.RowLength)))
	}
	// a row should start with te delete flag, a space ACTIVE(0x20) or DELETED(0x2A)
	rec.Deleted = Marker(data[0]) == Deleted
	if !rec.Deleted && Marker(data[0]) != Active {
		return nil, newError("dbase-table-bytestorow-2", fmt.Errorf("invalid row data, no delete flag found at beginning of row"))
	}
	// deleted flag already read
	offset := uint16(1)
	for i := 0; i < len(rec.fields); i++ {
		column := file.table.columns[i]
		val, err := file.dataToValue(data[offset:offset+uint16(column.Length)], file.table.columns[i])
		if err != nil {
			return rec, newError("dbase-table-bytestorow-3", err)
		}
		rec.fields[i] = &Field{
			column: column,
			value:  val,
		}
		offset += uint16(column.Length)
	}
	return rec, nil
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
	for _, field := range row.fields {
		val, err := row.handle.valueToByteRepresentation(field, false)
		if err != nil {
			return nil, newError("dbase-table-rowtobytes-1", err)
		}
		copy(data[offset:offset+uint16(field.column.Length)], val)
		offset += uint16(field.column.Length)
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
		mod := row.handle.table.mods[i]
		if row.handle.config.TrimSpaces {
			if str, ok := val.(string); ok {
				debugf("Trimming spaces from field %v due to default settings", field.Name())
				val = strings.TrimSpace(str)
			}
		}
		if mod != nil {
			if mod.TrimSpaces {
				if str, ok := val.(string); ok {
					debugf("Trimming spaces from field %v due to modification", field.Name())
					val = strings.TrimSpace(str)
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

// Parses the row from map to JSON-encoded and from there to a struct and stores the result in the value pointed to by v.
// Just a convenience function to avoid the intermediate JSON step.
func (row *Row) ToStruct(v interface{}) error {
	debugf("Converting row %v to struct...", row.Position)
	jsonRow, err := row.ToJSON()
	if err != nil {
		return newError("dbase-table-tostruct-1", err)
	}
	err = json.Unmarshal(jsonRow, v)
	if err != nil {
		return newError("dbase-table-tostruct-2", err)
	}
	return nil
}

// Converts a map of interfaces into the row representation
func (file *File) RowFromMap(m map[string]interface{}) (*Row, error) {
	debugf("Converting map to row...")
	row := file.NewRow()
	for i := range row.fields {
		field := &Field{column: file.table.columns[i]}
		mod := file.table.mods[i]
		if mod != nil {
			if len(mod.ExternalKey) != 0 {
				if val, ok := m[mod.ExternalKey]; ok {
					debugf("Resolving external key %v for field %v due to modification", mod.ExternalKey, field.Name())
					field.value = val
					row.fields[i] = field
					continue
				}
			}
		}
		if val, ok := m[field.Name()]; ok {
			field.value = val
		}
		row.fields[i] = field
	}
	return row, nil
}

// Converts a JSON-encoded row into the row representation
func (file *File) RowFromJSON(j []byte) (*Row, error) {
	debugf("Converting JSON to row...")
	m := make(map[string]interface{})
	err := json.Unmarshal(j, &m)
	if err != nil {
		return nil, newError("dbase-table-fromjson-1", err)
	}
	row, err := file.RowFromMap(m)
	if err != nil {
		return nil, newError("dbase-table-fromjson-2", err)
	}
	return row, nil
}

// Converts a struct into the row representation
func (file *File) RowFromStruct(v interface{}) (*Row, error) {
	debugf("Converting struct to row...")
	j, err := json.Marshal(v)
	if err != nil {
		return nil, newError("dbase-table-fromstruct-1", err)
	}
	row, err := file.RowFromJSON(j)
	if err != nil {
		return nil, newError("dbase-table-fromstruct-2", err)
	}
	return row, nil
}
