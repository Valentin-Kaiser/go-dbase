package dbase

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// File is the main struct to handle a dBase file.
// Each file type is basically a Table or a Memo file.
type File struct {
	config         *Config     // The config used when working with the DBF file.
	handle         interface{} // DBase file handle.
	relatedHandle  interface{} // Memo file handle.
	io             IO          // The IO interface used to work with the DBF file.
	header         *Header     // DBase file header containing relevant information.
	memoHeader     *MemoHeader // Memo file header containing relevant information.
	dbaseMutex     *sync.Mutex // Mutex locks for concurrent writing access to the DBF file.
	memoMutex      *sync.Mutex // Mutex locks for concurrent writing access to the FPT file.
	table          *Table      // Containing the columns and internal row pointer.
	nullFlagColumn *Column     // The column containing the null flag column (if varchar or varbinary field exists).
}

// Opens a dBase database file (and the memo file if needed).
// The config parameter is required to specify the file path, encoding, file handles (IO) and others.
// If IO is nil, the default implementation is used depending on the OS.
func OpenTable(config *Config) (*File, error) {
	if config.IO == nil {
		config.IO = DefaultIO
	}
	return config.IO.OpenTable(config)
}

// Closes all file handlers.
func (file *File) Close() error {
	return file.defaults().io.Close(file)
}

// Creates a new dBase database file (and the memo file if needed).
func (file *File) Create() error {
	return file.defaults().io.Create(file)
}

// Reads the DBF header from the file handle.
func (file *File) ReadHeader() error {
	return file.defaults().io.ReadHeader(file)
}

// WriteHeader writes the header to the dbase file.
func (file *File) WriteHeader() error {
	return file.defaults().io.WriteHeader(file)
}

// ReadColumns reads from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func (file *File) ReadColumns() ([]*Column, *Column, error) {
	return file.defaults().io.ReadColumns(file)
}

// WriteColumns writes the columns at the end of header in dbase file
func (file *File) WriteColumns() error {
	return file.defaults().io.WriteColumns(file)
}

// ReadMemoHeader reads the memo header from the given file handle.
func (file *File) ReadMemoHeader() error {
	return file.defaults().io.ReadMemoHeader(file)
}

// WriteMemoHeader writes the memo header to the memo file.
// Size is the number of blocks the new memo data will take up.
func (file *File) WriteMemoHeader(size int) error {
	return file.defaults().io.WriteMemoHeader(file, size)
}

// Reads raw row data of one row at rowPosition
func (file *File) ReadRow(position uint32) ([]byte, error) {
	return file.defaults().io.ReadRow(file, position)
}

// WriteRow writes a raw row data to the given row position
func (file *File) WriteRow(row *Row) error {
	return file.defaults().io.WriteRow(file, row)
}

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (file *File) ReadMemo(address []byte) ([]byte, bool, error) {
	return file.defaults().io.ReadMemo(file, address)
}

// WriteMemo writes a memo to the memo file and returns the address of the memo.
func (file *File) WriteMemo(data []byte, text bool, length int) ([]byte, error) {
	return file.defaults().io.WriteMemo(file, data, text, length)
}

// Read the nullFlag field at the end of the row
// The nullFlag field indicates if the field has a variable length
// If varlength is true, the field is variable length and the length is stored in the last byte
// If varlength is false, we read the complete field
// If the field is null, we return true as second return value
func (file *File) ReadNullFlag(position uint64, column *Column) (bool, bool, error) {
	return file.defaults().io.ReadNullFlag(file, position, column)
}

// Search searches for a row with the given value in the given field
func (file *File) Search(field *Field, exactMatch bool) ([]*Row, error) {
	return file.defaults().io.Search(file, field, exactMatch)
}

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (file *File) GoTo(row uint32) error {
	return file.defaults().io.GoTo(file, row)
}

// Skip adds offset to the internal row pointer
// If at end of file positions the pointer at lastRow+1
// If the row pointer would be become negative positions the pointer at 0
// Does not skip deleted rows
func (file *File) Skip(offset int64) {
	file.defaults().io.Skip(file, offset)
}

// Returns if the row at internal row pointer is deleted
func (file *File) Deleted() (bool, error) {
	return file.defaults().io.Deleted(file)
}

// Returns the used IO implementation
func (file *File) GetIO() IO {
	return file.io
}

// Returns the used file handle (DBF,FPT)
func (file *File) GetHandle() (interface{}, interface{}) {
	return file.handle, file.relatedHandle
}

// Sets the default if no io is set
func (file *File) defaults() *File {
	if file.io == nil {
		file.io = DefaultIO
	}
	return file
}

// Check if the file version is tested
func ValidateFileVersion(version byte, untested bool) error {
	if untested {
		return nil
	}
	debugf("Validating file version: %d", version)
	switch version {
	default:
		return newError("dbase-io-validatefileversion-1", fmt.Errorf("untested DBF file version: %d (0x%x)", version, version))
	case byte(FoxPro), byte(FoxProAutoincrement), byte(FoxProVar):
		return nil
	}
}

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

// Returns the dBase table file header struct for inspecting
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

// Returns all rows as a slice
func (file *File) Rows(skipInvalid bool, skipDeleted bool) ([]*Row, error) {
	rows := make([]*Row, 0)
	for !file.EOF() {
		row, err := file.Next()
		if err != nil {
			if skipInvalid {
				continue
			}
			return nil, newError("dbase-table-rows-1", err)
		}

		// skip deleted rows
		if row.Deleted && skipDeleted {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Reads the row and increments the row pointer by one
func (file *File) Next() (*Row, error) {
	row, err := file.Row()
	file.Skip(1)
	if err != nil {
		return nil, newError("dbase-table-next-1", err)
	}
	return row, err
}

// Returns the requested row at file.rowPointer.
func (file *File) Row() (*Row, error) {
	data, err := file.ReadRow(file.table.rowPointer)
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

// Converts raw row data to a Row struct
// If the data points to a memo (FPT) file this file is also read
func (file *File) BytesToRow(data []byte) (*Row, error) {
	debugf("Converting row data (%d bytes) to row struct...", len(data))
	rec := &Row{}
	rec.Position = file.table.rowPointer
	rec.handle = file
	rec.fields = make([]*Field, 0)
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
	for i := 0; i < int(file.ColumnsCount()); i++ {
		column := file.table.columns[i]
		val, err := file.Interpret(data[offset:offset+uint16(column.Length)], file.table.columns[i])
		if err != nil {
			return rec, newError("dbase-table-bytestorow-3", err)
		}
		if file.config.TrimSpaces {
			if str, ok := val.(string); ok {
				val = strings.TrimSpace(str)
			}
		}
		rec.fields = append(rec.fields, &Field{
			column: column,
			value:  val,
		})
		offset += uint16(column.Length)
	}
	return rec, nil
}

// Converts a map of interfaces into the row representation
func (file *File) RowFromMap(m map[string]interface{}) (*Row, error) {
	debugf("Converting map to row...")
	row := file.NewRow()
	for i := range row.fields {
		field := &Field{column: file.table.columns[i]}
		if i >= 0 && i < len(file.table.mods) {
			if mod := file.table.mods[i]; mod != nil {
				if len(mod.ExternalKey) != 0 {
					if val, ok := m[mod.ExternalKey]; ok {
						debugf("Resolving external key %v for field %v due to modification", mod.ExternalKey, field.Name())
						field.value = val
						row.fields[i] = field
						continue
					}
				}
			}
		}
		if val, ok := m[field.Name()]; ok {
			field.value = val
		}
		row.fields[i] = field
	}
	err := row.Increment()
	if err != nil {
		return nil, newError("dbase-file-rowfrommap-1", err)
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
// The struct must have the same field names as the columns in the table or the dbase tag must be set.
// The dbase tag can be used to name the field. For example: `dbase:"my_field_name"`
func (file *File) RowFromStruct(v interface{}) (*Row, error) {
	debugf("Converting struct to row...")
	m := make(map[string]interface{})
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("dbase")
		if len(tag) == 0 {
			tag = field.Name
		}
		m[tag] = rv.Field(i).Interface()
	}
	row, err := file.RowFromMap(m)
	if err != nil {
		return nil, newError("dbase-table-fromstruct-1", err)
	}
	return row, nil
}
