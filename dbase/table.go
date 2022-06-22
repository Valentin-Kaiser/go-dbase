package dbase

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"syscall"
	"time"
)

const (
	yearOffset      = 1900
	null       byte = 0x00
	blank      byte = 0x20

	columnNameByteLength         = 11
	maxUsableNameByteLength      = columnNameByteLength - 1
	endOfColumnNameMarker   byte = 0x0

	rowDeletionFlagIndex = 0
	rowIsActive          = blank
	rowIsDeleted         = 0x2A

	eofMarker byte = 0x1A
)

// Containing all raw DBF header columns.
type DBaseHeader struct {
	FileVersion byte     // File type flag
	Year        uint8    // Last update year (0-99)
	Month       uint8    // Last update month
	Day         uint8    // Last update day
	RowsCount   uint32   // Number of rows in file
	FirstRow    uint16   // Position of first data row
	RowLength   uint16   // Length of one data row, including delete flag
	Reserved    [16]byte // Reserved
	TableFlags  byte     // Table flags
	CodePage    byte     // Code page mark
}

// The raw header of the Memo file.
type MemoHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

type Table struct {
	columns []Column

	rowPointer uint32 // Internal row pointer, can be moved
}

// Contains the raw column info structure from the DBF header.
type Column struct {
	ColumnName [11]byte // Column name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
	DataType   byte     // Column type
	Position   uint32   // Displacement of column in row
	Length     uint8    // Length of column (in bytes)
	Decimals   uint8    // Number of decimal places
	Flags      byte     // Column flags
	Next       uint32   // Value of autoincrement Next value
	Step       uint16   // Value of autoincrement Step value
	Reserved   [8]byte  // Reserved
}

// Contains the raw row data and a deleted flag
type Row struct {
	DBF     *DBF
	Deleted bool
	Data    []interface{}
}

/**
 *	################################################################
 *	#					dBase header helpers
 *	################################################################
 */

// Parses the year, month and day to time.Time.
// Note: the year is stored in 2 digits, 15 is 2015
func (h *DBaseHeader) Modified() time.Time {
	return time.Date(2000+int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.Local)
}

// Returns the calculated number of columns from the header info alone (without the need to read the columninfo from the header).
// This is the fastest way to determine the number of rows in the file.
// Note: when OpenFile is used the columns have already been parsed so it is better to call DBF.ColumnsCount in that case.
func (h *DBaseHeader) ColumnsCount() uint16 {
	return uint16((h.FirstRow - 296) / 32)
}

// Returns the calculated file size based on the header info
func (h *DBaseHeader) FileSize() int64 {
	return 296 + int64(h.ColumnsCount()*32) + int64(h.RowsCount*uint32(h.RowLength))
}

/**
 *	################################################################
 *	#					dBase memo helper
 *	################################################################
 */

func (dbf *DBF) prepareMemo(fd syscall.Handle) error {
	memoHeader, err := readMemoHeader(fd)
	if err != nil {
		return fmt.Errorf("dbase-table-prepare-memo-1:FAILED:%v", err)

	}

	dbf.memoFileHandle = &fd
	dbf.memoHeader = memoHeader
	return nil
}

func readMemoHeader(fd syscall.Handle) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := syscall.Seek(syscall.Handle(fd), 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-1:FAILED:%v", err)
	}

	b := make([]byte, 1024)
	n, err := syscall.Read(syscall.Handle(fd), b)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-2:FAILED:%v", err)
	}

	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-3:FAILED:%v", err)
	}
	return h, nil
}

/**
 *	################################################################
 *	#						DBF helper
 *	################################################################
 */

// Returns if the internal row pointer is at end of file
func (dbf *DBF) EOF() bool {
	return dbf.table.rowPointer >= dbf.dbaseHeader.RowsCount
}

// Returns if the internal row pointer is before first row
func (dbf *DBF) BOF() bool {
	return dbf.table.rowPointer == 0
}

// Returns the dBase database file header struct for inspecting
func (dbf *DBF) Header() *DBaseHeader {
	return dbf.dbaseHeader
}

// returns the number of rows
func (dbf *DBF) RowsCount() uint32 {
	return dbf.dbaseHeader.RowsCount
}

// Returns all columns
func (dbf *DBF) Columns() []Column {
	return dbf.table.columns
}

// Returns the number of columns
func (dbf *DBF) ColumnsCount() uint16 {
	return uint16(len(dbf.table.columns))
}

// Returns a slice of all the column names
func (dbf *DBF) ColumnNames() []string {
	num := len(dbf.table.columns)
	names := make([]string, num)
	for i := 0; i < num; i++ {
		names[i] = dbf.table.columns[i].Name()
	}
	return names
}

// Returns the column position of a column name or -1 if not found.
func (dbf *DBF) ColumnPos(colname string) int {
	for i := 0; i < len(dbf.table.columns); i++ {
		if dbf.table.columns[i].Name() == colname {
			return i
		}
	}
	return -1
}

// Reads column number columnposition at the row number the internal pointer is pointing to and returns its Go value
func (dbf *DBF) Value(columnposition int) (interface{}, error) {
	data, err := dbf.readColumn(dbf.table.rowPointer, columnposition)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-value-1:FAILED:%v", err)
	}
	// columnposition is valid or readColumn would have returned an error
	return dbf.ColumnDataToValue(data, columnposition)
}

// Parses a memo file from raw []byte, decodes and returns as []byte
func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-1:FAILED:%v", err)
	}
	if isText {
		memo, err = dbf.convert.Decode(memo)
		if err != nil {
			return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-2:FAILED:%v", err)
		}
	}
	return memo, isText, nil
}

/**
 *	################################################################
 *	#						ColumnHeader helper
 *	################################################################
 */

// Returns the name of the column as a trimmed string (max length 10)
func (f *Column) Name() string {
	return string(bytes.TrimRight(f.ColumnName[:], "\x00"))
}

// Returns the type of the column as string (length 1)
func (f *Column) Type() string {
	return string(f.DataType)
}

// Reads raw row data of one row at rowPosition
func (dbf *DBF) readRow(rowPosition uint32) ([]byte, error) {
	if rowPosition >= dbf.dbaseHeader.RowsCount {
		return nil, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", ERROR_EOF.AsError())
	}
	buf := make([]byte, dbf.dbaseHeader.RowLength)

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.dbaseHeader.FirstRow)+(int64(rowPosition)*int64(dbf.dbaseHeader.RowLength)), 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-2:FAILED:%v", err)
	}

	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-3:FAILED:%v", err)
	}

	if read != int(dbf.dbaseHeader.RowLength) {
		return buf, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, nil
}

// Returns all rows
func (dbf *DBF) Rows(skipInvalid bool) ([]*Row, error) {
	rows := make([]*Row, 0)
	for !dbf.EOF() {
		// This reads the complete row
		row, err := dbf.Row()
		if err != nil && !skipInvalid {
			return nil, fmt.Errorf("dbase-table-rows-1:FAILED:%v", err)
		}

		dbf.Skip(1)
		// skip deleted rows
		if row.Deleted {
			continue
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// Returns the requested row at dbf.rowPointer.
func (dbf *DBF) Row() (*Row, error) {
	data, err := dbf.readRow(dbf.table.rowPointer)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-get-row-1:FAILED:%v", err)
	}

	return dbf.BytesToRow(data)
}

/**
 *	################################################################
 *	#						Row conversion
 *	################################################################
 */

// Returns all rows as a slice of maps.
func (dbf *DBF) RowsToMap(skipInvalid bool) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0)

	rows, err := dbf.Rows(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		rmap, err := row.ToMap()
		if err != nil {
			return nil, err
		}

		out = append(out, rmap)
	}

	return out, nil
}

// Returns all rows as json
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the row map are re-assigned)
func (dbf *DBF) RowsToJSON(skipInvalid bool, trimspaces bool) ([]byte, error) {
	rows, err := dbf.RowsToMap(skipInvalid)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-to-json-1:FAILED:%v", err)
	}

	mapRows := make([]map[string]interface{}, 0)
	for _, row := range rows {
		if trimspaces {
			for k, v := range row {
				if str, ok := v.(string); ok {
					row[k] = strings.TrimSpace(str)
				}
			}
		}
		mapRows = append(mapRows, row)
	}

	return json.Marshal(mapRows)
}

// Returns all rows as a slice of struct.
// Parses the row from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the row into a struct, json.Unmarshal matches incoming object keys to either the struct column name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
// v keeps the last converted struct.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the row map are re-assigned)
func (dbf *DBF) RowsToStruct(v interface{}, skipInvalid bool, trimspaces bool) ([]interface{}, error) {
	out := make([]interface{}, 0)

	rows, err := dbf.Rows(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		err := row.ToStruct(v, trimspaces)
		if err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, nil
}

// Returns a complete row as a map.
func (rec *Row) ToMap() (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for i, fn := range rec.DBF.ColumnNames() {
		val, err := rec.Column(i)
		if err != nil {
			return out, fmt.Errorf("dbase-table-to-map-1:FAILED:error on column %s (column %d): %s", fn, i, err)
		}
		out[fn] = val
	}
	return out, nil
}

// Returns a complete row as a JSON object.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the row map are re-assigned)
func (rec *Row) ToJSON(trimspaces bool) ([]byte, error) {
	m, err := rec.ToMap()
	if err != nil {
		return nil, fmt.Errorf("dbase-table-to-json-1:FAILED:%v", err)
	}
	if trimspaces {
		for k, v := range m {
			if str, ok := v.(string); ok {
				m[k] = strings.TrimSpace(str)
			}
		}
	}
	return json.Marshal(m)
}

// Parses the row from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the row into a struct, json.Unmarshal matches incoming object keys to either the struct column name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
func (rec *Row) ToStruct(v interface{}, trimspaces bool) error {
	jsonRow, err := rec.ToJSON(trimspaces)
	if err != nil {
		return fmt.Errorf("dbase-table-to-struct-1:FAILED:%v", err)
	}

	err = json.Unmarshal(jsonRow, v)
	if err != nil {
		return fmt.Errorf("dbase-table-to-struct-2:FAILED:%v", err)
	}

	return nil
}

// Column gets a columns value by column pos (index)
func (r *Row) Column(pos int) (interface{}, error) {
	if pos < 0 || len(r.Data) < pos {
		return 0, fmt.Errorf("dbase-table-column-1:FAILED:%v", ERROR_INVALID.AsError())
	}
	return r.Data[pos], nil
}

// ColumnSlice gets all columns as a slice
func (r *Row) ColumnSlice() []interface{} {
	return r.Data
}
