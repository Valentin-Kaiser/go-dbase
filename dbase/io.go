package dbase

import (
	"fmt"
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

// IO is the interface to work with the DBF file.
// Three implementations are available:
// - WindowsIO (for direct file access with Windows)
// - UnixIO (for direct file access with Unix)
// - GenericIO (for any custom file access implementing io.ReadWriteSeeker)
type IO interface {
	OpenTable(config *Config) (*File, error)
	Close(file *File) error
	Create(file *File) error
	ReadHeader(file *File) error
	WriteHeader(file *File) error
	ReadColumns(file *File) ([]*Column, *Column, error)
	WriteColumns(file *File) error
	ReadMemoHeader(file *File) error
	WriteMemoHeader(file *File, size int) error
	ReadMemo(file *File, address []byte) ([]byte, bool, error)
	WriteMemo(file *File, raw []byte, text bool, length int) ([]byte, error)
	ReadNullFlag(file *File, position uint64, column *Column) (bool, bool, error)
	ReadRow(file *File, position uint32) ([]byte, error)
	WriteRow(file *File, row *Row) error
	Search(file *File, field *Field, exactMatch bool) ([]*Row, error)
	GoTo(file *File, row uint32) error
	Skip(file *File, offset int64)
	Deleted(file *File) (bool, error)
}

// Opens a dBase database file (and the memo file if needed).
// Uses the specified io implementation. If nil, the default io implementation is used depending on the OS.
func OpenTable(config *Config, io IO) (*File, error) {
	if io == nil {
		io = defaultIO
	}
	return io.OpenTable(config)
}

// Closes all file handlers.
func (file *File) Close() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Close(file)
}

// Creates a new dBase database file (and the memo file if needed).
func (file *File) Create() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Create(file)
}

// Reads the DBF header from the file handle.
func (file *File) ReadHeader() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadHeader(file)
}

// WriteHeader writes the header to the dbase file.
func (file *File) WriteHeader() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteHeader(file)
}

// ReadColumns reads from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func (file *File) ReadColumns() ([]*Column, *Column, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadColumns(file)
}

// WriteColumns writes the columns at the end of header in dbase file
func (file *File) WriteColumns() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteColumns(file)
}

// ReadMemoHeader reads the memo header from the given file handle.
func (file *File) ReadMemoHeader() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadMemoHeader(file)
}

// WriteMemoHeader writes the memo header to the memo file.
// Size is the number of blocks the new memo data will take up.
func (file *File) WriteMemoHeader(size int) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteMemoHeader(file, size)
}

// Reads raw row data of one row at rowPosition
func (file *File) ReadRow(position uint32) ([]byte, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadRow(file, position)
}

// WriteRow writes a raw row data to the given row position
func (file *File) WriteRow(row *Row) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteRow(file, row)
}

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (file *File) ReadMemo(address []byte) ([]byte, bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadMemo(file, address)
}

// WriteMemo writes a memo to the memo file and returns the address of the memo.
func (file *File) WriteMemo(data []byte, text bool, length int) ([]byte, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteMemo(file, data, text, length)
}

// Read the nullFlag field at the end of the row
// The nullFlag field indicates if the field has a variable length
// If varlength is true, the field is variable length and the length is stored in the last byte
// If varlength is false, we read the complete field
// If the field is null, we return true as second return value
func (file *File) ReadNullFlag(position uint64, column *Column) (bool, bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadNullFlag(file, position, column)
}

// Search searches for a row with the given value in the given field
func (file *File) Search(field *Field, exactMatch bool) ([]*Row, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Search(file, field, exactMatch)
}

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (file *File) GoTo(row uint32) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.GoTo(file, row)
}

// Skip adds offset to the internal row pointer
// If at end of file positions the pointer at lastRow+1
// If the row pointer would be become negative positions the pointer at 0
// Does not skip deleted rows
func (file *File) Skip(offset int64) {
	if file.io == nil {
		file.io = defaultIO
	}
	file.io.Skip(file, offset)
}

// Returns if the row at internal row pointer is deleted
func (file *File) Deleted() (bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Deleted(file)
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
