package dbase

import (
	"fmt"
	"sync"
)

// File is the main struct to handle a dBase file.
// Each file type is basically a Table or a Memo file.
type File struct {
	config         *Config     // The config used when working with the DBF file.
	handle         interface{} // DBase file windows handle pointer.
	relatedHandle  interface{} // Memo file windows handle pointer.
	io             IO          // IO struct containing the file handles.
	header         *Header     // DBase file header containing relevant information.
	memoHeader     *MemoHeader // Memo file header containing relevant information.
	dbaseMutex     *sync.Mutex // Mutex locks for concurrent writing access to the DBF file.
	memoMutex      *sync.Mutex // Mutex locks for concurrent writing access to the FPT file.
	table          *Table      // Containing the columns and internal row pointer.
	nullFlagColumn *Column     // The column containing the null flag column (if varchar or varbinary field exists).
}

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

func OpenTable(config *Config, io IO) (*File, error) {
	if io == nil {
		io = defaultIO
	}
	return io.OpenTable(config)
}

func (file *File) Close() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Close(file)
}

func (file *File) Create() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Create(file)
}

func (file *File) WriteHeader() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteHeader(file)
}

func (file *File) WriteColumns() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteColumns(file)
}

func (file *File) ReadMemoHeader() error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadMemoHeader(file)
}

func (file *File) WriteMemoHeader(size int) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteMemoHeader(file, size)
}

func (file *File) ReadRow(position uint32) ([]byte, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadRow(file, position)
}

func (file *File) WriteRow(row *Row) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteRow(file, row)
}

func (file *File) ReadMemo(address []byte) ([]byte, bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadMemo(file, address)
}

func (file *File) WriteMemo(data []byte, text bool, length int) ([]byte, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.WriteMemo(file, data, text, length)
}

func (file *File) ReadNullFlag(position uint64, column *Column) (bool, bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.ReadNullFlag(file, position, column)
}

func (file *File) Search(field *Field, exactMatch bool) ([]*Row, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Search(file, field, exactMatch)
}

func (file *File) GoTo(row uint32) error {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.GoTo(file, row)
}

func (file *File) Skip(offset int64) {
	if file.io == nil {
		file.io = defaultIO
	}
	file.io.Skip(file, offset)
}

func (file *File) Deleted() (bool, error) {
	if file.io == nil {
		file.io = defaultIO
	}
	return file.io.Deleted(file)
}

// Check if the file version is supported
func validateFileVersion(version byte, untested bool) error {
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
