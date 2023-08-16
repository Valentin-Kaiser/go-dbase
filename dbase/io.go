package dbase

// IO is the interface to work with the DBF file.
// Three implementations are available:
// - WindowsIO (for direct file access with Windows)
// - UnixIO (for direct file access with Unix)
// - GenericIO (for any custom file access implementing io.ReadWriteSeeker)
// The IO interface can be implemented for any custom file access.
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
