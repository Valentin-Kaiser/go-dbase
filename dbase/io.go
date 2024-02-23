package dbase

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
		return NewErrorf("untested DBF file version: %d (0x%x)", version, version)
	case byte(FoxPro), byte(FoxProAutoincrement), byte(FoxProVar):
		return nil
	}
}
