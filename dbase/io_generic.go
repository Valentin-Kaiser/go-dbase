package dbase

/**
 *	################################################################
 *	#					IO Functions
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func OpenTable(config *Config) (*File, error) {
	return _openTable(config)
}

// Closes the file handlers.
func (file *File) Close() error {
	return file._close()
}

func (file *File) Search(field *Field, exactMatch bool) ([]*Row, error) {
	return file._search(field, exactMatch)
}

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (file *File) GoTo(row uint32) error {
	return file._goTo(row)
}

// Skip adds offset to the internal row pointer
// If at end of file positions the pointer at lastRow+1
// If the row pointer would be become negative positions the pointer at 0
// Does not skip deleted rows
func (file *File) Skip(offset int64) {
	file._skip(offset)
}

// Whether or not the write operations should lock the record
func (file *File) WriteLock(enabled bool) {
	file._writeLock(enabled)
}

// Returns if the row at internal row pointer is deleted
func (file *File) Deleted() (bool, error) {
	return file._deleted()
}

func create(file *File) (*File, error) {
	return _create(file)
}

func (file *File) writeHeader() (err error) {
	return file._writeHeader()
}

func (file *File) writeColumns() (err error) {
	return file._writeColumns()
}

func (file *File) writeMemoHeader(size int) (err error) {
	return file._writeMemoHeader(size)
}

func (file *File) readRow(position uint32) ([]byte, error) {
	return file._readRow(position)
}

func (row *Row) writeRow() (err error) {
	return row._writeRow()
}

func (file *File) readMemo(address []byte) ([]byte, bool, error) {
	return file._readMemo(address)
}

func (file *File) writeMemo(raw []byte, text bool, length int) ([]byte, error) {
	return file._writeMemo(raw, text, length)
}

func (file *File) readNullFlag(rowPosition uint64, column *Column) (bool, bool, error) {
	return file._readNullFlag(rowPosition, column)
}
