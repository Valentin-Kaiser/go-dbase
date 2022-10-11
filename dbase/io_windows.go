//go:build windows
// +build windows

package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/windows"
)

type Config struct {
	Filename           string            // The filename of the DBF file.
	Converter          EncodingConverter // The encoding converter to use.
	Exclusive          bool              // If true the file is opened in exclusive mode.
	Untested           bool              // If true the file version is not checked.
	TrimSpaces         bool              // Trimspaces default value
	WriteLock          bool              // Whether or not the write operations should lock the record
	CodePageValidation bool              // Whether or not the code page mark should be validated
}

// DBF is the main struct to handle a dBase file.
type DBF struct {
	config          *Config         // The config used when working with the DBF file.
	dbaseFileHandle *windows.Handle // DBase file windows handle pointer
	memoFileHandle  *windows.Handle // Memo file windows handle pointer
	header          *Header         // DBase file header containing relevant information
	memoHeader      *MemoHeader     // Memo file header containing relevant information
	dbaseMutex      *sync.Mutex     // Mutex locks for concurrent writing access to the DBF file
	memoMutex       *sync.Mutex     // Mutex locks for concurrent writing access to the FPT file
	table           *Table          // Containing the columns and internal row pointer
}

/**
 *	################################################################
 *	#					IO Functions
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the file handle(s) call DBF.Close().
func Open(config *Config) (*DBF, error) {
	if config == nil {
		return nil, newError("dbase-io-open-1", fmt.Errorf("missing configuration"))
	}
	if config.Filename == "" {
		return nil, newError("dbase-io-open-2", fmt.Errorf("missing filename"))
	}
	if config.Converter == nil {
		return nil, newError("dbase-io-open-3", fmt.Errorf("missing encoding converter"))
	}
	filename := filepath.Clean(config.Filename)
	mode := windows.O_RDWR | windows.O_CLOEXEC | windows.O_NONBLOCK
	if config.Exclusive {
		mode = windows.O_RDWR | windows.O_CLOEXEC | windows.O_EXCL
	}
	fd, err := windows.Open(filename, mode, 0644)
	if err != nil {
		return nil, newError("dbase-io-open-1", fmt.Errorf("opening DBF file failed with error: %w", err))
	}
	dbf, err := prepareDBF(fd, config)
	if err != nil {
		return nil, newError("dbase-io-open-2", err)
	}
	dbf.dbaseFileHandle = &fd
	// Check if the code page mark is matchin the converter
	if config.CodePageValidation && dbf.header.CodePage != dbf.config.Converter.CodePageMark() {
		return nil, newError("dbase-io-open-3", fmt.Errorf("code page mark mismatch: %d != %d", dbf.header.CodePage, dbf.config.Converter.CodePageMark()))
	}
	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.header.TableFlags & MemoFlag) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		fd, err := windows.Open(strings.TrimSuffix(filename, ext)+fptExt, mode, 0644)
		if err != nil {
			return nil, newError("dbase-io-open-4", fmt.Errorf("opening FPT file failed with error: %w", err))
		}
		err = dbf.prepareMemo(fd)
		if err != nil {
			return nil, newError("dbase-io-open-5", err)
		}
		dbf.memoFileHandle = &fd
	}
	return dbf, nil
}

// Closes the file handlers.
func (dbf *DBF) Close() error {
	if dbf.dbaseFileHandle != nil {
		err := windows.Close(*dbf.dbaseFileHandle)
		if err != nil {
			return newError("dbase-io-close-1", fmt.Errorf("closing DBF failed with error: %w", err))
		}
	}
	if dbf.memoFileHandle != nil {
		err := windows.Close(*dbf.memoFileHandle)
		if err != nil {
			return newError("dbase-io-close-2", fmt.Errorf("closing FPT failed with error: %w", err))
		}
	}
	return nil
}

/**
 *	################################################################
 *	#				dBase database file IO handler
 *	################################################################
 */

// Returns a DBF object pointer
// Reads the DBF Header, the column infos and validates file version.
func prepareDBF(fd windows.Handle, config *Config) (*DBF, error) {
	header, err := readHeader(fd)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-1", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType, config.Untested); err != nil {
		return nil, newError("dbase-io-preparedbf-2", err)
	}
	columns, err := readColumns(fd)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-3", err)
	}
	dbf := &DBF{
		config:          config,
		header:          header,
		dbaseFileHandle: &fd,
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, len(columns)),
		},
		dbaseMutex: &sync.Mutex{},
		memoMutex:  &sync.Mutex{},
	}
	return dbf, nil
}

// Reads the DBF header from the file handle.
func readHeader(fd windows.Handle) (*Header, error) {
	h := &Header{}
	if _, err := windows.Seek(fd, 0, 0); err != nil {
		return nil, newError("dbase-io-readdbfheader-1", err)
	}
	b := make([]byte, 1024)
	n, err := windows.Read(fd, b)
	if err != nil {
		return nil, newError("dbase-io-readdbfheader-2", err)
	}
	// LittleEndian - Integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return nil, newError("dbase-io-readdbfheader-3", err)
	}
	return h, nil
}

// writeHeader writes the header to the dbase file
func (dbf *DBF) writeHeader() (err error) {
	// Lock the block we are writing to
	position := uint32(0)
	o := &windows.Overlapped{
		Offset:     position,
		OffsetHigh: position + uint32(dbf.header.FirstRow),
	}
	// Lock the block we are writing to
	if dbf.config.WriteLock {
		err = windows.LockFileEx(*dbf.memoFileHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, position, position+uint32(dbf.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-writeheader-1", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*dbf.memoFileHandle, 0, position, position+uint32(dbf.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-writeheader-2", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*dbf.dbaseFileHandle, 0, 0)
	if err != nil {
		return newError("dbase-io-writeheader-3", err)
	}
	// Change the last modification date to the current date
	dbf.header.Year = uint8(time.Now().Year() - 2000)
	dbf.header.Month = uint8(time.Now().Month())
	dbf.header.Day = uint8(time.Now().Day())
	// Write the header
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, dbf.header)
	if err != nil {
		return newError("dbase-io-writeheader-4", err)
	}
	_, err = windows.Write(*dbf.dbaseFileHandle, buf.Bytes())
	if err != nil {
		return newError("dbase-io-writeheader-5", err)
	}
	return nil
}

// Check if the file version is supported
func validateFileVersion(version byte, untested bool) error {
	switch version {
	default:
		if untested {
			return nil
		}
		return newError("dbase-io-validatefileversion-1", fmt.Errorf("untested DBF file version: %d (0x%x)", version, version))
	case FoxPro, FoxProAutoincrement, FoxProVar:
		return nil
	}
}

// Reads columns from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func readColumns(fd windows.Handle) ([]*Column, error) {
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := windows.Seek(fd, offset, 0); err != nil {
			return nil, newError("dbase-io-readcolumninfos-1", err)
		}
		if _, err := windows.Read(fd, b); err != nil {
			return nil, newError("dbase-io-readcolumninfos-2", err)
		}
		if b[0] == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := windows.Seek(fd, -1, 1); err != nil {
			return nil, newError("dbase-io-readcolumninfos-3", err)
		}
		buf := make([]byte, 2048)
		n, err := windows.Read(fd, buf)
		if err != nil {
			return nil, newError("dbase-io-readcolumninfos-4", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, newError("dbase-io-readcolumninfos-5", err)
		}
		if column.Name() == "_NullFlags" {
			offset += 32
			continue
		}
		columns = append(columns, column)
		offset += 32
	}
	return columns, nil
}

/**
 *	################################################################
 *	#				Memo file IO handler
 *	################################################################
 */

// prepareMemo prepares the memo file for reading.
func (dbf *DBF) prepareMemo(fd windows.Handle) error {
	memoHeader, err := readMemoHeader(fd)
	if err != nil {
		return newError("dbase-io-prepare-memo-1", err)
	}
	dbf.memoFileHandle = &fd
	dbf.memoHeader = memoHeader
	return nil
}

// readMemoHeader reads the memo header from the given file handle.
func readMemoHeader(fd windows.Handle) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := windows.Seek(fd, 0, 0); err != nil {
		return nil, newError("dbase-io-read-memo-header-1", err)
	}
	b := make([]byte, 1024)
	n, err := windows.Read(fd, b)
	if err != nil {
		return nil, newError("dbase-io-read-memo-header-2", err)
	}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return nil, newError("dbase-io-read-memo-header-3", err)
	}
	return h, nil
}

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readMemo(address []byte) ([]byte, bool, error) {
	if dbf.memoFileHandle == nil {
		return nil, false, newError("dbase-io-readmemo-1", ErrNoFPT)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(address)
	// The position in the file is blocknumber*blocksize
	_, err := windows.Seek(*dbf.memoFileHandle, int64(dbf.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, newError("dbase-io-readmemo-2", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = windows.Read(*dbf.memoFileHandle, hbuf)
	if err != nil {
		return nil, false, newError("dbase-io-readmemo-3", err)
	}
	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])
	if leng == 0 {
		// No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}
	// Now read the actual data
	buf := make([]byte, leng)
	read, err := windows.Read(*dbf.memoFileHandle, buf)
	if err != nil {
		return buf, false, newError("dbase-io-readmemo-4", err)
	}
	if read != int(leng) {
		return buf, sign == 1, newError("dbase-io-readmemo-5", ErrIncomplete)
	}
	return buf, sign == 1, nil
}

// Parses a memo file from raw []byte, decodes and returns as []byte
func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, newError("dbase-io-parse-memo-1", err)
	}
	if isText {
		memo, err = dbf.config.Converter.Decode(memo)
		if err != nil {
			return []byte{}, false, newError("dbase-io-parse-memo-2", err)
		}
	}
	return memo, isText, nil
}

// writeMemo writes a memo to the memo file and returns the address of the memo.
func (dbf *DBF) writeMemo(raw []byte, text bool, length int) ([]byte, error) {
	dbf.memoMutex.Lock()
	defer dbf.memoMutex.Unlock()
	if dbf.memoFileHandle == nil {
		return nil, newError("dbase-io-writememo-1", ErrNoFPT)
	}
	// Get the block position
	blockPosition := dbf.memoHeader.NextFree
	// Write the memo header
	err := dbf.writeMemoHeader()
	if err != nil {
		return nil, newError("dbase-io-writememo-2", err)
	}
	// Put the block data together
	block := make([]byte, dbf.memoHeader.BlockSize)
	// The first 4 bytes are the signature, 1 for text, 0 for binary(image)
	if text {
		binary.BigEndian.PutUint32(block[:4], 1)
	} else {
		binary.BigEndian.PutUint32(block[:4], 0)
	}
	// The next 4 bytes are the length of the data
	binary.BigEndian.PutUint32(block[4:8], uint32(length))
	// The rest is the data
	copy(block[8:], raw)
	// Lock the block we are writing to
	if dbf.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     blockPosition,
			OffsetHigh: blockPosition + uint32(dbf.memoHeader.BlockSize),
		}
		err = windows.LockFileEx(*dbf.memoFileHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, blockPosition, blockPosition+uint32(dbf.memoHeader.BlockSize), o)
		if err != nil {
			return nil, newError("dbase-io-writememo-3", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*dbf.memoFileHandle, 0, blockPosition, blockPosition+uint32(dbf.memoHeader.BlockSize), o)
			if err != nil {
				err = newError("dbase-io-writememoheader-4", ulockErr)
			}
		}()
	}
	// Seek to new the next free block
	_, err = windows.Seek(*dbf.memoFileHandle, int64(blockPosition)*int64(dbf.memoHeader.BlockSize), 0)
	if err != nil {
		return nil, newError("dbase-io-writememo-5", err)
	}
	// Write the memo data
	_, err = windows.Write(*dbf.memoFileHandle, block)
	if err != nil {
		return nil, newError("dbase-io-writememo-6", err)
	}
	// Convert the block number to []byte
	address, err := toBinary(blockPosition)
	if err != nil {
		return nil, newError("dbase-io-writememo-7", err)
	}
	return address, nil
}

// writeMemoHeader writes the memo header to the memo file.
func (dbf *DBF) writeMemoHeader() (err error) {
	if dbf.memoFileHandle == nil {
		return newError("dbase-io-writememoheader-1", ErrNoFPT)
	}
	// Lock the block we are writing to
	o := &windows.Overlapped{
		Offset:     0,
		OffsetHigh: uint32(dbf.header.FirstRow),
	}
	if dbf.config.WriteLock {
		err = windows.LockFileEx(*dbf.memoFileHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 0, uint32(dbf.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-writememoheader-2", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*dbf.memoFileHandle, 0, 0, uint32(dbf.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-writememoheader-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*dbf.memoFileHandle, 0, 0)
	if err != nil {
		return newError("dbase-io-writememoheader-4", err)
	}
	// Calculate the next free block
	dbf.memoHeader.NextFree++
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], dbf.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], dbf.memoHeader.BlockSize)
	_, err = windows.Write(*dbf.memoFileHandle, buf)
	if err != nil {
		return newError("dbase-io-writememoheader-5", err)
	}
	return nil
}

/**
 *	################################################################
 *	#				Row and Field IO handler
 *	################################################################
 */

// Reads raw row data of one row at rowPosition
func (dbf *DBF) readRow(rowPosition uint32) ([]byte, error) {
	if rowPosition >= dbf.header.RowsCount {
		return nil, newError("dbase-io-readrow-1", ErrEOF)
	}
	buf := make([]byte, dbf.header.RowLength)
	_, err := windows.Seek(*dbf.dbaseFileHandle, int64(dbf.header.FirstRow)+(int64(rowPosition)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return buf, newError("dbase-io-readrow-2", err)
	}
	read, err := windows.Read(*dbf.dbaseFileHandle, buf)
	if err != nil {
		return buf, newError("dbase-io-readrow-3", err)
	}
	if read != int(dbf.header.RowLength) {
		return buf, newError("dbase-io-readrow-4", ErrIncomplete)
	}
	return buf, nil
}

// writeRow writes raw row data to the given row position
func (row *Row) writeRow() (err error) {
	row.dbf.dbaseMutex.Lock()
	defer row.dbf.dbaseMutex.Unlock()
	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return newError("dbase-io-writerow-1", err)
	}
	// Update the header
	position := int64(row.dbf.header.FirstRow) + (int64(row.Position) * int64(row.dbf.header.RowLength))
	if row.Position >= row.dbf.header.RowsCount {
		position = int64(row.dbf.header.FirstRow) + (int64(row.Position-1) * int64(row.dbf.header.RowLength))
		row.dbf.header.RowsCount++
	}
	err = row.dbf.writeHeader()
	if err != nil {
		return newError("dbase-io-writerow-2", err)
	}
	// Lock the block we are writing to
	if row.dbf.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     uint32(position),
			OffsetHigh: uint32(position + int64(row.dbf.header.RowLength)),
		}
		err = windows.LockFileEx(*row.dbf.dbaseFileHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, uint32(position), uint32(position+int64(row.dbf.header.RowLength)), o)
		if err != nil {
			return newError("dbase-io-writerow-3", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*row.dbf.dbaseFileHandle, 0, uint32(position), uint32(position+int64(row.dbf.header.RowLength)), o)
			if err != nil {
				err = newError("dbase-io-writerow-4", ulockErr)
			}
		}()
	}
	// Seek to the correct position
	_, err = windows.Seek(*row.dbf.dbaseFileHandle, position, 0)
	if err != nil {
		return newError("dbase-io-writerow-5", err)
	}
	// Write the row
	_, err = windows.Write(*row.dbf.dbaseFileHandle, r)
	if err != nil {
		return newError("dbase-io-writerow-6", err)
	}
	return nil
}

/**
 *	################################################################
 *	#						Search
 *	################################################################
 */

// Search searches for a row with the given value in the given field
func (dbf *DBF) Search(field *Field, exactMatch bool) ([]*Row, error) {
	if field.column.DataType == 'M' {
		return nil, newError("dbase-io-search-1", fmt.Errorf("searching memo fields is not supported"))
	}
	// convert the value to a string
	val, err := dbf.valueToByteRepresentation(field, !exactMatch)
	if err != nil {
		return nil, newError("dbase-io-search-1", err)
	}
	// Search for the value
	rows := make([]*Row, 0)
	position := uint64(dbf.header.FirstRow)
	for i := uint32(0); i < dbf.header.RowsCount; i++ {
		// Read the field value
		_, err := windows.Seek(*dbf.dbaseFileHandle, int64(position)+int64(field.column.Position), 0)
		position += uint64(dbf.header.RowLength)
		if err != nil {
			continue
		}
		buf := make([]byte, field.column.Length)
		read, err := windows.Read(*dbf.dbaseFileHandle, buf)
		if err != nil {
			continue
		}
		if read != int(field.column.Length) {
			continue
		}
		// Check if the value matches
		if bytes.Contains(buf, val) {
			err := dbf.GoTo(i)
			if err != nil {
				continue
			}
			row, err := dbf.Row()
			if err != nil {
				continue
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

/**
 *	################################################################
 *	#				General DBF handler
 *	################################################################
 */

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (dbf *DBF) GoTo(rowNumber uint32) error {
	if rowNumber > dbf.header.RowsCount {
		dbf.table.rowPointer = dbf.header.RowsCount
		return newError("dbase-io-goto-1", fmt.Errorf("%w, go to %v > %v", ErrEOF, rowNumber, dbf.header.RowsCount))
	}
	dbf.table.rowPointer = rowNumber
	return nil
}

// Skip adds offset to the internal row pointer
// If at end of file positions the pointer at lastRow+1
// If the row pointer would be become negative positions the pointer at 0
// Does not skip deleted rows
func (dbf *DBF) Skip(offset int64) {
	newval := int64(dbf.table.rowPointer) + offset
	if newval >= int64(dbf.header.RowsCount) {
		dbf.table.rowPointer = dbf.header.RowsCount
	}
	if newval < 0 {
		dbf.table.rowPointer = 0
	}
	dbf.table.rowPointer = uint32(newval)
}

// Whether or not the write operations should lock the record
func (dbf *DBF) WriteLock(enabled bool) {
	dbf.config.WriteLock = enabled
}

// Returns if the row at internal row pointer is deleted
func (dbf *DBF) Deleted() (bool, error) {
	if dbf.table.rowPointer >= dbf.header.RowsCount {
		return false, newError("dbase-io-deleted-1", ErrEOF)
	}
	_, err := windows.Seek(*dbf.dbaseFileHandle, int64(dbf.header.FirstRow)+(int64(dbf.table.rowPointer)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return false, newError("dbase-io-deleted-2", err)
	}
	buf := make([]byte, 1)
	read, err := windows.Read(*dbf.dbaseFileHandle, buf)
	if err != nil {
		return false, newError("dbase-io-deleted-3", err)
	}
	if read != 1 {
		return false, newError("dbase-io-deleted-4", ErrIncomplete)
	}
	return buf[0] == Deleted, nil
}
