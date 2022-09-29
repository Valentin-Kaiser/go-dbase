//go:build windows
// +build windows

package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"

	syscall "golang.org/x/sys/windows"
)

// DBF is the main struct to handle a dBase file.
type DBF struct {
	// The used converter instance passed by opening a file
	convert EncodingConverter
	// DBase and memo file syscall handle pointer
	dbaseFileHandle *syscall.Handle
	memoFileHandle  *syscall.Handle
	// DBase and memo file header containing relevant information
	header     *Header
	memoHeader *MemoHeader
	// Containing the columns and internal row pointer
	table *Table
}

/**
 *	################################################################
 *	#					Stream and File handler
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func Open(filename string, conv EncodingConverter) (*DBF, error) {
	filename = filepath.Clean(filename)
	// Open file in non blocking mode with syscall
	fd, err := syscall.Open(filename, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-1:FAILED:%w", err)
	}
	dbf, err := prepareDBF(fd, conv)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-2:FAILED:%w", err)
	}
	dbf.dbaseFileHandle = &fd
	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.header.TableFlags & Memo) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		fd, err := syscall.Open(strings.TrimSuffix(filename, ext)+fptExt, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-3:FAILED:%w", err)
		}
		err = dbf.prepareMemo(fd)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-4:FAILED:%w", err)
		}
		dbf.memoFileHandle = &fd
	}
	return dbf, nil
}

// Closes the file handlers.
func (dbf *DBF) Close() error {
	if dbf.dbaseFileHandle != nil {
		err := syscall.Close(*dbf.dbaseFileHandle)
		if err != nil {
			return fmt.Errorf("dbase-io-close-1:FAILED:Closing DBF failed with error: %w", err)
		}
	}
	if dbf.memoFileHandle != nil {
		err := syscall.Close(*dbf.memoFileHandle)
		if err != nil {
			return fmt.Errorf("dbase-io-close-2:FAILED:Closing FPT failed with error: %w", err)
		}
	}
	return nil
}

/**
 *	################################################################
 *	#				dBase database file handler
 *	################################################################
 */

// Returns a DBF object pointer
// Reads the DBF Header, the column infos and validates file version.
func prepareDBF(fd syscall.Handle, conv EncodingConverter) (*DBF, error) {
	header, err := readDBFHeader(fd)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-1:FAILED:%w", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType); err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-2:FAILED:%w", err)
	}
	columns, err := readColumnInfos(fd)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-3:FAILED:%w", err)
	}
	dbf := &DBF{
		header:          header,
		dbaseFileHandle: &fd,
		table: &Table{
			columns:    columns,
			columnMods: make([]*ColumnModification, len(columns)),
		},
		convert: conv,
	}
	return dbf, nil
}

// Reads the DBF header from the file handle.
func readDBFHeader(fd syscall.Handle) (*Header, error) {
	h := &Header{}
	if _, err := syscall.Seek(fd, 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-1:FAILED:%w", err)
	}
	b := make([]byte, 1024)
	n, err := syscall.Read(fd, b)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-2:FAILED:%w", err)
	}
	// LittleEndian - Integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-3:FAILED:%w", err)
	}
	return h, nil
}

// Reads raw column data of one column at columnPosition at rowPosition
func (dbf *DBF) readColumn(rowPosition uint32, columnPosition int) ([]byte, error) {
	if rowPosition >= dbf.header.RowsCount {
		return nil, fmt.Errorf("dbase-io-readcolumn-1:FAILED:%v", EOF)
	}
	if columnPosition < 0 || columnPosition > int(dbf.ColumnsCount()) {
		return nil, fmt.Errorf("dbase-io-readcolumn-2:FAILED:%v", InvalidPosition)
	}
	buf := make([]byte, dbf.table.columns[columnPosition].Length)
	pos := int64(dbf.header.FirstRow) + (int64(rowPosition) * int64(dbf.header.RowLength)) + int64(dbf.table.columns[columnPosition].Position)
	_, err := syscall.Seek(*dbf.dbaseFileHandle, pos, 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-io-readcolumn-3:FAILED:%w", err)
	}
	read, err := syscall.Read(*dbf.dbaseFileHandle, buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-io-readcolumn-4:FAILED:%w", err)
	}
	if read != int(dbf.table.columns[columnPosition].Length) {
		return buf, fmt.Errorf("dbase-io-readcolumn-5:FAILED:%v", Incomplete)
	}
	return buf, nil
}

// Reads column infos from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func readColumnInfos(fd syscall.Handle) ([]*Column, error) {
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := syscall.Seek(fd, offset, 0); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-1:FAILED:%w", err)
		}
		if _, err := syscall.Read(fd, b); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-2:FAILED:%w", err)
		}
		if b[0] == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := syscall.Seek(fd, -1, 1); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-3:FAILED:%w", err)
		}
		buf := make([]byte, 2048)
		n, err := syscall.Read(fd, buf)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-4:FAILED:%w", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-5:FAILED:%w", err)
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

// Reads raw row data of one row at rowPosition
func (dbf *DBF) readRow(rowPosition uint32) ([]byte, error) {
	if rowPosition >= dbf.header.RowsCount {
		return nil, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", EOF)
	}
	buf := make([]byte, dbf.header.RowLength)
	_, err := syscall.Seek(*dbf.dbaseFileHandle, int64(dbf.header.FirstRow)+(int64(rowPosition)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-2:FAILED:%w", err)
	}
	read, err := syscall.Read(*dbf.dbaseFileHandle, buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-3:FAILED:%w", err)
	}
	if read != int(dbf.header.RowLength) {
		return buf, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", Incomplete)
	}
	return buf, nil
}

// prepareMemo prepares the memo file for reading.
func (dbf *DBF) prepareMemo(fd syscall.Handle) error {
	memoHeader, err := readMemoHeader(fd)
	if err != nil {
		return fmt.Errorf("dbase-table-prepare-memo-1:FAILED:%w", err)
	}
	dbf.memoFileHandle = &fd
	dbf.memoHeader = memoHeader
	return nil
}

// readMemoHeader reads the memo header from the given file handle.
func readMemoHeader(fd syscall.Handle) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := syscall.Seek(fd, 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-1:FAILED:%w", err)
	}
	b := make([]byte, 1024)
	n, err := syscall.Read(fd, b)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-2:FAILED:%w", err)
	}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-3:FAILED:%w", err)
	}
	return h, nil
}

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readMemo(blockdata []byte) ([]byte, bool, error) {
	if dbf.memoFileHandle == nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-1:FAILED:%v", NoFPT)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	_, err := syscall.Seek(*dbf.memoFileHandle, int64(dbf.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-2:FAILED:%w", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = syscall.Read(*dbf.memoFileHandle, hbuf)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-3:FAILED:%w", err)
	}
	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])
	if leng == 0 {
		// No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}
	// Now read the actual data
	buf := make([]byte, leng)
	read, err := syscall.Read(*dbf.memoFileHandle, buf)
	if err != nil {
		return buf, false, fmt.Errorf("dbase-io-readmemo-4:FAILED:%w", err)
	}
	if read != int(leng) {
		return buf, sign == 1, fmt.Errorf("dbase-io-readmemo-5:FAILED:%v", Incomplete)
	}
	return buf, sign == 1, nil
}

func validateFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("dbase-io-validatefileversion-1:FAILED:untested DBF file version: %d (%x hex)", version, version)
	case FoxPro, FoxProAutoincrement:
		return nil
	}
}

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (dbf *DBF) GoTo(rowNumber uint32) error {
	if rowNumber > dbf.header.RowsCount {
		dbf.table.rowPointer = dbf.header.RowsCount
		return fmt.Errorf("dbase-io-goto-1:FAILED:go to %v > %v:%v", rowNumber, dbf.header.RowsCount, EOF)
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

// Returns if the row at internal row pointer is deleted
func (dbf *DBF) Deleted() (bool, error) {
	if dbf.table.rowPointer >= dbf.header.RowsCount {
		return false, fmt.Errorf("dbase-interpreter-deleted-1:FAILED:%v", EOF)
	}
	_, err := syscall.Seek(*dbf.dbaseFileHandle, int64(dbf.header.FirstRow)+(int64(dbf.table.rowPointer)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-2:FAILED:%w", err)
	}
	buf := make([]byte, 1)
	read, err := syscall.Read(*dbf.dbaseFileHandle, buf)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-3:FAILED:%w", err)
	}
	if read != 1 {
		return false, fmt.Errorf("dbase-interpreter-deleted-4:FAILED:%v", Incomplete)
	}
	return buf[0] == Deleted, nil
}
