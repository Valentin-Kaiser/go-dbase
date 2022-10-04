//go:build !windows
// +build !windows

package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DBF is the main struct to handle a dBase file.
type DBF struct {
	// The used converter instance passed by opening a file
	converter EncodingConverter
	// DBase and memo file syscall handle pointer
	dbaseFile *os.File
	memoFile  *os.File
	// DBase and memo file header containing relevant information
	header     *Header
	memoHeader *MemoHeader
	// 	// Mutex locks
	dbaseMutex *sync.Mutex
	memoMutex  *sync.Mutex
	// Containing the columns and internal row pointer
	table *Table
}

// The raw header of the Memo file.
type MemoHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

/**
 *	################################################################
 *	#					IO Functions
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func Open(filename string, conv EncodingConverter, useUntested bool) (*DBF, error) {
	filename = filepath.Clean(filename)
	dbaseFile, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-1:FAILED:%w", err)
	}
	dbf, err := prepareDBF(dbaseFile, conv, useUntested)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-2:FAILED:%w", err)
	}
	dbf.dbaseFile = dbaseFile
	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.header.TableFlags & Memo) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		memoFile, err := os.OpenFile(strings.TrimSuffix(filename, ext)+fptExt, os.O_RDWR, 0600)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-3:FAILED:%w", err)
		}
		err = dbf.prepareMemo(memoFile)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-4:FAILED:%w", err)
		}
		dbf.memoFile = memoFile
	}
	return dbf, nil
}

// Closes the file handlers.
func (dbf *DBF) Close() error {
	if dbf.dbaseFile != nil {
		err := dbf.dbaseFile.Close()
		if err != nil {
			return fmt.Errorf("dbase-io-close-1:FAILED:Closing DBF failed with error: %w", err)
		}
	}
	if dbf.memoFile != nil {
		err := dbf.memoFile.Close()
		if err != nil {
			return fmt.Errorf("dbase-io-close-2:FAILED:Closing FPT failed with error: %w", err)
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
func prepareDBF(dbaseFile *os.File, conv EncodingConverter, useUntested bool) (*DBF, error) {
	header, err := readHeader(dbaseFile)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-1:FAILED:%w", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType, useUntested); err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-2:FAILED:%w", err)
	}
	columns, err := readColumns(dbaseFile)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-3:FAILED:%w", err)
	}
	dbf := &DBF{
		header:    header,
		dbaseFile: dbaseFile,
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, len(columns)),
		},
		dbaseMutex: &sync.Mutex{},
		memoMutex:  &sync.Mutex{},
		converter:  conv,
	}
	return dbf, nil
}

// Reads the DBF header from the file handle.
func readHeader(dbaseFile *os.File) (*Header, error) {
	h := &Header{}
	if _, err := dbaseFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-1:FAILED:%w", err)
	}
	b := make([]byte, 1024)
	n, err := dbaseFile.Read(b)
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

// writeHeader writes the header to the dbase file
func (dbf *DBF) writeHeader() error {
	// Seek to the beginning of the file
	_, err := dbf.dbaseFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("dbase-table-write-header-1:FAILED:%w", err)
	}
	// Change the last modification date to the current date
	dbf.header.Year = uint8(time.Now().Year() - 2000)
	dbf.header.Month = uint8(time.Now().Month())
	dbf.header.Day = uint8(time.Now().Day())
	// Write the header
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, dbf.header)
	if err != nil {
		return fmt.Errorf("dbase-table-write-header-2:FAILED:%w", err)
	}
	_, err = dbf.dbaseFile.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("dbase-table-write-header-3:FAILED:%w", err)
	}
	return nil
}

// Check if the file version is supported
func validateFileVersion(version byte, useUntested bool) error {
	switch version {
	default:
		if useUntested {
			return nil
		}
		return fmt.Errorf("dbase-io-validatefileversion-1:FAILED:untested DBF file version: %d (%x hex)", version, version)
	case FoxPro, FoxProAutoincrement:
		return nil
	}
}

// Reads column infos from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func readColumns(dbaseFile *os.File) ([]*Column, error) {
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := dbaseFile.Seek(offset, 0); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumns-1:FAILED:%w", err)
		}
		if _, err := dbaseFile.Read(b); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumns-2:FAILED:%w", err)
		}
		if b[0] == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := dbaseFile.Seek(-1, 1); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumns-3:FAILED:%w", err)
		}
		buf := make([]byte, 2048)
		n, err := dbaseFile.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumns-4:FAILED:%w", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumns-5:FAILED:%w", err)
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
func (dbf *DBF) prepareMemo(memoFile *os.File) error {
	memoHeader, err := readMemoHeader(memoFile)
	if err != nil {
		return fmt.Errorf("dbase-table-prepare-memo-1:FAILED:%w", err)
	}
	dbf.memoFile = memoFile
	dbf.memoHeader = memoHeader
	return nil
}

// readMemoHeader reads the memo header from the given file handle.
func readMemoHeader(memoFile *os.File) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := memoFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-1:FAILED:%w", err)
	}
	b := make([]byte, 1024)
	n, err := memoFile.Read(b)
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
	if dbf.memoFile == nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-1:FAILED:%v", NoFPT)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	_, err := dbf.memoFile.Seek(int64(dbf.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-2:FAILED:%w", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = dbf.memoFile.Read(hbuf)
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
	read, err := dbf.memoFile.Read(buf)
	if err != nil {
		return buf, false, fmt.Errorf("dbase-io-readmemo-4:FAILED:%w", err)
	}
	if read != int(leng) {
		return buf, sign == 1, fmt.Errorf("dbase-io-readmemo-5:FAILED:%v", Incomplete)
	}
	return buf, sign == 1, nil
}

// Parses a memo file from raw []byte, decodes and returns as []byte
func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-1:FAILED:%w", err)
	}
	if isText {
		memo, err = dbf.converter.Decode(memo)
		if err != nil {
			return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-2:FAILED:%w", err)
		}
	}
	return memo, isText, nil
}

// writeMemo writes a memo to the memo file and returns the address of the memo.
func (dbf *DBF) writeMemo(raw []byte, text bool, length int) ([]byte, error) {
	dbf.memoMutex.Lock()
	defer dbf.memoMutex.Unlock()

	if dbf.memoFile == nil {
		return nil, fmt.Errorf("dbase-io-writememo-1:FAILED:%v", NoFPT)
	}
	// Get the block position
	blockPosition := dbf.memoHeader.NextFree
	// Write the memo header
	err := dbf.writeMemoHeader()
	if err != nil {
		return nil, fmt.Errorf("dbase-io-writememo-2:FAILED:%w", err)
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
	// Seek to new the next free block
	_, err = dbf.memoFile.Seek(int64(blockPosition)*int64(dbf.memoHeader.BlockSize), 0)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-writememop-3:FAILED:%w", err)
	}
	// Write the memo data
	_, err = dbf.memoFile.Write(block)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-writememo-4:FAILED:%w", err)
	}
	// Convert the block number to []byte
	address, err := toBinary(blockPosition)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-writememo-5:FAILED:%w", err)
	}
	return address, nil
}

// writeMemoHeader writes the memo header to the memo file.
func (dbf *DBF) writeMemoHeader() error {
	if dbf.memoFile == nil {
		return fmt.Errorf("dbase-io-writememoheader-1:FAILED:%v", NoFPT)
	}
	// Seek to the beginning of the file
	_, err := dbf.memoFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("dbase-io-writememoheader-2:FAILED:%w", err)
	}
	// Calculate the next free block
	dbf.memoHeader.NextFree++
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], dbf.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], dbf.memoHeader.BlockSize)
	_, err = dbf.memoFile.Write(buf)
	if err != nil {
		return fmt.Errorf("dbase-io-writememoheader-4:FAILED:%w", err)
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
		return nil, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", EOF)
	}
	buf := make([]byte, dbf.header.RowLength)
	_, err := dbf.dbaseFile.Seek(int64(dbf.header.FirstRow)+(int64(rowPosition)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-2:FAILED:%w", err)
	}
	read, err := dbf.dbaseFile.Read(buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-table-read-row-3:FAILED:%w", err)
	}
	if read != int(dbf.header.RowLength) {
		return buf, fmt.Errorf("dbase-table-read-row-1:FAILED:%v", Incomplete)
	}
	return buf, nil
}

// writeRow writes raw row data to the given row position
func (row *Row) writeRow() error {
	row.dbf.dbaseMutex.Lock()
	defer row.dbf.dbaseMutex.Unlock()
	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return fmt.Errorf("dbase-table-write-row-1:FAILED:%w", err)
	}
	// Update the header
	position := int64(row.dbf.header.FirstRow) + (int64(row.Position) * int64(row.dbf.header.RowLength))
	if row.Position >= row.dbf.header.RowsCount {
		position = int64(row.dbf.header.FirstRow) + (int64(row.Position-1) * int64(row.dbf.header.RowLength))
		row.dbf.header.RowsCount++
	}
	err = row.dbf.writeHeader()
	if err != nil {
		return fmt.Errorf("dbase-table-write-row-2:FAILED:%w", err)
	}
	// Seek to the correct position
	_, err = row.dbf.dbaseFile.Seek(position, 0)
	if err != nil {
		return fmt.Errorf("dbase-table-write-row-3:FAILED:%w", err)
	}
	// Write the row
	_, err = row.dbf.dbaseFile.Write(r)
	if err != nil {
		return fmt.Errorf("dbase-table-write-row-4:FAILED:%w", err)
	}
	return nil
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
	_, err := dbf.dbaseFile.Seek(int64(dbf.header.FirstRow)+(int64(dbf.table.rowPointer)*int64(dbf.header.RowLength)), 0)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-2:FAILED:%w", err)
	}
	buf := make([]byte, 1)
	read, err := dbf.dbaseFile.Read(buf)
	if err != nil {
		return false, fmt.Errorf("dbase-interpreter-deleted-3:FAILED:%w", err)
	}
	if read != 1 {
		return false, fmt.Errorf("dbase-interpreter-deleted-4:FAILED:%v", Incomplete)
	}
	return buf[0] == Deleted, nil
}
