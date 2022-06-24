package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
)

type DBF struct {
	// the used converter instance passed by opening a file
	convert EncodingConverter
	// dBase and memo file syscall handle pointer
	dbaseFileHandle *syscall.Handle
	memoFileHandle  *syscall.Handle
	// dBase and memo file header containing relevant information
	dbaseHeader *DBaseHeader
	memoHeader  *MemoHeader
	// containing the columns and internal row pointer
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

	// open file in non blocking mode with syscall
	fd, err := syscall.Open(filename, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-1:FAILED:%v", err)
	}

	dbf, err := prepareDBF(fd, conv)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-open-2:FAILED:%v", err)
	}

	dbf.dbaseFileHandle = &fd

	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.dbaseHeader.TableFlags & MEMO) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		fd, err := syscall.Open(strings.TrimSuffix(filename, ext)+fptExt, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-3:FAILED:%v", err)
		}

		err = dbf.prepareMemo(fd)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-open-4:FAILED:%v", err)
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
			return fmt.Errorf("dbase-io-close-1:FAILED:Closing DBF failed with error: %v", err)
		}
	}

	if dbf.memoFileHandle != nil {
		err := syscall.Close(*dbf.memoFileHandle)
		if err != nil {
			return fmt.Errorf("dbase-io-close-2:FAILED:Closing FPT failed with error: %v", err)
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
		return nil, fmt.Errorf("dbase-io-preparedbf-1:FAILED:%v", err)
	}

	// check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType); err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-2:FAILED:%v", err)
	}

	// read columninfo
	columns, err := readColumnInfos(fd)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-preparedbf-3:FAILED:%v", err)
	}

	dbf := &DBF{
		dbaseHeader:     header,
		dbaseFileHandle: &fd,
		table: &Table{
			columns:    columns,
			columnMods: make([]*ColumnModification, len(columns)),
		},
		convert: conv,
	}
	return dbf, nil
}

func readDBFHeader(fd syscall.Handle) (*DBaseHeader, error) {
	h := &DBaseHeader{}
	if _, err := syscall.Seek(syscall.Handle(fd), 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-1:FAILED:%v", err)
	}

	b := make([]byte, 1024)
	n, err := syscall.Read(syscall.Handle(fd), b)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-2:FAILED:%v", err)
	}

	// integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-io-readdbfheader-3:FAILED:%v", err)
	}
	return h, nil
}

// Reads raw column data of one column at columnPosition at rowPosition
func (dbf *DBF) readColumn(rowPosition uint32, columnPosition int) ([]byte, error) {
	if rowPosition >= dbf.dbaseHeader.RowsCount {
		return nil, fmt.Errorf("dbase-io-readcolumn-1:FAILED:%v", ERROR_EOF.AsError())
	}

	if columnPosition < 0 || columnPosition > int(dbf.ColumnsCount()) {
		return nil, fmt.Errorf("dbase-io-readcolumn-2:FAILED:%v", ERROR_INVALID.AsError())
	}

	buf := make([]byte, dbf.table.columns[columnPosition].Length)
	pos := int64(dbf.dbaseHeader.FirstRow) + (int64(rowPosition) * int64(dbf.dbaseHeader.RowLength)) + int64(dbf.table.columns[columnPosition].Position)

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), pos, 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-io-readcolumn-3:FAILED:%v", err)
	}

	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-io-readcolumn-4:FAILED:%v", err)
	}

	if read != int(dbf.table.columns[columnPosition].Length) {
		return buf, fmt.Errorf("dbase-io-readcolumn-5:FAILED:%v", ERROR_INCOMPLETE.AsError())
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
		if _, err := syscall.Seek(syscall.Handle(fd), offset, 0); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-1:FAILED:%v", err)
		}
		if _, err := syscall.Read(syscall.Handle(fd), b); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-2:FAILED:%v", err)
		}
		if b[0] == END_OF_COLUMN {
			break
		}

		// Position back one byte and read the column
		if _, err := syscall.Seek(syscall.Handle(fd), -1, 1); err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-3:FAILED:%v", err)
		}

		buf := make([]byte, 2048)
		n, err := syscall.Read(syscall.Handle(fd), buf)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-4:FAILED:%v", err)
		}

		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, fmt.Errorf("dbase-io-readcolumninfos-5:FAILED:%v", err)
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

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readMemo(blockdata []byte) ([]byte, bool, error) {
	if dbf.memoFileHandle == nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-1:FAILED:%v", ERROR_NO_FPT_FILE.AsError())
	}

	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	_, err := syscall.Seek(syscall.Handle(*dbf.memoFileHandle), int64(dbf.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-2:FAILED:%v", err)
	}

	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	read, err := syscall.Read(syscall.Handle(*dbf.memoFileHandle), hbuf)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-io-readmemo-3:FAILED:%v", err)
	}

	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])
	if leng == 0 {
		// No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}

	// Now read the actual data
	buf := make([]byte, leng)
	read, err = syscall.Read(syscall.Handle(*dbf.memoFileHandle), buf)
	if err != nil {
		return buf, false, fmt.Errorf("dbase-io-readmemo-4:FAILED:%v", err)
	}
	if read != int(leng) {
		return buf, sign == 1, fmt.Errorf("dbase-io-readmemo-5:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, sign == 1, nil
}

func validateFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("dbase-io-validatefileversion-1:FAILED:untested DBF file version: %d (%x hex)", version, version)
	case FOXPRO, FOXPRO_AUTOINCREMENT:
		return nil
	}
}

// GoTo sets the internal row pointer to row rowNumber
// Returns and EOF error if at EOF and positions the pointer at lastRow+1
func (dbf *DBF) GoTo(rowNumber uint32) error {
	if rowNumber > dbf.dbaseHeader.RowsCount {
		dbf.table.rowPointer = dbf.dbaseHeader.RowsCount
		return fmt.Errorf("dbase-io-goto-1:FAILED:go to %v > %v:%v", rowNumber, dbf.dbaseHeader.RowsCount, ERROR_EOF.AsError())
	}
	dbf.table.rowPointer = rowNumber
	return nil
}

// Skip adds offset to the internal row pointer
// Returns EOF error if at end of file and positions the pointer at lastRow+1
// Returns BOF error is the row pointer would be become negative and positions the pointer at 0
// Does not skip deleted rows
func (dbf *DBF) Skip(offset int64) error {
	newval := int64(dbf.table.rowPointer) + offset
	if newval >= int64(dbf.dbaseHeader.RowsCount) {
		dbf.table.rowPointer = dbf.dbaseHeader.RowsCount
		return fmt.Errorf("dbase-io-skip-1:FAILED:%v", ERROR_EOF.AsError())
	}
	if newval < 0 {
		dbf.table.rowPointer = 0
		return fmt.Errorf("dbase-io-skip-2:FAILED:%v", ERROR_BOF.AsError())
	}
	dbf.table.rowPointer = uint32(newval)
	return nil
}
