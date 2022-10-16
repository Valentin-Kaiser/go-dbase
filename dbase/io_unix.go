//go:build !windows
// +build !windows

package dbase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
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
	config         *Config     // The config used when working with the DBF file.
	dbaseFile      *os.File    // DBase file handle
	memoFile       *os.File    // Memo file handle
	header         *Header     // DBase file header containing relevant information
	memoHeader     *MemoHeader // Memo file header containing relevant information
	dbaseMutex     *sync.Mutex // Mutex locks for concurrent writing access to the DBF file
	memoMutex      *sync.Mutex // Mutex locks for concurrent writing access to the FPT file
	table          *Table      // Containing the columns and internal row pointer
	nullFlagColumn *Column     // The column containing the null flag column (if varchar or varbinary field exists)
}

/**
 *	################################################################
 *	#					IO Functions
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func Open(config *Config) (*DBF, error) {
	filename := filepath.Clean(config.Filename)
	mode := os.O_RDWR
	if config.Exclusive {
		mode |= os.O_EXCL
	}
	dbaseFile, err := os.OpenFile(filename, mode, 0600)
	if err != nil {
		return nil, newError("dbase-io-open-1", fmt.Errorf("opening DBF file failed with error: %w", err))
	}
	dbf, err := prepareDBF(dbaseFile, config)
	if err != nil {
		return nil, newError("dbase-io-open-2", err)
	}
	file.dbaseFile = dbaseFile
	// Check if the code page mark is matchin the converter
	if config.CodePageValidation && file.header.CodePage != file.config.Converter.CodePageMark() {
		return nil, newError("dbase-io-open-3", fmt.Errorf("code page mark mismatch: %d != %d", file.header.CodePage, file.config.Converter.CodePageMark()))
	}
	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (file.header.TableFlags & byte(MemoFlag)) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		memoFile, err := os.OpenFile(strings.TrimSuffix(filename, ext)+fptExt, mode, 0600)
		if err != nil {
			return nil, newError("dbase-io-open-4", fmt.Errorf("opening FPT file failed with error: %w", err))
		}
		err = file.prepareMemo(memoFile)
		if err != nil {
			return nil, newError("dbase-io-open-5", err)
		}
		file.memoFile = memoFile
	}
	return dbf, nil
}

// Closes the file handlers.
func (file *File) Close() error {
	if file.dbaseFile != nil {
		err := file.dbaseFile.Close()
		if err != nil {
			return newError("dbase-io-close-1", fmt.Errorf("closing DBF failed with error: %w", err))
		}
	}
	if file.memoFile != nil {
		err := file.memoFile.Close()
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

func create(file *File) (*DBF, error) {
	file.config.Filename = strings.ToUpper(strings.TrimSpace(file.config.Filename))
	// Check for valid file name
	if len(file.config.Filename) == 0 {
		return nil, newError("dbase-io-create-1", fmt.Errorf("missing filename"))
	}
	// Check for valid file extension
	if filepath.Ext(strings.ToUpper(file.config.Filename)) != ".DBF" {
		return nil, newError("dbase-io-create-2", fmt.Errorf("invalid file extension"))
	}
	// Check if file exists already
	if _, err := os.Stat(file.config.Filename); err == nil {
		return nil, newError("dbase-io-create-3", fmt.Errorf("file already exists"))
	}
	// Create the file
	dbaseFile, err := os.Create(strings.ToUpper(file.config.Filename))
	if err != nil {
		return nil, newError("dbase-io-create-2", fmt.Errorf("creating DBF file failed with error: %w", err))
	}
	file.dbaseFile = dbaseFile
	if file.memoHeader != nil {
		// Create the memo file
		memoFile, err := os.Create(strings.TrimSuffix(file.config.Filename, filepath.Ext(file.config.Filename)) + ".FPT")
		if err != nil {
			return nil, newError("dbase-io-create-4", fmt.Errorf("creating FPT file failed with error: %w", err))
		}
		file.memoFile = memoFile
	}
	return dbf, nil
}

// Returns a DBF object pointer
// Reads the DBF Header, the column infos and validates file version.
func prepareDBF(dbaseFile *os.File, config *Config) (*DBF, error) {
	header, err := readHeader(dbaseFile)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-1", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType, config.Untested); err != nil {
		return nil, newError("dbase-io-preparedbf-2", err)
	}
	columns, nullFlag, err := readColumns(dbaseFile)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-3", err)
	}
	dbf := &DBF{
		config:    config,
		header:    header,
		dbaseFile: dbaseFile,
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, len(columns)),
		},
		dbaseMutex:     &sync.Mutex{},
		memoMutex:      &sync.Mutex{},
		nullFlagColumn: nullFlag,
	}
	return dbf, nil
}

// Reads the DBF header from the file handle.
func readHeader(dbaseFile *os.File) (*Header, error) {
	h := &Header{}
	if _, err := dbaseFile.Seek(0, 0); err != nil {
		return nil, newError("dbase-io-readdbfheader-1", err)
	}
	b := make([]byte, 1024)
	n, err := dbaseFile.Read(b)
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
func (file *File) writeHeader() (err error) {
	// Lock the block we are writing to
	if file.config.WriteLock {
		flock := &unix.Flock_t{
			Type:   unix.F_WRLCK,
			Start:  0,
			Len:    int64(file.header.FirstRow),
			Whence: 0,
		}
		for {
			err = unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_SETLK, flock)
			if err == nil {
				break
			}
			if !errors.Is(err, unix.EAGAIN) {
				return newError("dbase-io-writeheader-1", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		defer func() {
			flock.Type = unix.F_ULOCK
			ulockErr := unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_ULOCK, flock)
			if ulockErr != nil {
				err = newError("dbase-io-writeheader-2", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = file.dbaseFile.Seek(0, 0)
	if err != nil {
		return newError("dbase-io-writeheader-3", err)
	}
	// Change the last modification date to the current date
	file.header.Year = uint8(time.Now().Year() - 2000)
	file.header.Month = uint8(time.Now().Month())
	file.header.Day = uint8(time.Now().Day())
	// Write the header
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, file.header)
	if err != nil {
		return newError("dbase-io-writeheader-4", err)
	}
	_, err = file.dbaseFile.Write(buf.Bytes())
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
	case byte(FoxPro), byte(FoxProAutoincrement), byte(FoxProVar):
		return nil
	}
}

// Reads column infos from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func readColumns(dbaseFile *os.File) ([]*Column, *Column, error) {
	var nullFlag *Column
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := dbaseFile.Seek(offset, 0); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-1", err)
		}
		if _, err := dbaseFile.Read(b); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-2", err)
		}
		if b[0] == byte(ColumnEnd) {
			break
		}
		// Position back one byte and read the column
		if _, err := dbaseFile.Seek(-1, 1); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-3", err)
		}
		buf := make([]byte, 33)
		n, err := dbaseFile.Read(buf)
		if err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-4", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-5", err)
		}
		if column.Name() == "_NullFlags" {
			nullFlag = column
			offset += 32
			continue
		}
		columns = append(columns, column)
		offset += 32
	}
	return columns, nullFlag, nil
}

func (file *File) writeColumns() (err error) {
	// Lock the block we are writing to
	position := uint32(32)
	// Lock the block we are writing to
	if file.config.WriteLock {
		flock := &unix.Flock_t{
			Type:   unix.F_WRLCK,
			Start:  int64(position),
			Len:    int64(file.header.FirstRow),
			Whence: 0,
		}
		for {
			err = unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_SETLK, flock)
			if err == nil {
				break
			}
			if !errors.Is(err, unix.EAGAIN) {
				return newError("dbase-io-writecolumns-1", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		defer func() {
			flock.Type = unix.F_ULOCK
			ulockErr := unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_ULOCK, flock)
			if ulockErr != nil {
				err = newError("dbase-io-writecolumns-2", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = file.dbaseFile.Seek(32, 0)
	if err != nil {
		return newError("dbase-io-writecolumns-3", err)
	}
	// Write the columns
	buf := new(bytes.Buffer)
	for _, column := range file.table.columns {
		err = binary.Write(buf, binary.LittleEndian, column)
		if err != nil {
			return newError("dbase-io-writecolumns-4", err)
		}
	}
	if file.nullFlagColumn != nil {
		err = binary.Write(buf, binary.LittleEndian, file.nullFlagColumn)
		if err != nil {
			return newError("dbase-io-writecolumns-5", err)
		}
	}
	_, err = file.dbaseFile.Write(buf.Bytes())
	if err != nil {
		return newError("dbase-io-writecolumns-5", err)
	}
	// Write the column terminator
	_, err = file.dbaseFile.Write([]byte{byte(ColumnEnd)})
	if err != nil {
		return newError("dbase-io-writecolumns-6", err)
	}
	// Write null till the end of the header
	pos := file.header.FirstRow - uint16(len(file.table.columns)*32) - 32
	if file.nullFlagColumn != nil {
		pos -= 32
	}
	_, err = file.dbaseFile.Write(make([]byte, pos))
	if err != nil {
		return newError("dbase-io-writecolumns-7", err)
	}
	return nil
}

// Read the nullFlag field at the end of the row
// The nullFlag field indicates if the field has a variable length
// If varlength is true, the field is variable length and the length is stored in the last byte
// If varlength is false, we read the complete field
// If the field is null, we return true as second return value
func (file *File) readNullFlag(rowPosition uint64, column *Column) (bool, bool, error) {
	if file.nullFlagColumn == nil {
		return false, false, fmt.Errorf("null flag column missing")
	}
	if column.DataType != byte(Varchar) && column.DataType != byte(Varbinary) {
		return false, false, fmt.Errorf("column not a varchar or varbinary")
	}
	// count what number of varchar field this field is
	bitCount := 0
	for _, c := range file.table.columns {
		if c.DataType == byte(Varchar) || c.DataType == byte(Varbinary) {
			if c == column {
				break
			}
			if c.Flag == byte(NullableFlag) || c.Flag == byte(NullableFlag|BinaryFlag) {
				bitCount += 2
			} else {
				bitCount++
			}
		}
	}
	// Read the null flag field
	position := uint64(file.header.FirstRow) + rowPosition*uint64(file.header.RowLength) + uint64(file.nullFlagColumn.Position)
	_, err := file.dbaseFile.Seek(int64(position), 0)
	if err != nil {
		return false, false, newError("dbase-io-readnullflag-1", err)
	}
	buf := make([]byte, file.nullFlagColumn.Length)
	n, err := file.dbaseFile.Read(buf)
	if err != nil {
		return false, false, newError("dbase-io-readnullflag-2", err)
	}
	if n != int(file.nullFlagColumn.Length) {
		return false, false, newError("dbase-io-readnullflag-3", fmt.Errorf("read %d bytes, expected %d", n, file.nullFlagColumn.Length))
	}

	if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
		return nthBit(buf, bitCount), nthBit(buf, bitCount+1), nil
	}

	return nthBit(buf, bitCount), false, nil
}

/**
 *	################################################################
 *	#				Memo file IO handler
 *	################################################################
 */

// prepareMemo prepares the memo file for reading.
func (file *File) prepareMemo(memoFile *os.File) error {
	memoHeader, err := readMemoHeader(memoFile)
	if err != nil {
		return newError("dbase-io-prepare-memo-1", err)
	}
	file.memoFile = memoFile
	file.memoHeader = memoHeader
	return nil
}

// readMemoHeader reads the memo header from the given file handle.
func readMemoHeader(memoFile *os.File) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := memoFile.Seek(0, 0); err != nil {
		return nil, newError("dbase-io-read-memo-header-1", err)
	}
	b := make([]byte, 1024)
	n, err := memoFile.Read(b)
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
func (file *File) readMemo(blockdata []byte) ([]byte, bool, error) {
	if file.memoFile == nil {
		return nil, false, newError("dbase-io-readmemo-1", ErrNoFPT)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	_, err := file.memoFile.Seek(int64(file.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, newError("dbase-io-readmemo-2", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = file.memoFile.Read(hbuf)
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
	read, err := file.memoFile.Read(buf)
	if err != nil {
		return buf, false, newError("dbase-io-readmemo-4", err)
	}
	if read != int(leng) {
		return buf, sign == 1, newError("dbase-io-readmemo-5", ErrIncomplete)
	}
	return buf, sign == 1, nil
}

// Parses a memo file from raw []byte, decodes and returns as []byte
func (file *File) parseMemoFile(raw []byte) ([]byte, bool, error) {
	memo, isText, err := file.readMemo(raw)
	if err != nil {
		return []byte{}, false, newError("dbase-io-parse-memo-1", err)
	}
	if isText {
		memo, err = file.config.Converter.Decode(memo)
		if err != nil {
			return []byte{}, false, newError("dbase-io-parse-memo-2", err)
		}
	}
	return memo, isText, nil
}

// writeMemo writes a memo to the memo file and returns the address of the memo.
func (file *File) writeMemo(raw []byte, text bool, length int) ([]byte, error) {
	file.memoMutex.Lock()
	defer file.memoMutex.Unlock()
	if file.memoFile == nil {
		return nil, newError("dbase-io-writememo-1", ErrNoFPT)
	}
	// Get the block position
	blockPosition := file.memoHeader.NextFree
	// Write the memo header
	err := file.writeMemoHeader()
	if err != nil {
		return nil, newError("dbase-io-writememo-2", err)
	}
	// Put the block data together
	block := make([]byte, file.memoHeader.BlockSize)
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
	flock := &unix.Flock_t{
		Type:   unix.F_WRLCK,
		Start:  int64(blockPosition),
		Len:    int64(file.memoHeader.BlockSize),
		Whence: 0,
	}
	if file.config.WriteLock {
		for {
			err = unix.FcntlFlock(file.memoFile.Fd(), unix.F_SETLK, flock)
			if err == nil {
				break
			}
			if !errors.Is(err, unix.EAGAIN) {
				return nil, newError("dbase-io-writememo-3", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		defer func() {
			flock.Type = unix.F_ULOCK
			ulockErr := unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_ULOCK, flock)
			if ulockErr != nil {
				err = newError("dbase-io-writememoheader-4", ulockErr)
			}
		}()
	}
	// Seek to new the next free block
	_, err = file.memoFile.Seek(int64(blockPosition)*int64(file.memoHeader.BlockSize), 0)
	if err != nil {
		return nil, newError("dbase-io-writememo-5", err)
	}
	// Write the memo data
	_, err = file.memoFile.Write(block)
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
func (file *File) writeMemoHeader() (err error) {
	if file.memoFile == nil {
		return newError("dbase-io-writememoheader-1", ErrNoFPT)
	}
	// Lock the block we are writing to
	if file.config.WriteLock {
		flock := &unix.Flock_t{
			Type:   unix.F_WRLCK,
			Start:  0,
			Len:    int64(file.header.FirstRow),
			Whence: 0,
		}
		for {
			err = unix.FcntlFlock(file.memoFile.Fd(), unix.F_SETLK, flock)
			if err == nil {
				break
			}

			if !errors.Is(err, unix.EAGAIN) {
				return newError("dbase-io-writememoheader-2", err)
			}

			time.Sleep(10 * time.Millisecond)
		}
		defer func() {
			flock.Type = unix.F_ULOCK
			ulockErr := unix.FcntlFlock(file.dbaseFile.Fd(), unix.F_ULOCK, flock)
			if ulockErr != nil {
				err = newError("dbase-io-writememoheader-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = file.memoFile.Seek(0, 0)
	if err != nil {
		return newError("dbase-io-writememoheader-4", err)
	}
	// Calculate the next free block
	file.memoHeader.NextFree++
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], file.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], file.memoHeader.BlockSize)
	_, err = file.memoFile.Write(buf)
	if err != nil {
		return newError("dbase-io-writememoheader-5", err)
	}
	// Write null till end of header
	_, err = file.memoFile.Write(make([]byte, 512-8))
	if err != nil {
		return newError("dbase-io-writememoheader-6", err)
	}
	return nil
}

/**
 *	################################################################
 *	#				Row and Field IO handler
 *	################################################################
 */

// Reads raw row data of one row at rowPosition
func (file *File) readRow(rowPosition uint32) ([]byte, error) {
	if rowPosition >= file.header.RowsCount {
		return nil, newError("dbase-io-readrow-1", ErrEOF)
	}
	buf := make([]byte, file.header.RowLength)
	_, err := file.dbaseFile.Seek(int64(file.header.FirstRow)+(int64(rowPosition)*int64(file.header.RowLength)), 0)
	if err != nil {
		return buf, newError("dbase-io-readrow-2", err)
	}
	read, err := file.dbaseFile.Read(buf)
	if err != nil {
		return buf, newError("dbase-io-readrow-3", err)
	}
	if read != int(file.header.RowLength) {
		return buf, newError("dbase-io-readrow-4", ErrIncomplete)
	}
	return buf, nil
}

// writeRow writes raw row data to the given row position
func (row *Row) writeRow() (err error) {
	row.file.dbaseMutex.Lock()
	defer row.file.dbaseMutex.Unlock()
	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return newError("dbase-io-writerow-1", err)
	}
	// Update the header
	position := int64(row.file.header.FirstRow) + (int64(row.Position) * int64(row.file.header.RowLength))
	if row.Position >= row.file.header.RowsCount {
		position = int64(row.file.header.FirstRow) + (int64(row.Position-1) * int64(row.file.header.RowLength))
		row.file.header.RowsCount++
	}
	err = row.file.writeHeader()
	if err != nil {
		return newError("dbase-io-writerow-2", err)
	}
	// Lock the block we are writing to
	if row.file.config.WriteLock {
		flock := &unix.Flock_t{
			Type:   unix.F_WRLCK,
			Start:  position,
			Len:    int64(row.file.header.RowLength),
			Whence: 0,
		}
		for {
			err = unix.FcntlFlock(row.file.dbaseFile.Fd(), unix.F_SETLK, flock)
			if err == nil {
				break
			}

			if !errors.Is(err, unix.EAGAIN) {
				return newError("dbase-io-writerow-3", err)
			}

			time.Sleep(10 * time.Millisecond)
		}
		defer func() {
			flock.Type = unix.F_ULOCK
			ulockErr := unix.FcntlFlock(row.file.dbaseFile.Fd(), unix.F_ULOCK, flock)
			if ulockErr != nil {
				err = newError("dbase-io-writerow-4", ulockErr)
			}
		}()
	}
	// Seek to the correct position
	_, err = row.file.dbaseFile.Seek(position, 0)
	if err != nil {
		return newError("dbase-io-writerow-5", err)
	}
	// Write the row
	_, err = row.file.dbaseFile.Write(r)
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
func (file *File) Search(field *Field, exactMatch bool) ([]*Row, error) {
	if field.column.DataType == 'M' {
		return nil, newError("dbase-io-search-1", fmt.Errorf("searching memo fields is not supported"))
	}
	// convert the value to a string
	val, err := file.valueToByteRepresentation(field, !exactMatch)
	if err != nil {
		return nil, newError("dbase-io-search-1", err)
	}
	// Search for the value
	rows := make([]*Row, 0)
	position := uint64(file.header.FirstRow)
	for i := uint32(0); i < file.header.RowsCount; i++ {
		// Read the field value
		_, err := file.dbaseFile.Seek(int64(position)+int64(field.column.Position), 0)
		position += uint64(file.header.RowLength)
		if err != nil {
			continue
		}
		buf := make([]byte, field.column.Length)
		read, err := file.dbaseFile.Read(buf)
		if err != nil {
			continue
		}
		if read != int(field.column.Length) {
			continue
		}
		// Check if the value matches
		if bytes.Contains(buf, val) {
			err := file.GoTo(i)
			if err != nil {
				continue
			}
			row, err := file.Row()
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
func (file *File) GoTo(rowNumber uint32) error {
	if rowNumber > file.header.RowsCount {
		file.table.rowPointer = file.header.RowsCount
		return newError("dbase-io-goto-1", fmt.Errorf("%w, go to %v > %v", ErrEOF, rowNumber, file.header.RowsCount))
	}
	file.table.rowPointer = rowNumber
	return nil
}

// Skip adds offset to the internal row pointer
// If at end of file positions the pointer at lastRow+1
// If the row pointer would be become negative positions the pointer at 0
// Does not skip deleted rows
func (file *File) Skip(offset int64) {
	newval := int64(file.table.rowPointer) + offset
	if newval >= int64(file.header.RowsCount) {
		file.table.rowPointer = file.header.RowsCount
	}
	if newval < 0 {
		file.table.rowPointer = 0
	}
	file.table.rowPointer = uint32(newval)
}

// Whether or not the write operations should lock the record
func (file *File) WriteLock(enabled bool) {
	file.config.WriteLock = enabled
}

// Returns if the row at internal row pointer is deleted
func (file *File) Deleted() (bool, error) {
	if file.table.rowPointer >= file.header.RowsCount {
		return false, newError("dbase-io-deleted-1", ErrEOF)
	}
	_, err := file.dbaseFile.Seek(int64(file.header.FirstRow)+(int64(file.table.rowPointer)*int64(file.header.RowLength)), 0)
	if err != nil {
		return false, newError("dbase-io-deleted-2", err)
	}
	buf := make([]byte, 1)
	read, err := file.dbaseFile.Read(buf)
	if err != nil {
		return false, newError("dbase-io-deleted-3", err)
	}
	if read != 1 {
		return false, newError("dbase-io-deleted-4", ErrIncomplete)
	}
	return buf[0] == byte(Deleted), nil
}
