//go:build windows
// +build windows

package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/windows"
)

// Configures the file you want to open.
// The filename is mandatory. The other fields are optional and are false by default.
// If Converter and InterpretCodePage are both not set the package will try to interpret the code page mark.
// To open untested files set Untested to true. Tested files are defined in the constants.go file.
type Config struct {
	Filename          string            // The filename of the DBF file.
	Converter         EncodingConverter // The encoding converter to use.
	Exclusive         bool              // If true the file is opened in exclusive mode.
	Untested          bool              // If true the file version is not checked.
	TrimSpaces        bool              // Trimspaces default value.
	WriteLock         bool              // Whether or not the write operations should lock the record.
	ValidateCodePage  bool              // Whether or not the code page mark should be validated.
	InterpretCodePage bool              // Whether or not the code page mark should be interpreted. Ignores the defined converter.
}

// File is the main struct to handle a dBase file.
// Each file type is basically a Table or a Memo file.
type File struct {
	config         *Config         // The config used when working with the DBF file.
	handle         *windows.Handle // DBase file windows handle pointer.
	relatedHandle  *windows.Handle // Memo file windows handle pointer.
	header         *Header         // DBase file header containing relevant information.
	memoHeader     *MemoHeader     // Memo file header containing relevant information.
	dbaseMutex     *sync.Mutex     // Mutex locks for concurrent writing access to the DBF file.
	memoMutex      *sync.Mutex     // Mutex locks for concurrent writing access to the FPT file.
	table          *Table          // Containing the columns and internal row pointer.
	nullFlagColumn *Column         // The column containing the null flag column (if varchar or varbinary field exists).
}

/**
 *	################################################################
 *	#					IO Functions
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the file handle(s) call DBF.Close().
func OpenTable(config *Config) (*File, error) {
	if config == nil {
		return nil, newError("dbase-io-opentable-1", fmt.Errorf("missing configuration"))
	}
	debugf("Opening table: %s - Exclusive: %v - Untested: %v - Trim spaces: %v - Write lock: %v - ValidateCodepage: %v - InterpretCodepage: %v", config.Filename, config.Exclusive, config.Untested, config.TrimSpaces, config.WriteLock, config.ValidateCodePage, config.InterpretCodePage)
	if len(strings.TrimSpace(config.Filename)) == 0 {
		return nil, newError("dbase-io-opentable-2", fmt.Errorf("missing filename"))
	}
	fileName := filepath.Clean(config.Filename)
	fileExtension := FileExtension(strings.ToUpper(filepath.Ext(config.Filename)))
	fileName, err := findFile(fileName)
	if err != nil {
		return nil, newError("dbase-io-opentable-3", err)
	}
	mode := windows.O_RDWR | windows.O_CLOEXEC | windows.O_NONBLOCK
	if config.Exclusive {
		mode = windows.O_RDWR | windows.O_CLOEXEC | windows.O_EXCL
	}
	fd, err := windows.Open(fileName, mode, 0644)
	if err != nil {
		return nil, newError("dbase-io-opentable-3", fmt.Errorf("opening DBF file %v failed with error: %w", fileName, err))
	}
	file, err := prepareFile(fd, config)
	if err != nil {
		return nil, newError("dbase-io-opentable-4", err)
	}
	file.handle = &fd
	// Interpret the code page mark if needed
	if config.InterpretCodePage || config.Converter == nil {
		if config.Converter == nil {
			debugf("No encoding converter defined, falling back to default (interpreting)")
		}
		debugf("Interpreting code page mark...")
		file.config.Converter = ConverterFromCodePage(file.header.CodePage)
		debugf("Code page: 0x%02x => interpreted: 0x%02x", file.header.CodePage, file.config.Converter.CodePage())
	}
	// Check if the code page mark is matchin the converter
	if config.ValidateCodePage && file.header.CodePage != file.config.Converter.CodePage() {
		return nil, newError("dbase-io-opentable-6", fmt.Errorf("code page mark mismatch: %d != %d", file.header.CodePage, file.config.Converter.CodePage()))
	}
	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if MemoFlag.Defined(file.header.TableFlags) {
		ext := FPT
		if fileExtension == DBC {
			ext = DCT
		}
		relatedFile := strings.TrimSuffix(fileName, path.Ext(fileName)) + string(ext)
		debugf("Opening related file: %s\n", relatedFile)
		fd, err := windows.Open(relatedFile, mode, 0644)
		if err != nil {
			return nil, newError("dbase-io-opentable-7", fmt.Errorf("opening related file %v failed with error: %w", relatedFile, err))
		}
		err = file.readMemoHeader(fd)
		if err != nil {
			return nil, newError("dbase-io-opentable-8", err)
		}
		file.relatedHandle = &fd
	}
	return file, nil
}

// Closes the file handlers.
func (file *File) Close() error {
	if file.handle != nil {
		debugf("Closing file: %s", file.config.Filename)
		err := windows.Close(*file.handle)
		if err != nil {
			return newError("dbase-io-close-1", fmt.Errorf("closing DBF failed with error: %w", err))
		}
	}
	if file.relatedHandle != nil {
		debugf("Closing related file: %s", file.config.Filename)
		err := windows.Close(*file.relatedHandle)
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

func create(file *File) (*File, error) {
	file.config.Filename = strings.ToUpper(strings.TrimSpace(file.config.Filename))
	// Check for valid file name
	if len(file.config.Filename) == 0 {
		return nil, newError("dbase-io-create-1", fmt.Errorf("missing filename"))
	}
	dbfname, err := windows.UTF16FromString(file.config.Filename)
	if err != nil {
		return nil, newError("dbase-io-create-2", fmt.Errorf("converting filename to UTF16 failed with error: %w", err))
	}
	// Check if file exists already
	_, err = windows.GetFileAttributes(&dbfname[0])
	if err == nil {
		return nil, newError("dbase-io-create-3", fmt.Errorf("file already exists"))
	}
	// Create the file
	debugf("Creating file: %s", file.config.Filename)
	fd, err := windows.CreateFile(&dbfname[0], windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.CREATE_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, newError("dbase-io-create-4", fmt.Errorf("creating DBF file failed with error: %w", err))
	}
	file.handle = &fd
	if file.memoHeader != nil {
		debugf("Creating related file: %s", file.config.Filename)
		// Create the memo file
		fptname, err := windows.UTF16FromString(strings.TrimSuffix(file.config.Filename, filepath.Ext(file.config.Filename)) + ".FPT")
		if err != nil {
			return nil, newError("dbase-io-create-5", fmt.Errorf("converting filename to UTF16 failed with error: %w", err))
		}
		fd, err := windows.CreateFile(&fptname[0], windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.CREATE_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
		if err != nil {
			return nil, newError("dbase-io-create-6", fmt.Errorf("creating FPT file failed with error: %w", err))
		}
		file.relatedHandle = &fd
	}
	return file, nil
}

// Returns a File object pointer
// Reads the File Header, the column infos and validates file version.
func prepareFile(fd windows.Handle, config *Config) (*File, error) {
	header, err := readHeader(fd)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-1", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileType, config.Untested); err != nil {
		return nil, newError("dbase-io-preparedbf-2", err)
	}
	columns, nullFlag, err := readColumns(fd)
	if err != nil {
		return nil, newError("dbase-io-preparedbf-3", err)
	}
	file := &File{
		config: config,
		header: header,
		handle: &fd,
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, len(columns)),
		},
		dbaseMutex:     &sync.Mutex{},
		memoMutex:      &sync.Mutex{},
		nullFlagColumn: nullFlag,
	}
	return file, nil
}

// Reads the DBF header from the file handle.
func readHeader(fd windows.Handle) (*Header, error) {
	debugf("Reading header...")
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
func (file *File) writeHeader() (err error) {
	debugf("Writing header - exclusive writing: %v", file.config.WriteLock)
	// Lock the block we are writing to
	position := uint32(0)
	o := &windows.Overlapped{
		Offset:     position,
		OffsetHigh: position + uint32(file.header.FirstRow),
	}
	// Lock the block we are writing to
	if file.config.WriteLock {
		err = windows.LockFileEx(*file.relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, position, position+uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-writeheader-1", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*file.relatedHandle, 0, position, position+uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-writeheader-2", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*file.handle, 0, 0)
	if err != nil {
		return newError("dbase-io-writeheader-3", err)
	}
	// Change the last modification date to the current date
	file.header.Year = uint8(time.Now().Year() - 2000)
	file.header.Month = uint8(time.Now().Month())
	file.header.Day = uint8(time.Now().Day())
	debugf("Writing header: %+v", file.header)
	// Write the header
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, file.header)
	if err != nil {
		return newError("dbase-io-writeheader-4", err)
	}
	_, err = windows.Write(*file.handle, buf.Bytes())
	if err != nil {
		return newError("dbase-io-writeheader-5", err)
	}
	return nil
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

// Reads columns from DBF header, starting at pos 32, until it finds the Header row terminator END_OF_COLUMN(0x0D).
func readColumns(fd windows.Handle) ([]*Column, *Column, error) {
	debugf("Reading columns...")
	var nullFlag *Column
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := windows.Seek(fd, offset, 0); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-1", err)
		}
		if _, err := windows.Read(fd, b); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-2", err)
		}
		if Marker(b[0]) == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := windows.Seek(fd, -1, 1); err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-3", err)
		}
		buf := make([]byte, 33)
		n, err := windows.Read(fd, buf)
		if err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-4", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, nil, newError("dbase-io-readcolumninfos-5", err)
		}
		if column.Name() == "_NullFlags" {
			debugf("Found null flag column: %s", column.Name())
			nullFlag = column
			offset += 32
			continue
		}
		debugf("Found column %v of type %v at offset: %d", column.Name(), column.Type(), offset)
		columns = append(columns, column)
		offset += 32
	}
	return columns, nullFlag, nil
}

func (file *File) writeColumns() (err error) {
	debugf("Writing columns - exclusive writing: %v", file.config.WriteLock)
	// Lock the block we are writing to
	position := uint32(32)
	o := &windows.Overlapped{
		Offset:     position,
		OffsetHigh: position + uint32(file.header.FirstRow),
	}
	// Lock the block we are writing to
	if file.config.WriteLock {
		err = windows.LockFileEx(*file.relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, position, position+uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-writecolumns-1", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*file.relatedHandle, 0, position, position+uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-writecolumns-2", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*file.handle, 32, 0)
	if err != nil {
		return newError("dbase-io-writecolumns-3", err)
	}
	// Write the columns
	buf := new(bytes.Buffer)
	for _, column := range file.table.columns {
		debugf("Writing column: %+v", column)
		err = binary.Write(buf, binary.LittleEndian, column)
		if err != nil {
			return newError("dbase-io-writecolumns-4", err)
		}
	}
	if file.nullFlagColumn != nil {
		debugf("Writing null flag column: %s", file.nullFlagColumn.Name())
		err = binary.Write(buf, binary.LittleEndian, file.nullFlagColumn)
		if err != nil {
			return newError("dbase-io-writecolumns-5", err)
		}
	}
	_, err = windows.Write(*file.handle, buf.Bytes())
	if err != nil {
		return newError("dbase-io-writecolumns-6", err)
	}
	// Write the column terminator
	_, err = windows.Write(*file.handle, []byte{byte(ColumnEnd)})
	if err != nil {
		return newError("dbase-io-writecolumns-7", err)
	}
	// Write null till the end of the header
	pos := file.header.FirstRow - uint16(len(file.table.columns)*32) - 32
	if file.nullFlagColumn != nil {
		pos -= 32
	}
	_, err = windows.Write(*file.handle, make([]byte, pos))
	if err != nil {
		return newError("dbase-io-writecolumns-8", err)
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
	_, err := windows.Seek(*file.handle, int64(position), 0)
	if err != nil {
		return false, false, newError("dbase-io-readnullflag-1", err)
	}
	buf := make([]byte, file.nullFlagColumn.Length)
	n, err := windows.Read(*file.handle, buf)
	if err != nil {
		return false, false, newError("dbase-io-readnullflag-2", err)
	}
	if n != int(file.nullFlagColumn.Length) {
		return false, false, newError("dbase-io-readnullflag-3", fmt.Errorf("read %d bytes, expected %d", n, file.nullFlagColumn.Length))
	}

	if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
		debugf("Read _NullFlag for column %s => varlength: %v - null: %v", column.Name(), nthBit(buf, bitCount), nthBit(buf, bitCount+1))
		return nthBit(buf, bitCount), nthBit(buf, bitCount+1), nil
	}

	debugf("Read _NullFlag for column %s => varlength: %v ", column.Name(), nthBit(buf, bitCount))
	return nthBit(buf, bitCount), false, nil
}

/**
 *	################################################################
 *	#				Memo file IO handler
 *	################################################################
 */

// readMemoHeader reads the memo header from the given file handle.
func (file *File) readMemoHeader(fd windows.Handle) error {
	debugf("Reading memo header...")
	h := &MemoHeader{}
	if _, err := windows.Seek(fd, 0, 0); err != nil {
		return newError("dbase-io-read-memo-header-1", err)
	}
	b := make([]byte, 1024)
	n, err := windows.Read(fd, b)
	if err != nil {
		return newError("dbase-io-read-memo-header-2", err)
	}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return newError("dbase-io-read-memo-header-3", err)
	}
	debugf("Memo header: %+v", h)
	file.relatedHandle = &fd
	file.memoHeader = h
	return nil
}

// Reads one or more blocks from the FPT file, called for each memo column.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (file *File) readMemo(address []byte) ([]byte, bool, error) {
	if file.relatedHandle == nil {
		return nil, false, newError("dbase-io-readmemo-1", ErrNoFPT)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(address)
	if block == 0 {
		return []byte{}, false, nil
	}
	position := int64(file.memoHeader.BlockSize) * int64(block)
	debugf("Reading memo block %d at position %d", block, position)
	// The position in the file is blocknumber*blocksize
	_, err := windows.Seek(*file.relatedHandle, position, 0)
	if err != nil {
		return nil, false, newError("dbase-io-readmemo-2", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = windows.Read(*file.relatedHandle, hbuf)
	if err != nil {
		return nil, false, newError("dbase-io-readmemo-3", err)
	}
	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])
	debugf("Memo block header => text: %v, length: %d", sign == 1, leng)
	if leng == 0 {
		// No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}
	// Now read the actual data
	buf := make([]byte, leng)
	read, err := windows.Read(*file.relatedHandle, buf)
	if err != nil {
		return buf, false, newError("dbase-io-readmemo-4", err)
	}
	if read != int(leng) {
		return buf, sign == 1, newError("dbase-io-readmemo-5", ErrIncomplete)
	}
	if sign == 1 {
		buf, err = file.config.Converter.Decode(buf)
		if err != nil {
			return []byte{}, false, newError("dbase-io-readmemo-6", err)
		}
	}
	return buf, sign == 1, nil
}

// writeMemo writes a memo to the memo file and returns the address of the memo.
func (file *File) writeMemo(raw []byte, text bool, length int) ([]byte, error) {
	file.memoMutex.Lock()
	defer file.memoMutex.Unlock()
	if file.relatedHandle == nil {
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
	if file.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     blockPosition,
			OffsetHigh: blockPosition + uint32(file.memoHeader.BlockSize),
		}
		err = windows.LockFileEx(*file.relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, blockPosition, blockPosition+uint32(file.memoHeader.BlockSize), o)
		if err != nil {
			return nil, newError("dbase-io-writememo-3", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*file.relatedHandle, 0, blockPosition, blockPosition+uint32(file.memoHeader.BlockSize), o)
			if err != nil {
				err = newError("dbase-io-writememoheader-4", ulockErr)
			}
		}()
	}
	position := int64(blockPosition) * int64(file.memoHeader.BlockSize)
	debugf("Writing memo block %d at position %d", blockPosition, position)
	// Seek to new the next free block
	_, err = windows.Seek(*file.relatedHandle, position, 0)
	if err != nil {
		return nil, newError("dbase-io-writememo-5", err)
	}
	// Write the memo data
	_, err = windows.Write(*file.relatedHandle, block)
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
	if file.relatedHandle == nil {
		return newError("dbase-io-writememoheader-1", ErrNoFPT)
	}
	debugf("Writing memo header...")
	// Lock the block we are writing to
	o := &windows.Overlapped{
		Offset:     0,
		OffsetHigh: uint32(file.header.FirstRow),
	}
	if file.config.WriteLock {
		err = windows.LockFileEx(*file.relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 0, uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-writememoheader-2", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*file.relatedHandle, 0, 0, uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-writememoheader-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*file.relatedHandle, 0, 0)
	if err != nil {
		return newError("dbase-io-writememoheader-4", err)
	}
	// Calculate the next free block
	file.memoHeader.NextFree++
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], file.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], file.memoHeader.BlockSize)
	debugf("Writing memo header - next free: %d, block size: %d", file.memoHeader.NextFree, file.memoHeader.BlockSize)
	_, err = windows.Write(*file.relatedHandle, buf)
	if err != nil {
		return newError("dbase-io-writememoheader-5", err)
	}
	// Write null till end of header
	_, err = windows.Write(*file.relatedHandle, make([]byte, 512-8))
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
	position := int64(file.header.FirstRow) + (int64(rowPosition) * int64(file.header.RowLength))
	debugf("Reading row: %d at offset: %v", rowPosition, position)
	buf := make([]byte, file.header.RowLength)
	_, err := windows.Seek(*file.handle, position, 0)
	if err != nil {
		return buf, newError("dbase-io-readrow-2", err)
	}
	read, err := windows.Read(*file.handle, buf)
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
	debugf("Writing row: %d ...", row.Position)
	row.handle.dbaseMutex.Lock()
	defer row.handle.dbaseMutex.Unlock()
	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return newError("dbase-io-writerow-1", err)
	}
	// Update the header
	position := int64(row.handle.header.FirstRow) + (int64(row.Position) * int64(row.handle.header.RowLength))
	if row.Position >= row.handle.header.RowsCount {
		position = int64(row.handle.header.FirstRow) + (int64(row.Position-1) * int64(row.handle.header.RowLength))
		row.handle.header.RowsCount++
	}
	err = row.handle.writeHeader()
	if err != nil {
		return newError("dbase-io-writerow-2", err)
	}
	// Lock the block we are writing to
	if row.handle.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     uint32(position),
			OffsetHigh: uint32(position + int64(row.handle.header.RowLength)),
		}
		err = windows.LockFileEx(*row.handle.handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, uint32(position), uint32(position+int64(row.handle.header.RowLength)), o)
		if err != nil {
			return newError("dbase-io-writerow-3", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*row.handle.handle, 0, uint32(position), uint32(position+int64(row.handle.header.RowLength)), o)
			if err != nil {
				err = newError("dbase-io-writerow-4", ulockErr)
			}
		}()
	}
	debugf("Writing row: %d at offset: %v", row.Position, position)
	// Seek to the correct position
	_, err = windows.Seek(*row.handle.handle, position, 0)
	if err != nil {
		return newError("dbase-io-writerow-5", err)
	}
	// Write the row
	_, err = windows.Write(*row.handle.handle, r)
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
	debugf("Searching for value: %v in field: %s", field.GetValue(), field.column.Name())
	// convert the value to bytes
	val, err := file.valueToByteRepresentation(field, !exactMatch)
	if err != nil {
		return nil, newError("dbase-io-search-2", err)
	}
	// Search for the value
	rows := make([]*Row, 0)
	position := uint64(file.header.FirstRow)
	for i := uint32(0); i < file.header.RowsCount; i++ {
		// Read the field value
		p := int64(position) + int64(field.column.Position)
		debugf("Searching at position: %d", p)
		_, err := windows.Seek(*file.handle, p, 0)
		position += uint64(file.header.RowLength)
		if err != nil {
			continue
		}
		buf := make([]byte, field.column.Length)
		read, err := windows.Read(*file.handle, buf)
		if err != nil {
			continue
		}
		if read != int(field.column.Length) {
			continue
		}
		// Check if the value matches
		if bytes.Contains(buf, val) {
			debugf("Found matching row %v at position: %d", i, p-int64(field.column.Position))
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
	debugf("Going to row: %d", rowNumber)
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
	debugf("Skipping %d row/s, new position: %d", offset, file.table.rowPointer)
}

// Whether or not the write operations should lock the record
func (file *File) WriteLock(enabled bool) {
	debugf("Setting write lock to: %v", enabled)
	file.config.WriteLock = enabled
}

// Returns if the row at internal row pointer is deleted
func (file *File) Deleted() (bool, error) {
	if file.table.rowPointer >= file.header.RowsCount {
		return false, newError("dbase-io-deleted-1", ErrEOF)
	}
	_, err := windows.Seek(*file.handle, int64(file.header.FirstRow)+(int64(file.table.rowPointer)*int64(file.header.RowLength)), 0)
	if err != nil {
		return false, newError("dbase-io-deleted-2", err)
	}
	buf := make([]byte, 1)
	read, err := windows.Read(*file.handle, buf)
	if err != nil {
		return false, newError("dbase-io-deleted-3", err)
	}
	if read != 1 {
		return false, newError("dbase-io-deleted-4", ErrIncomplete)
	}
	return Marker(buf[0]) == Deleted, nil
}

func findFile(name string) (string, error) {
	debugf("Searching for file: %s", name)
	// Read all files in the directory
	files, err := os.ReadDir(filepath.Dir(name))
	if err != nil {
		return "", newError("dbase-io-findfile-1", err)
	}
	for _, file := range files {
		if strings.EqualFold(file.Name(), filepath.Base(name)) {
			debugf("Found file: %s", file.Name())
			return filepath.Join(filepath.Dir(name), file.Name()), nil
		}
	}
	return name, nil
}
