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
	"reflect"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/windows"
)

var DefaultIO WindowsIO

// WindowsIO implements the IO interface for Windows systems.
type WindowsIO struct{}

func (w WindowsIO) OpenTable(config *Config) (*File, error) {
	if config == nil {
		return nil, newError("dbase-io-windows-opentable-1", fmt.Errorf("missing configuration"))
	}
	debugf("Opening table: %s - Exclusive: %v - Untested: %v - Trim spaces: %v - Write lock: %v - ValidateCodepage: %v - InterpretCodepage: %v", config.Filename, config.Exclusive, config.Untested, config.TrimSpaces, config.WriteLock, config.ValidateCodePage, config.InterpretCodePage)
	if len(strings.TrimSpace(config.Filename)) == 0 {
		return nil, newError("dbase-io-windows-opentable-2", fmt.Errorf("missing filename"))
	}
	fileName := filepath.Clean(config.Filename)
	fileExtension := FileExtension(strings.ToUpper(filepath.Ext(config.Filename)))
	fileName, err := _findFile(fileName)
	if err != nil {
		return nil, newError("dbase-io-windows-opentable-3", err)
	}
	mode := windows.O_RDWR | windows.O_CLOEXEC | windows.O_NONBLOCK
	if config.Exclusive {
		mode = windows.O_RDWR | windows.O_CLOEXEC | windows.O_EXCL
	}
	fd, err := windows.Open(fileName, mode, 0644)
	if err != nil {
		return nil, newError("dbase-io-windows-opentable-3", fmt.Errorf("opening DBF file %v failed with error: %w", fileName, err))
	}
	file := &File{
		config:     config,
		io:         w,
		handle:     &fd,
		dbaseMutex: &sync.Mutex{},
		memoMutex:  &sync.Mutex{},
	}
	err = file.ReadHeader()
	if err != nil {
		return nil, newError("dbase-io-windows-preparedbf-1", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := ValidateFileVersion(file.header.FileType, config.Untested); err != nil {
		return nil, newError("dbase-io-windows-preparedbf-2", err)
	}
	columns, nullFlag, err := file.ReadColumns()
	if err != nil {
		return nil, newError("dbase-io-windows-preparedbf-3", err)
	}
	file.nullFlagColumn = nullFlag
	file.table = &Table{
		columns: columns,
		mods:    make([]*Modification, len(columns)),
	}
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
		return nil, newError("dbase-io-windows-opentable-6", fmt.Errorf("code page mark mismatch: %d != %d", file.header.CodePage, file.config.Converter.CodePage()))
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
		relatedFD, err := windows.Open(relatedFile, mode, 0644)
		if err != nil {
			return nil, newError("dbase-io-windows-opentable-7", fmt.Errorf("opening related file %v failed with error: %w", relatedFile, err))
		}
		file.relatedHandle = &relatedFD
		err = file.ReadMemoHeader()
		if err != nil {
			return nil, newError("dbase-io-windows-opentable-8", err)
		}
	}
	return file, nil
}

func (w WindowsIO) Close(file *File) error {
	if file.handle != nil {
		handle, err := w.getHandle(file)
		if err != nil {
			return newError("dbase-io-windows-close-1", err)
		}

		debugf("Closing file: %s", file.config.Filename)
		err = windows.Close(*handle)
		if err != nil {
			return newError("dbase-io-windows-close-2", fmt.Errorf("closing DBF failed with error: %w", err))
		}
	}
	if file.relatedHandle != nil {
		relatedHandle, err := w.getRelatedHandle(file)
		if err != nil {
			return newError("dbase-io-windows-close-3", err)
		}

		debugf("Closing related file: %s", file.config.Filename)
		err = windows.Close(*relatedHandle)
		if err != nil {
			return newError("dbase-io-windows-close-4", fmt.Errorf("closing FPT failed with error: %w", err))
		}
	}
	return nil
}

func (w WindowsIO) Create(file *File) error {
	file.config.Filename = strings.ToUpper(strings.TrimSpace(file.config.Filename))
	// Check for valid file name
	if len(file.config.Filename) == 0 {
		return newError("dbase-io-windows-create-1", fmt.Errorf("missing filename"))
	}
	dbfname, err := windows.UTF16FromString(file.config.Filename)
	if err != nil {
		return newError("dbase-io-windows-create-2", fmt.Errorf("converting filename to UTF16 failed with error: %w", err))
	}
	// Check if file exists already
	_, err = windows.GetFileAttributes(&dbfname[0])
	if err == nil {
		return newError("dbase-io-windows-create-3", fmt.Errorf("file already exists"))
	}
	// Create the file
	debugf("Creating file: %s", file.config.Filename)
	fd, err := windows.CreateFile(&dbfname[0], windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.CREATE_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return newError("dbase-io-windows-create-4", fmt.Errorf("creating DBF file failed with error: %w", err))
	}
	file.handle = &fd
	if file.memoHeader != nil {
		debugf("Creating related file: %s", file.config.Filename)
		// Create the memo file
		fptname, err := windows.UTF16FromString(strings.TrimSuffix(file.config.Filename, filepath.Ext(file.config.Filename)) + ".FPT")
		if err != nil {
			return newError("dbase-io-windows-create-5", fmt.Errorf("converting filename to UTF16 failed with error: %w", err))
		}
		fd, err := windows.CreateFile(&fptname[0], windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.CREATE_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
		if err != nil {
			return newError("dbase-io-windows-create-6", fmt.Errorf("creating FPT file failed with error: %w", err))
		}
		file.relatedHandle = &fd
	}
	return nil
}

func (w WindowsIO) ReadHeader(file *File) error {
	debugf("Reading header...")
	handle, err := w.getHandle(file)
	if err != nil {
		return newError("dbase-io-windows-readheader-1", err)
	}
	h := &Header{}
	if _, err := windows.Seek(*handle, 0, 0); err != nil {
		return newError("dbase-io-windows-readheader-2", err)
	}
	b := make([]byte, 30)
	n, err := windows.Read(*handle, b)
	if err != nil {
		return newError("dbase-io-windows-readheader-3", err)
	}
	// LittleEndian - Integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return newError("dbase-io-windows-readheader-4", err)
	}
	file.header = h
	return nil
}

func (w WindowsIO) WriteHeader(file *File) (err error) {
	debugf("Writing header - exclusive writing: %v", file.config.WriteLock)
	handle, err := w.getHandle(file)
	if err != nil {
		return newError("dbase-io-windows-writeheader-1", err)
	}
	// Lock the block we are writing to
	position := uint32(0)
	o := &windows.Overlapped{
		Offset:     position,
		OffsetHigh: position + uint32(file.header.FirstRow),
	}
	// Lock the block we are writing to
	if file.config.WriteLock {
		err = windows.LockFileEx(*handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, position, position+uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-windows-writeheader-2", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*handle, 0, position, position+uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-windows-writeheader-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*handle, 0, 0)
	if err != nil {
		return newError("dbase-io-windows-writeheader-4", err)
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
		return newError("dbase-io-windows-writeheader-5", err)
	}
	_, err = windows.Write(*handle, buf.Bytes())
	if err != nil {
		return newError("dbase-io-windows-writeheader-6", err)
	}
	return nil
}

func (w WindowsIO) ReadColumns(file *File) ([]*Column, *Column, error) {
	debugf("Reading columns...")
	handle, err := w.getHandle(file)
	if err != nil {
		return nil, nil, newError("dbase-io-windows-readcolumns-1", err)
	}
	var nullFlag *Column
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := windows.Seek(*handle, offset, 0); err != nil {
			return nil, nil, newError("dbase-io-windows-readcolumns-1", err)
		}
		if _, err := windows.Read(*handle, b); err != nil {
			return nil, nil, newError("dbase-io-windows-readcolumns-2", err)
		}
		if Marker(b[0]) == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := windows.Seek(*handle, -1, 1); err != nil {
			return nil, nil, newError("dbase-io-windows-readcolumns-3", err)
		}
		buf := make([]byte, 33)
		n, err := windows.Read(*handle, buf)
		if err != nil {
			return nil, nil, newError("dbase-io-windows-readcolumns-4", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, nil, newError("dbase-io-windows-readcolumns-5", err)
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

func (w WindowsIO) WriteColumns(file *File) (err error) {
	debugf("Writing columns - exclusive writing: %v", file.config.WriteLock)
	handle, err := w.getHandle(file)
	if err != nil {
		return newError("dbase-io-windows-writecolumns-1", err)
	}
	// Lock the block we are writing to
	position := uint32(32)
	o := &windows.Overlapped{
		Offset:     position,
		OffsetHigh: position + uint32(file.header.FirstRow),
	}
	// Lock the block we are writing to
	if file.config.WriteLock {
		err = windows.LockFileEx(*handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, position, position+uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-windows-writecolumns-2", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*handle, 0, position, position+uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-windows-writecolumns-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*handle, 32, 0)
	if err != nil {
		return newError("dbase-io-windows-writecolumns-4", err)
	}
	// Write the columns
	buf := new(bytes.Buffer)
	for _, column := range file.table.columns {
		debugf("Writing column: %+v", column)
		err = binary.Write(buf, binary.LittleEndian, column)
		if err != nil {
			return newError("dbase-io-windows-writecolumns-5", err)
		}
	}
	if file.nullFlagColumn != nil {
		debugf("Writing null flag column: %s", file.nullFlagColumn.Name())
		err = binary.Write(buf, binary.LittleEndian, file.nullFlagColumn)
		if err != nil {
			return newError("dbase-io-windows-writecolumns-6", err)
		}
	}
	_, err = windows.Write(*handle, buf.Bytes())
	if err != nil {
		return newError("dbase-io-windows-writecolumns-7", err)
	}
	// Write the column terminator
	_, err = windows.Write(*handle, []byte{byte(ColumnEnd)})
	if err != nil {
		return newError("dbase-io-windows-writecolumns-8", err)
	}
	// Write null till the end of the header
	pos := file.header.FirstRow - uint16(len(file.table.columns)*32) - 33
	if file.nullFlagColumn != nil {
		pos -= 32
	}
	_, err = windows.Write(*handle, make([]byte, pos))
	if err != nil {
		return newError("dbase-io-windows-writecolumns-9", err)
	}
	return nil
}

func (w WindowsIO) ReadNullFlag(file *File, position uint64, column *Column) (bool, bool, error) {
	handle, err := w.getHandle(file)
	if err != nil {
		return false, false, newError("dbase-io-windows-readnullflag-1", err)
	}
	if file.nullFlagColumn == nil {
		return false, false, newError("dbase-io-windows-readnullflag-2", fmt.Errorf("null flag column is nil"))
	}
	if column.DataType != byte(Varchar) && column.DataType != byte(Varbinary) {
		return false, false, newError("dbase-io-windows-readnullflag-3", fmt.Errorf("column is not of type varchar or varbinary"))
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
	pos := uint64(file.header.FirstRow) + position*uint64(file.header.RowLength) + uint64(file.nullFlagColumn.Position)
	_, err = windows.Seek(*handle, int64(pos), 0)
	if err != nil {
		return false, false, newError("dbase-io-windows-readnullflag-1", err)
	}
	buf := make([]byte, file.nullFlagColumn.Length)
	n, err := windows.Read(*handle, buf)
	if err != nil {
		return false, false, newError("dbase-io-windows-readnullflag-2", err)
	}
	if n != int(file.nullFlagColumn.Length) {
		return false, false, newError("dbase-io-windows-readnullflag-3", fmt.Errorf("read %d bytes, expected %d", n, file.nullFlagColumn.Length))
	}
	if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
		debugf("Read _NullFlag for column %s => varlength: %v - null: %v", column.Name(), nthBit(buf, bitCount), nthBit(buf, bitCount+1))
		return nthBit(buf, bitCount), nthBit(buf, bitCount+1), nil
	}
	debugf("Read _NullFlag for column %s => varlength: %v ", column.Name(), nthBit(buf, bitCount))
	return nthBit(buf, bitCount), false, nil
}

func (w WindowsIO) ReadMemoHeader(file *File) error {
	debugf("Reading memo header...")
	relatedHandle, err := w.getRelatedHandle(file)
	if err != nil {
		return newError("dbase-io-windows-readmemoheader-1", err)
	}
	if _, err := windows.Seek(*relatedHandle, 0, 0); err != nil {
		return newError("dbase-io-windows-readmemoheader-2", err)
	}
	b := make([]byte, 8)
	n, err := windows.Read(*relatedHandle, b)
	if err != nil {
		return newError("dbase-io-windows-readmemoheader-3", err)
	}
	h := &MemoHeader{}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return newError("dbase-io-windows-readmemoheader-4", err)
	}
	debugf("Memo header: %+v", h)
	file.relatedHandle = relatedHandle
	file.memoHeader = h
	return nil
}

func (w WindowsIO) ReadMemo(file *File, address []byte) ([]byte, bool, error) {
	if file.relatedHandle == nil {
		return nil, false, newError("dbase-io-windows-readmemo-1", ErrNoFPT)
	}
	relatedHandle, err := w.getRelatedHandle(file)
	if err != nil {
		return nil, false, newError("dbase-io-windows-readmemo-2", err)
	}
	// Determine the block number
	block := binary.LittleEndian.Uint32(address)
	if block == 0 {
		return []byte{}, false, nil
	}
	position := int64(file.memoHeader.BlockSize) * int64(block)
	debugf("Reading memo block %d at position %d", block, position)
	// The position in the file is blocknumber*blocksize
	_, err = windows.Seek(*relatedHandle, position, 0)
	if err != nil {
		return nil, false, newError("dbase-io-windows-readmemo-3", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = windows.Read(*relatedHandle, hbuf)
	if err != nil {
		return nil, false, newError("dbase-io-windows-readmemo-4", err)
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
	read, err := windows.Read(*relatedHandle, buf)
	if err != nil {
		return buf, false, newError("dbase-io-windows-readmemo-5", err)
	}
	if read != int(leng) {
		return buf, sign == 1, newError("dbase-io-windows-readmemo-6", ErrIncomplete)
	}
	if sign == 1 {
		buf, err = file.config.Converter.Decode(buf)
		if err != nil {
			return []byte{}, false, newError("dbase-io-windows-readmemo-7", err)
		}
	}
	return buf, sign == 1, nil
}

func (w WindowsIO) WriteMemo(file *File, raw []byte, text bool, length int) ([]byte, error) {
	file.memoMutex.Lock()
	defer file.memoMutex.Unlock()
	relatedHandle, err := w.getRelatedHandle(file)
	if err != nil {
		return nil, newError("dbase-io-windows-writememo-1", err)
	}
	blocks := 1
	blockPosition := file.memoHeader.NextFree
	if length > 0 && file.memoHeader.BlockSize > 0 {
		blocks = length / int(file.memoHeader.BlockSize)
		if length%int(file.memoHeader.BlockSize) > 0 {
			blocks++
		}
	}
	// Write the memo header
	err = file.WriteMemoHeader(blocks)
	if err != nil {
		return nil, newError("dbase-io-windows-writememo-2", err)
	}
	// Put the block data together
	data := make([]byte, 8)
	// The first 4 bytes are the signature, 1 for text, 0 for binary(image)
	if text {
		binary.BigEndian.PutUint32(data[:4], 1)
	} else {
		binary.BigEndian.PutUint32(data[:4], 0)
	}
	// The next 4 bytes are the length of the data
	binary.BigEndian.PutUint32(data[4:8], uint32(length))
	// The rest is the data
	data = append(data, raw...)
	// Lock the block we are writing to
	if file.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     blockPosition,
			OffsetHigh: blockPosition + uint32(file.memoHeader.BlockSize),
		}
		err = windows.LockFileEx(*relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, blockPosition, blockPosition+uint32(file.memoHeader.BlockSize), o)
		if err != nil {
			return nil, newError("dbase-io-windows-writememo-3", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*relatedHandle, 0, blockPosition, blockPosition+uint32(file.memoHeader.BlockSize), o)
			if err != nil {
				err = newError("dbase-io-windows-writememo-4", ulockErr)
			}
		}()
	}
	position := int64(blockPosition) * int64(file.memoHeader.BlockSize)
	debugf("Writing memo block %d at position %d", blockPosition, position)
	// Seek to new the next free block
	_, err = windows.Seek(*relatedHandle, position, 0)
	if err != nil {
		return nil, newError("dbase-io-windows-writememo-5", err)
	}
	// Write the memo data
	_, err = windows.Write(*relatedHandle, data)
	if err != nil {
		return nil, newError("dbase-io-windows-writememo-6", err)
	}
	// Convert the block number to []byte
	address, err := toBinary(blockPosition)
	if err != nil {
		return nil, newError("dbase-io-windows-writememo-7", err)
	}
	return address, nil
}

func (w WindowsIO) WriteMemoHeader(file *File, size int) (err error) {
	relatedHandle, err := w.getRelatedHandle(file)
	if err != nil {
		return newError("dbase-io-windows-writememoheader-1", err)
	}
	debugf("Writing memo header...")
	// Lock the block we are writing to
	o := &windows.Overlapped{
		Offset:     0,
		OffsetHigh: uint32(file.header.FirstRow),
	}
	if file.config.WriteLock {
		err = windows.LockFileEx(*relatedHandle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 0, uint32(file.header.FirstRow), o)
		if err != nil {
			return newError("dbase-io-windows-writememoheader-2", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*relatedHandle, 0, 0, uint32(file.header.FirstRow), o)
			if err != nil {
				err = newError("dbase-io-windows-writememoheader-3", ulockErr)
			}
		}()
	}
	// Seek to the beginning of the file
	_, err = windows.Seek(*relatedHandle, 0, 0)
	if err != nil {
		return newError("dbase-io-windows-writememoheader-4", err)
	}
	// Calculate the next free block
	file.memoHeader.NextFree += uint32(size)
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], file.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], file.memoHeader.BlockSize)
	debugf("Writing memo header - next free: %d, block size: %d", file.memoHeader.NextFree, file.memoHeader.BlockSize)
	_, err = windows.Write(*relatedHandle, buf)
	if err != nil {
		return newError("dbase-io-windows-writememoheader-5", err)
	}
	// Write null till end of header
	_, err = windows.Write(*relatedHandle, make([]byte, 512-8))
	if err != nil {
		return newError("dbase-io-windows-writememoheader-6", err)
	}
	return nil
}

func (w WindowsIO) ReadRow(file *File, position uint32) ([]byte, error) {
	handle, err := w.getHandle(file)
	if err != nil {
		return nil, newError("dbase-io-windows-readrow-1", err)
	}
	if position >= file.header.RowsCount {
		return nil, newError("dbase-io-windows-readrow-2", ErrEOF)
	}
	pos := int64(file.header.FirstRow) + (int64(position) * int64(file.header.RowLength))
	debugf("Reading row: %d at offset: %v", position, pos)
	buf := make([]byte, file.header.RowLength)
	_, err = windows.Seek(*handle, pos, 0)
	if err != nil {
		return buf, newError("dbase-io-windows-readrow-3", err)
	}
	read, err := windows.Read(*handle, buf)
	if err != nil {
		return buf, newError("dbase-io-windows-readrow-4", err)
	}
	if read != int(file.header.RowLength) {
		return buf, newError("dbase-io-windows-readrow-5", ErrIncomplete)
	}
	return buf, nil
}

// writeRow writes raw row data to the given row position
func (w WindowsIO) WriteRow(file *File, row *Row) (err error) {
	debugf("Writing row: %d ...", row.Position)
	row.handle.dbaseMutex.Lock()
	defer row.handle.dbaseMutex.Unlock()
	handle, err := w.getHandle(file)
	if err != nil {
		return newError("dbase-io-windows-writerow-1", err)
	}
	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return newError("dbase-io-windows-writerow-2", err)
	}
	// Update the header
	position := int64(row.handle.header.FirstRow) + (int64(row.Position) * int64(row.handle.header.RowLength))
	if row.Position >= row.handle.header.RowsCount {
		position = int64(row.handle.header.FirstRow) + (int64(row.Position-1) * int64(row.handle.header.RowLength))
		row.handle.header.RowsCount++
	}
	err = row.handle.WriteHeader()
	if err != nil {
		return newError("dbase-io-windows-writerow-3", err)
	}
	// Lock the block we are writing to
	if row.handle.config.WriteLock {
		o := &windows.Overlapped{
			Offset:     uint32(position),
			OffsetHigh: uint32(position + int64(row.handle.header.RowLength)),
		}
		err = windows.LockFileEx(*handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, uint32(position), uint32(position+int64(row.handle.header.RowLength)), o)
		if err != nil {
			return newError("dbase-io-windows-writerow-4", err)
		}
		defer func() {
			ulockErr := windows.UnlockFileEx(*handle, 0, uint32(position), uint32(position+int64(row.handle.header.RowLength)), o)
			if err != nil {
				err = newError("dbase-io-windows-writerow-5", ulockErr)
			}
		}()
	}
	debugf("Writing row: %d at offset: %v", row.Position, position)
	// Seek to the correct position
	_, err = windows.Seek(*handle, position, 0)
	if err != nil {
		return newError("dbase-io-windows-writerow-6", err)
	}
	// Write the row
	_, err = windows.Write(*handle, r)
	if err != nil {
		return newError("dbase-io-windows-writerow-7", err)
	}
	return nil
}

func (w WindowsIO) Search(file *File, field *Field, exactMatch bool) ([]*Row, error) {
	if field.column.DataType == 'M' {
		return nil, newError("dbase-io-windows-search-1", fmt.Errorf("searching memo fields is not supported"))
	}
	handle, err := w.getHandle(file)
	if err != nil {
		return nil, newError("dbase-io-windows-search-2", err)
	}
	debugf("Searching for value: %v in field: %s", field.GetValue(), field.column.Name())
	// convert the value to bytes
	val, err := file.GetRepresentation(field, !exactMatch)
	if err != nil {
		return nil, newError("dbase-io-windows-search-3", err)
	}
	// Search for the value
	rows := make([]*Row, 0)
	position := uint64(file.header.FirstRow)
	for i := uint32(0); i < file.header.RowsCount; i++ {
		// Read the field value
		p := int64(position) + int64(field.column.Position)
		debugf("Searching at position: %d", p)
		_, err := windows.Seek(*handle, p, 0)
		position += uint64(file.header.RowLength)
		if err != nil {
			continue
		}
		buf := make([]byte, field.column.Length)
		read, err := windows.Read(*handle, buf)
		if err != nil {
			continue
		}
		if read != int(field.column.Length) {
			continue
		}
		// Check if the value matches
		if bytes.Contains(buf, val) {
			debugf("Found matching field at position: %d - Record %v position: %v ", p, i+1, p-int64(field.column.Position))
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

func (w WindowsIO) GoTo(file *File, row uint32) error {
	if row > file.header.RowsCount {
		file.table.rowPointer = file.header.RowsCount
		return newError("dbase-io-windows-goto-1", fmt.Errorf("%w, go to %v > %v", ErrEOF, row, file.header.RowsCount))
	}
	debugf("Going to row: %d", row)
	file.table.rowPointer = row
	return nil
}

func (w WindowsIO) Skip(file *File, offset int64) {
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

func (w WindowsIO) Deleted(file *File) (bool, error) {
	if file.table.rowPointer >= file.header.RowsCount {
		return false, newError("dbase-io-windows-deleted-1", ErrEOF)
	}
	handle, err := w.getHandle(file)
	if err != nil {
		return false, newError("dbase-io-windows-deleted-2", err)
	}
	_, err = windows.Seek(*handle, int64(file.header.FirstRow)+(int64(file.table.rowPointer)*int64(file.header.RowLength)), 0)
	if err != nil {
		return false, newError("dbase-io-windows-deleted-3", err)
	}
	buf := make([]byte, 1)
	read, err := windows.Read(*handle, buf)
	if err != nil {
		return false, newError("dbase-io-windows-deleted-4", err)
	}
	if read != 1 {
		return false, newError("dbase-io-windows-deleted-5", ErrIncomplete)
	}
	return Marker(buf[0]) == Deleted, nil
}

func _findFile(name string) (string, error) {
	debugf("Searching for file: %s", name)
	// Read all files in the directory
	files, err := os.ReadDir(filepath.Dir(name))
	if err != nil {
		return "", newError("dbase-io-windows-findfile-1", err)
	}
	for _, file := range files {
		if strings.EqualFold(file.Name(), filepath.Base(name)) {
			debugf("Found file: %s", file.Name())
			return filepath.Join(filepath.Dir(name), file.Name()), nil
		}
	}
	return name, nil
}

func (w WindowsIO) getHandle(file *File) (*windows.Handle, error) {
	handle, ok := file.handle.(*windows.Handle)
	if !ok {
		return nil, newError("dbase-io-windows-gethandle-1", fmt.Errorf("handle is of wrong type %T expected *windows.Handle", file.handle))
	}
	if handle == nil || reflect.ValueOf(handle).IsNil() {
		return nil, newError("dbase-io-windows-gethandle-2", fmt.Errorf("handle is nil"))
	}
	return handle, nil
}

func (w WindowsIO) getRelatedHandle(file *File) (*windows.Handle, error) {
	handle, ok := file.relatedHandle.(*windows.Handle)
	if !ok {
		return nil, newError("dbase-io-windows-getrelatedhandle-1", fmt.Errorf("memo handle is of wrong type %T expected *windows.Handle", file.relatedHandle))
	}
	if handle == nil || reflect.ValueOf(handle).IsNil() {
		return nil, newError("dbase-io-windows-getrelatedhandle-2", fmt.Errorf("memo handle is nil"))
	}
	return handle, nil
}
