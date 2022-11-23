package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

// GenericIO implements the IO interface for generic io.ReadWriteSeeker.
// Handle is the main file handle, relatedHandle is the memo file handle.
type GenericIO struct {
	Handle        io.ReadWriteSeeker
	RelatedHandle io.ReadWriteSeeker
}

func (g GenericIO) OpenTable(config *Config) (*File, error) {
	if config == nil {
		return nil, newError("dbase-io-generic-opentable-1", fmt.Errorf("missing configuration"))
	}
	debugf("Opening table from custom io interface - Untested: %v - Trim spaces: %v - ValidateCodepage: %v - InterpretCodepage: %v", config.Untested, config.TrimSpaces, config.ValidateCodePage, config.InterpretCodePage)
	file := &File{
		config:        config,
		io:            g,
		handle:        g.Handle,
		relatedHandle: g.RelatedHandle,
		dbaseMutex:    &sync.Mutex{},
		memoMutex:     &sync.Mutex{},
	}
	err := file.ReadHeader()
	if err != nil {
		return nil, newError("dbase-io-generic-preparedbf-2", err)
	}
	// Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := ValidateFileVersion(file.header.FileType, config.Untested); err != nil {
		return nil, newError("dbase-io-generic-preparedbf-3", err)
	}
	columns, nullFlag, err := file.ReadColumns()
	if err != nil {
		return nil, newError("dbase-io-generic-preparedbf-4", err)
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
		return nil, newError("dbase-io-generic-opentable-5", fmt.Errorf("code page mark mismatch: %d != %d", file.header.CodePage, file.config.Converter.CodePage()))
	}

	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if MemoFlag.Defined(file.header.TableFlags) {
		if file.relatedHandle == nil {
			return nil, newError("dbase-io-generic-opentable-6", fmt.Errorf("no related handle defined"))
		}
		err = file.ReadMemoHeader()
		if err != nil {
			return nil, newError("dbase-io-generic-opentable-7", err)
		}
	}
	return file, nil
}

func (g GenericIO) Close(file *File) error {
	if file.handle != nil {
		handle, ok := file.handle.(io.Closer)
		if !ok {
			return newError("dbase-io-generic-close-1", fmt.Errorf("handle is of wrong type %T expected io.Closer", file.handle))
		}

		debugf("Closing file: %s", file.config.Filename)
		err := handle.Close()
		if err != nil {
			return newError("dbase-io-generic-close-2", fmt.Errorf("closing DBF failed with error: %w", err))
		}
	}
	if file.relatedHandle != nil {
		relatedHandle, ok := file.relatedHandle.(io.Closer)
		if !ok {
			return newError("dbase-io-generic-close-3", fmt.Errorf("handle is of wrong type %T expected *windows.Handle", file.handle))
		}

		debugf("Closing related file: %s", file.config.Filename)
		err := relatedHandle.Close()
		if err != nil {
			return newError("dbase-io-generic-close-4", fmt.Errorf("closing FPT failed with error: %w", err))
		}
	}
	return nil
}

func (g GenericIO) Create(file *File) error {
	file.handle = g.Handle
	if file.memoHeader != nil {
		file.relatedHandle = g.RelatedHandle
	}
	return nil
}

func (g GenericIO) ReadHeader(file *File) error {
	debugf("Reading header...")
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-generic-readheader-1", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}
	_, err := handle.Seek(0, 0)
	if err != nil {
		return newError("dbase-io-generic-generic-readheader-2", err)
	}
	b := make([]byte, 30)
	n, err := handle.Read(b)
	if err != nil {
		return newError("dbase-io-generic-generic-readheader-3", err)
	}
	h := &Header{}
	// LittleEndian - Integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return newError("dbase-io-generic-generic-readheader-4", err)
	}
	file.header = h
	return nil
}

func (g GenericIO) WriteHeader(file *File) error {
	debugf("Writing header...")
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-generic-writeheader-1", fmt.Errorf("invalid handle"))
	}

	_, err := handle.Seek(0, 0)
	if err != nil {
		return newError("dbase-io-generic-generic-writeheader-2", err)
	}
	// Change the last modification date to the current date
	file.header.Year = uint8(time.Now().Year() - 2000)
	file.header.Month = uint8(time.Now().Month())
	file.header.Day = uint8(time.Now().Day())

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, file.header)
	if err != nil {
		return newError("dbase-io-generic-generic-writeheader-3", err)
	}

	b, err := handle.Write(buf.Bytes())
	if err != nil {
		return newError("dbase-io-generic-generic-writeheader-4", err)
	}

	if b != len(buf.Bytes()) {
		return newError("dbase-io-generic-generic-writeheader-5", fmt.Errorf("wrote %d bytes, expected %d", b, len(buf.Bytes())))
	}

	return nil
}

func (g GenericIO) ReadColumns(file *File) ([]*Column, *Column, error) {
	debugf("Reading columns...")
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return nil, nil, newError("dbase-io-generic-generic-readcolumns-1", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}
	var nullFlag *Column
	columns := make([]*Column, 0)
	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := handle.Seek(offset, 0); err != nil {
			return nil, nil, newError("dbase-io-generic-generic-readcolumns-2", err)
		}
		if _, err := handle.Read(b); err != nil {
			return nil, nil, newError("dbase-io-generic-generic-readcolumns-3", err)
		}
		if Marker(b[0]) == ColumnEnd {
			break
		}
		// Position back one byte and read the column
		if _, err := handle.Seek(-1, 1); err != nil {
			return nil, nil, newError("dbase-io-generic-generic-readcolumns-4", err)
		}
		buf := make([]byte, 33)
		n, err := handle.Read(buf)
		if err != nil {
			return nil, nil, newError("dbase-io-generic-generic-readcolumns-5", err)
		}
		column := &Column{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, column)
		if err != nil {
			return nil, nil, newError("dbase-io-generic-generic-readcolumns-6", err)
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

func (g GenericIO) WriteColumns(file *File) error {
	debugf("Writing columns...")
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-generic-writeheader-1", fmt.Errorf("invalid handle"))
	}

	// Seek to the beginning of the file
	_, err := handle.Seek(32, 0)
	if err != nil {
		return newError("dbase-io-generic-generic-writecolumns-1", err)
	}
	// Write the columns
	buf := new(bytes.Buffer)
	for _, column := range file.table.columns {
		debugf("Writing column: %+v", column)
		err = binary.Write(buf, binary.LittleEndian, column)
		if err != nil {
			return newError("dbase-io-generic-generic-writecolumns-2", err)
		}
	}
	if file.nullFlagColumn != nil {
		debugf("Writing null flag column: %s", file.nullFlagColumn.Name())
		err = binary.Write(buf, binary.LittleEndian, file.nullFlagColumn)
		if err != nil {
			return newError("dbase-io-generic-generic-writecolumns-3", err)
		}
	}
	_, err = handle.Write(buf.Bytes())
	if err != nil {
		return newError("dbase-io-generic-generic-writecolumns-4", err)
	}
	// Write the column terminator
	_, err = handle.Write([]byte{byte(ColumnEnd)})
	if err != nil {
		return newError("dbase-io-generic-generic-writecolumns-5", err)
	}
	// Write null till the end of the header
	pos := file.header.FirstRow - uint16(len(file.table.columns)*32) - 33
	if file.nullFlagColumn != nil {
		pos -= 32
	}
	_, err = handle.Write(make([]byte, pos))
	if err != nil {
		return newError("dbase-io-generic-writecolumns-7", err)
	}
	return nil
}

func (g GenericIO) ReadMemoHeader(file *File) error {
	debugf("Reading memo header...")
	relatedHandle, ok := file.relatedHandle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-readmemoheader-1", fmt.Errorf("invalid handle"))
	}
	h := &MemoHeader{}
	if _, err := relatedHandle.Seek(0, 0); err != nil {
		return newError("dbase-io-generic-read-memo-header-2", err)
	}
	b := make([]byte, 8)
	n, err := relatedHandle.Read(b)
	if err != nil {
		return newError("dbase-io-generic-read-memo-header-3", err)
	}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return newError("dbase-io-generic-read-memo-header-4", err)
	}
	debugf("Memo header: %+v", h)
	file.memoHeader = h
	return nil
}

func (g GenericIO) WriteMemoHeader(file *File, size int) error {
	if file.relatedHandle == nil {
		return newError("dbase-io-generic-writememoheader-1", ErrNoFPT)
	}
	relatedHandle, ok := file.relatedHandle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-writememoheader-2", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}

	debugf("Writing memo header...")
	// Seek to the beginning of the file
	_, err := relatedHandle.Seek(0, 0)
	if err != nil {
		return newError("dbase-io-generic-writememoheader-3", err)
	}
	// Calculate the next free block
	file.memoHeader.NextFree += uint32(size)
	// Write the memo header
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], file.memoHeader.NextFree)
	binary.BigEndian.PutUint16(buf[6:8], file.memoHeader.BlockSize)
	debugf("Writing memo header - next free: %d, block size: %d", file.memoHeader.NextFree, file.memoHeader.BlockSize)
	_, err = relatedHandle.Write(buf)
	if err != nil {
		return newError("dbase-io-generic-writememoheader-5", err)
	}
	// Write null till end of header
	_, err = relatedHandle.Write(make([]byte, 512-8))
	if err != nil {
		return newError("dbase-io-generic-writememoheader-6", err)
	}
	return nil
}

func (g GenericIO) ReadMemo(file *File, address []byte) ([]byte, bool, error) {
	relatedHandle, ok := file.relatedHandle.(io.ReadWriteSeeker)
	if !ok {
		return nil, false, newError("dbase-io-generic-readmemo-1", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}

	// Determine the block number
	block := binary.LittleEndian.Uint32(address)
	if block == 0 {
		return []byte{}, false, nil
	}
	position := int64(file.memoHeader.BlockSize) * int64(block)
	debugf("Reading memo block %d at position %d", block, position)
	// The position in the file is blocknumber*blocksize
	_, err := relatedHandle.Seek(position, 0)
	if err != nil {
		return nil, false, newError("dbase-io-generic-readmemo-2", err)
	}
	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo columns
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err = relatedHandle.Read(hbuf)
	if err != nil {
		return nil, false, newError("dbase-io-generic-readmemo-3", err)
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
	read, err := relatedHandle.Read(buf)
	if err != nil {
		return buf, false, newError("dbase-io-generic-readmemo-4", err)
	}
	if read != int(leng) {
		return buf, sign == 1, newError("dbase-io-generic-readmemo-5", ErrIncomplete)
	}
	if sign == 1 {
		buf, err = file.config.Converter.Decode(buf)
		if err != nil {
			return []byte{}, false, newError("dbase-io-generic-readmemo-6", err)
		}
	}
	return buf, sign == 1, nil
}

func (g GenericIO) WriteMemo(file *File, raw []byte, text bool, length int) ([]byte, error) {
	file.memoMutex.Lock()
	defer file.memoMutex.Unlock()
	if file.relatedHandle == nil {
		return nil, newError("dbase-io-generic-writememo-1", ErrNoFPT)
	}
	relatedHandle, ok := file.relatedHandle.(io.ReadWriteSeeker)
	if !ok {
		return nil, newError("dbase-io-generic-writememo-2", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}
	// Get the block position
	blockPosition := file.memoHeader.NextFree
	blocks := length / int(file.memoHeader.BlockSize)
	if length%int(file.memoHeader.BlockSize) > 0 {
		blocks++
	}
	// Write the memo header
	err := file.WriteMemoHeader(blocks)
	if err != nil {
		return nil, newError("dbase-io-generic-writememo-2", err)
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
	position := int64(blockPosition) * int64(file.memoHeader.BlockSize)
	debugf("Writing memo block %d at position %d", blockPosition, position)
	// Seek to new the next free block
	_, err = relatedHandle.Seek(position, 0)
	if err != nil {
		return nil, newError("dbase-io-generic-writememo-4", err)
	}
	// Write the memo data
	_, err = relatedHandle.Write(data)
	if err != nil {
		return nil, newError("dbase-io-generic-writememo-5", err)
	}
	// Convert the block number to []byte
	address, err := toBinary(blockPosition)
	if err != nil {
		return nil, newError("dbase-io-generic-writememo-6", err)
	}
	return address, nil
}

func (g GenericIO) ReadNullFlag(file *File, position uint64, column *Column) (bool, bool, error) {
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return false, false, newError("dbase-io-generic-readnullflag-1", fmt.Errorf("invalid handle"))
	}

	if file.nullFlagColumn == nil {
		return false, false, newError("dbase-io-generic-readnullflag-2", fmt.Errorf("null flag column missing"))
	}
	if column.DataType != byte(Varchar) && column.DataType != byte(Varbinary) {
		return false, false, newError("dbase-io-generic-readnullflag-3", fmt.Errorf("column not a varchar or varbinary"))
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
	position = uint64(file.header.FirstRow) + position*uint64(file.header.RowLength) + uint64(file.nullFlagColumn.Position)
	_, err := handle.Seek(int64(position), 0)
	if err != nil {
		return false, false, newError("dbase-io-generic-readnullflag-1", err)
	}
	buf := make([]byte, file.nullFlagColumn.Length)
	n, err := handle.Read(buf)
	if err != nil {
		return false, false, newError("dbase-io-generic-readnullflag-2", err)
	}
	if n != int(file.nullFlagColumn.Length) {
		return false, false, newError("dbase-io-generic-readnullflag-3", fmt.Errorf("read %d bytes, expected %d", n, file.nullFlagColumn.Length))
	}
	if column.Flag == byte(NullableFlag) || column.Flag == byte(NullableFlag|BinaryFlag) {
		debugf("Read _NullFlag for column %s => varlength: %v - null: %v", column.Name(), nthBit(buf, bitCount), nthBit(buf, bitCount+1))
		return nthBit(buf, bitCount), nthBit(buf, bitCount+1), nil
	}
	debugf("Read _NullFlag for column %s => varlength: %v ", column.Name(), nthBit(buf, bitCount))
	return nthBit(buf, bitCount), false, nil
}

func (g GenericIO) ReadRow(file *File, position uint32) ([]byte, error) {
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return nil, newError("dbase-io-generic-readrow-1", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}
	if position >= file.header.RowsCount {
		return nil, newError("dbase-io-generic-readrow-2", ErrEOF)
	}
	pos := int64(file.header.FirstRow) + (int64(position) * int64(file.header.RowLength))
	debugf("Reading row: %d at offset: %v", position, pos)
	buf := make([]byte, file.header.RowLength)
	_, err := handle.Seek(pos, 0)
	if err != nil {
		return buf, newError("dbase-io-generic-readrow-3", err)
	}
	read, err := handle.Read(buf)
	if err != nil {
		return buf, newError("dbase-io-generic-readrow-4", err)
	}
	if read != int(file.header.RowLength) {
		return buf, newError("dbase-io-generic-readrow-5", ErrIncomplete)
	}
	return buf, nil
}

func (g GenericIO) WriteRow(file *File, row *Row) error {
	debugf("Writing row: %d ...", row.Position)
	row.handle.dbaseMutex.Lock()
	defer row.handle.dbaseMutex.Unlock()
	handle, ok := row.handle.handle.(io.ReadWriteSeeker)
	if !ok {
		return newError("dbase-io-generic-writerow-1", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", row.handle.handle))
	}

	// Convert the row to raw bytes
	r, err := row.ToBytes()
	if err != nil {
		return newError("dbase-io-generic-writerow-1", err)
	}
	// Update the header
	position := int64(row.handle.header.FirstRow) + (int64(row.Position) * int64(row.handle.header.RowLength))
	if row.Position >= row.handle.header.RowsCount {
		position = int64(row.handle.header.FirstRow) + (int64(row.Position-1) * int64(row.handle.header.RowLength))
		row.handle.header.RowsCount++
	}
	err = row.handle.WriteHeader()
	if err != nil {
		return newError("dbase-io-generic-writerow-2", err)
	}
	debugf("Writing row: %d at offset: %v", row.Position, position)
	// Seek to the correct position
	_, err = handle.Seek(position, 0)
	if err != nil {
		return newError("dbase-io-generic-writerow-3", err)
	}
	// Write the row
	_, err = handle.Write(r)
	if err != nil {
		return newError("dbase-io-generic-writerow-4", err)
	}
	return nil
}

func (g GenericIO) Search(file *File, field *Field, exactMatch bool) ([]*Row, error) {
	if field.column.DataType == 'M' {
		return nil, newError("dbase-io-generic-search-1", fmt.Errorf("searching memo fields is not supported"))
	}
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return nil, newError("dbase-io-generic-search-2", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}

	debugf("Searching for value: %v in field: %s", field.GetValue(), field.column.Name())
	// convert the value to bytes
	val, err := file.GetRepresentation(field, !exactMatch)
	if err != nil {
		return nil, newError("dbase-io-generic-search-3", err)
	}
	// Search for the value
	rows := make([]*Row, 0)
	position := uint64(file.header.FirstRow)
	for i := uint32(0); i < file.header.RowsCount; i++ {
		// Read the field value
		p := int64(position) + int64(field.column.Position)
		debugf("Searching at position: %d", p)
		_, err := handle.Seek(p, 0)
		position += uint64(file.header.RowLength)
		if err != nil {
			continue
		}
		buf := make([]byte, field.column.Length)
		read, err := handle.Read(buf)
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

func (g GenericIO) GoTo(file *File, row uint32) error {
	if row > file.header.RowsCount {
		file.table.rowPointer = file.header.RowsCount
		return newError("dbase-io-generic-goto-1", fmt.Errorf("%w, go to %v > %v", ErrEOF, row, file.header.RowsCount))
	}
	debugf("Going to row: %d", row)
	file.table.rowPointer = row
	return nil
}

func (g GenericIO) Skip(file *File, offset int64) {
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

func (g GenericIO) Deleted(file *File) (bool, error) {
	if file.table.rowPointer >= file.header.RowsCount {
		return false, newError("dbase-io-generic-deleted-1", ErrEOF)
	}
	handle, ok := file.handle.(io.ReadWriteSeeker)
	if !ok {
		return false, newError("dbase-io-generic-deleted-2", fmt.Errorf("handle is of wrong type %T expected io.ReadWriteSeeker", file.handle))
	}

	_, err := handle.Seek(int64(file.header.FirstRow)+(int64(file.table.rowPointer)*int64(file.header.RowLength)), 0)
	if err != nil {
		return false, newError("dbase-io-generic-deleted-3", err)
	}
	buf := make([]byte, 1)
	read, err := handle.Read(buf)
	if err != nil {
		return false, newError("dbase-io-generic-deleted-4", err)
	}
	if read != 1 {
		return false, newError("dbase-io-generic-deleted-5", ErrIncomplete)
	}
	return Marker(buf[0]) == Deleted, nil
}
