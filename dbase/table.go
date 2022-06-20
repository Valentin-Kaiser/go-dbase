package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
	"time"
)

type DBF struct {
	dbaseFileHandle *syscall.Handle
	memoFileHandle  *syscall.Handle

	decoder Decoder

	dbaseHeader *DBaseFileHeader
	memoHeader  *MemoFileHeader

	fields []FieldHeader

	recordPointer uint32 // Internal record pointer, can be moved
}

// Containing all raw DBF header fields.
type DBaseFileHeader struct {
	FileVersion  byte     // File type flag
	Year         uint8    // Last update year (0-99)
	Month        uint8    // Last update month
	Day          uint8    // Last update day
	RecordsCount uint32   // Number of records in file
	FirstRecord  uint16   // Position of first data record
	RecordLength uint16   // Length of one data record, including delete flag
	Reserved     [16]byte // Reserved
	TableFlags   byte     // Table flags
	CodePage     byte     // Code page mark
}

// The raw header of the Memo file.
type MemoFileHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

// Contains the raw field info structure from the DBF header.
type FieldHeader struct {
	Name     [11]byte // Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
	Type     byte     // Field type
	Position uint32   // Displacement of field in record
	Length   uint8    // Length of field (in bytes)
	Decimals uint8    // Number of decimal places
	Flags    byte     // Field flags
	Next     uint32   // Value of autoincrement Next value
	Step     uint16   // Value of autoincrement Step value
	Reserved [8]byte  // Reserved
}

/**
 *	################################################################
 *	#					dBase file header handler
 *	################################################################
 */

// Parses the year, month and day to time.Time.
// Note: the year is stored in 2 digits, 15 is 2015
func (h *DBaseFileHeader) Modified() time.Time {
	return time.Date(2000+int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.Local)
}

// Returns the calculated number of fields from the header info alone (without the need to read the fieldinfo from the header).
// This is the fastest way to determine the number of records in the file.
// Note: when OpenFile is used the fields have already been parsed so it is better to call DBF.FieldsCount in that case.
func (h *DBaseFileHeader) FieldsCount() uint16 {
	return uint16((h.FirstRecord - 296) / 32)
}

// Returns the calculated file size based on the header info
func (h *DBaseFileHeader) FileSize() int64 {
	return 296 + int64(h.FieldsCount()*32) + int64(h.RecordsCount*uint32(h.RecordLength))
}

/**
 *	################################################################
 *	#					dBase memo file handler
 *	################################################################
 */

func (dbf *DBF) prepareMemo(fd syscall.Handle) error {
	memoHeader, err := readMemoHeader(fd)
	if err != nil {
		return fmt.Errorf("dbase-reader-prepare-memo-1:FAILED:%v", err)

	}

	dbf.memoFileHandle = &fd
	dbf.memoHeader = memoHeader
	return nil
}

func readMemoHeader(fd syscall.Handle) (*MemoFileHeader, error) {
	h := &MemoFileHeader{}
	if _, err := syscall.Seek(syscall.Handle(fd), 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-reader-read-memo-header-1:FAILED:%v", err)
	}

	b := make([]byte, 1024)
	n, err := syscall.Read(syscall.Handle(fd), b)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-read-memo-header-2:FAILED:%v", err)
	}

	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-read-memo-header-3:FAILED:%v", err)
	}
	return h, nil
}

/**
 *	################################################################
 *	#						DBF helper
 *	################################################################
 */

// Returns if the internal recordpointer is at end of file
func (dbf *DBF) EOF() bool {
	return dbf.recordPointer >= dbf.dbaseHeader.RecordsCount
}

// Returns if the internal recordpointer is before first record
func (dbf *DBF) BOF() bool {
	return dbf.recordPointer == 0
}

// Returns the dBase database file header struct for inspecting
func (dbf *DBF) Header() *DBaseFileHeader {
	return dbf.dbaseHeader
}

// returns the number of records
func (dbf *DBF) RecordsCount() uint32 {
	return dbf.dbaseHeader.RecordsCount
}

// Returns all the FieldHeaders
func (dbf *DBF) Fields() []FieldHeader {
	return dbf.fields
}

// Returns the number of fields
func (dbf *DBF) FieldsCount() uint16 {
	return uint16(len(dbf.fields))
}

// Returns a slice of all the field names
func (dbf *DBF) FieldNames() []string {
	num := len(dbf.fields)
	names := make([]string, num)
	for i := 0; i < num; i++ {
		names[i] = dbf.fields[i].FieldName()
	}
	return names
}

// Returns the field position of a fieldname or -1 if not found.
func (dbf *DBF) FieldPos(fieldname string) int {
	for i := 0; i < len(dbf.fields); i++ {
		if dbf.fields[i].FieldName() == fieldname {
			return i
		}
	}
	return -1
}

// Reads field number fieldpos at the record number the internal pointer is pointing to and returns its Go value
func (dbf *DBF) Field(fieldPosition int) (interface{}, error) {
	data, err := dbf.readField(dbf.recordPointer, fieldPosition)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-field-1:FAILED:%v", err)
	}
	// fieldPosition is valid or readField would have returned an error
	return dbf.FieldToValue(data, fieldPosition)
}

func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, fmt.Errorf("dbase-reader-parse-memo-1:FAILED:%v", err)
	}
	if isText {
		memo, err = dbf.decoder.Decode(memo)
		if err != nil {
			return []byte{}, false, fmt.Errorf("dbase-reader-parse-memo-2:FAILED:%v", err)
		}
	}
	return memo, isText, nil
}

/**
 *	################################################################
 *	#						FieldHeader helper
 *	################################################################
 */

// Returns the name of the field as a trimmed string (max length 10)
func (f *FieldHeader) FieldName() string {
	return string(bytes.TrimRight(f.Name[:], "\x00"))
}

// Returns the type of the field as string (length 1)
func (f *FieldHeader) FieldType() string {
	return string(f.Type)
}
