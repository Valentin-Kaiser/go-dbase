package dbase

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Used to open files from memory
type MemoryReader interface {
	io.ReadSeeker
	io.ReaderAt
}

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

// Contains the raw record data and a deleted flag
type Record struct {
	DBF     *DBF
	Deleted bool
	Data    []interface{}
}

/**
 *	################################################################
 *	#					Stream and File handler
 *	################################################################
 */

// OpenFile opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func OpenFile(filename string, dec Decoder) (*DBF, error) {
	filename = filepath.Clean(filename)

	fd, err := syscall.Open(filename, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-open-file-1:FAILED:%v", err)
	}

	dbf, err := prepareDBF(fd, dec)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-open-file-2:FAILED:%v", err)
	}

	dbf.dbaseFileHandle = &fd

	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.dbaseHeader.TableFlags & 0x02) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		fd, err := syscall.Open(strings.TrimSuffix(filename, ext)+fptExt, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0644)
		if err != nil {
			return nil, fmt.Errorf("dbase-reader-open-file-3:FAILED:%v", err)
		}

		err = dbf.prepareMemo(fd)
		if err != nil {
			return nil, fmt.Errorf("dbase-reader-open-file-4:FAILED:%v", err)
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
			return fmt.Errorf("dbase-reader-close-1:FAILED:Closing DBF failed with error: %v", err)
		}
	}

	if dbf.memoFileHandle != nil {
		err := syscall.Close(*dbf.memoFileHandle)
		if err != nil {
			return fmt.Errorf("dbase-reader-close-2:FAILED:Closing FPT failed with error: %v", err)
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
// Reads the DBF Header, the field infos and validates file version.
func prepareDBF(fd syscall.Handle, dec Decoder) (*DBF, error) {
	header, err := readDBFHeader(fd)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-prepare-dbf-1:FAILED:%v", err)
	}

	// check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileVersion); err != nil {
		return nil, fmt.Errorf("dbase-reader-prepare-dbf-2:FAILED:%v", err)
	}

	// read fieldinfo
	fields, err := readFieldInfos(fd)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-prepare-dbf-3:FAILED:%v", err)
	}

	dbf := &DBF{
		dbaseHeader:     header,
		dbaseFileHandle: &fd,
		fields:          fields,
		decoder:         dec,
	}

	return dbf, nil
}

func readDBFHeader(fd syscall.Handle) (*DBaseFileHeader, error) {
	h := &DBaseFileHeader{}
	if _, err := syscall.Seek(syscall.Handle(fd), 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-reader-read-dbf-header-1:FAILED:%v", err)
	}

	b := make([]byte, 1024)
	n, err := syscall.Read(syscall.Handle(fd), b)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-read-dbf-header-2:FAILED:%v", err)
	}

	// integers in table files are stored with the least significant byte first.
	err = binary.Read(bytes.NewReader(b[:n]), binary.LittleEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-read-dbf-header-3:FAILED:%v", err)
	}
	return h, nil
}

// Reads field infos from DBF header, starting at pos 32, until it finds the Header record terminator (0x0D).
func readFieldInfos(fd syscall.Handle) ([]FieldHeader, error) {
	fields := make([]FieldHeader, 0)

	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := syscall.Seek(syscall.Handle(fd), offset, 0); err != nil {
			return nil, fmt.Errorf("dbase-reader-read-field-infos-1:FAILED:%v", err)
		}
		if _, err := syscall.Read(syscall.Handle(fd), b); err != nil {
			return nil, fmt.Errorf("dbase-reader-read-field-infos-2:FAILED:%v", err)
		}
		if b[0] == 0x0D {
			break
		}

		// Position back one byte and read the field
		if _, err := syscall.Seek(syscall.Handle(fd), -1, 1); err != nil {
			return nil, fmt.Errorf("dbase-reader-read-field-infos-3:FAILED:%v", err)
		}

		buf := make([]byte, 2048)
		n, err := syscall.Read(syscall.Handle(fd), buf)
		if err != nil {
			return nil, fmt.Errorf("dbase-reader-read-field-infos-4:FAILED:%v", err)
		}

		field := FieldHeader{}
		err = binary.Read(bytes.NewReader(buf[:n]), binary.LittleEndian, &field)
		if err != nil {
			return nil, fmt.Errorf("dbase-reader-read-field-infos-5:FAILED:%v", err)
		}

		if field.FieldName() == "_NullFlags" {
			offset += 32
			continue
		}

		fields = append(fields, field)

		offset += 32
	}
	return fields, nil
}

func validateFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("dbase-reader-validate-file-version-1:FAILED:untested DBF file version: %d (%x hex)", version, version)
	case 0x30, 0x31:
		return nil
	}
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

// GoTo sets the internal record pointer to record recNumber
// Returns and EOF error if at EOF and positions the pointer at lastRecord+1
func (dbf *DBF) GoTo(recNumber uint32) error {
	if recNumber > dbf.dbaseHeader.RecordsCount {
		dbf.recordPointer = dbf.dbaseHeader.RecordsCount
		return fmt.Errorf("dbase-reader-go-to-1:FAILED:go to %v > %v:%v", recNumber, dbf.dbaseHeader.RecordsCount, ERROR_EOF.AsError())
	}
	dbf.recordPointer = recNumber
	return nil
}

// Skip adds offset to the internal record pointer
// Returns EOF error if at end of file and positions the pointer at lastRecord+1
// Returns BOF error is the record pointer would be become negative and positions the pointer at 0
// Does not skip deleted records
func (dbf *DBF) Skip(offset int64) error {
	newval := int64(dbf.recordPointer) + offset
	if newval >= int64(dbf.dbaseHeader.RecordsCount) {
		dbf.recordPointer = dbf.dbaseHeader.RecordsCount
		return fmt.Errorf("dbase-reader-skip-1:FAILED:%v", ERROR_EOF.AsError())
	}
	if newval < 0 {
		dbf.recordPointer = 0
		return fmt.Errorf("dbase-reader-skip-2:FAILED:%v", ERROR_BOF.AsError())
	}
	dbf.recordPointer = uint32(newval)
	return nil
}

// Returns all records
func (dbf *DBF) Records(skipInvalid bool) ([]*Record, error) {
	records := make([]*Record, 0)
	for !dbf.EOF() {
		// This reads the complete record
		record, err := dbf.GetRecord()
		if err != nil && !skipInvalid {
			return nil, fmt.Errorf("dbase-reader-records-1:FAILED:%v", err)
		}

		dbf.Skip(1)
		// skip deleted records
		if record.Deleted {
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

// Returns the requested record at dbf.recordPointer.
func (dbf *DBF) GetRecord() (*Record, error) {
	data, err := dbf.readRecord(dbf.recordPointer)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-get-record-1:FAILED:%v", err)
	}

	return dbf.bytesToRecord(data)
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

// Returns if the internal recordpointer is at end of file
func (dbf *DBF) EOF() bool {
	return dbf.recordPointer >= dbf.dbaseHeader.RecordsCount
}

// Returns if the internal recordpointer is before first record
func (dbf *DBF) BOF() bool {
	return dbf.recordPointer == 0
}

// Reads raw field data of one field at fieldPosition at recordPosition
func (dbf *DBF) readField(recordPosition uint32, fieldPosition int) ([]byte, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return nil, fmt.Errorf("dbase-reader-read-field-1:FAILED:%v", ERROR_EOF.AsError())
	}

	if fieldPosition < 0 || fieldPosition > int(dbf.FieldsCount()) {
		return nil, fmt.Errorf("dbase-reader-read-field-2:FAILED:%v", ERROR_INVALID.AsError())
	}

	buf := make([]byte, dbf.fields[fieldPosition].Length)
	pos := int64(dbf.dbaseHeader.FirstRecord) + (int64(recordPosition) * int64(dbf.dbaseHeader.RecordLength)) + int64(dbf.fields[fieldPosition].Position)

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), pos, 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-field-3:FAILED:%v", err)
	}

	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-field-4:FAILED:%v", err)
	}

	if read != int(dbf.fields[fieldPosition].Length) {
		return buf, fmt.Errorf("dbase-reader-read-field-5:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, nil
}

// Reads raw record data of one record at recordPosition
func (dbf *DBF) readRecord(recordPosition uint32) ([]byte, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return nil, fmt.Errorf("dbase-reader-read-record-1:FAILED:%v", ERROR_EOF.AsError())
	}
	buf := make([]byte, dbf.dbaseHeader.RecordLength)

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)), 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-record-2:FAILED:%v", err)
	}

	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-record-3:FAILED:%v", err)
	}

	if read != int(dbf.dbaseHeader.RecordLength) {
		return buf, fmt.Errorf("dbase-reader-read-record-1:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, nil
}

// Converts raw field data to the correct type for the given field
// For C and M fields a charset conversion is done
// For M fields the data is read from the memo file
func (dbf *DBF) FieldToValue(raw []byte, fieldPosition int) (interface{}, error) {
	// Not all field types have been implemented because we don't use them in our DBFs
	// Extend this function if needed
	if fieldPosition < 0 || len(dbf.fields) < fieldPosition {
		return nil, fmt.Errorf("dbase-reader-field-to-value-1:FAILED:%v", ERROR_INVALID.AsError())
	}

	switch dbf.fields[fieldPosition].FieldType() {
	case "M":
		// M values contain the address in the FPT file from where to read data
		memo, isText, err := dbf.parseMemo(raw)
		if isText {
			if err != nil {
				return string(memo), fmt.Errorf("dbase-reader-field-to-value-2:FAILED:%v", err)
			}
			return string(memo), nil
		}
		return memo, nil
	case "C":
		// C values are stored as strings, the returned string is not trimmed
		str, err := dbf.toUTF8String(raw)
		if err != nil {
			return str, fmt.Errorf("dbase-reader-field-to-value-4:FAILED:%v", err)
		}
		return str, nil
	case "I":
		// I values are stored as numeric values
		return int32(binary.LittleEndian.Uint32(raw)), nil
	case "B":
		// B (double) values are stored as numeric values
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
	case "D":
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		date, err := dbf.parseDate(raw)
		if err != nil {
			return date, fmt.Errorf("dbase-reader-field-to-value-5:FAILED:%v", err)
		}
		return date, nil
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		dateTime, err := dbf.parseDateTime(raw)
		if err != nil {
			return dateTime, fmt.Errorf("dbase-reader-field-to-value-6:FAILED:%v", err)
		}
		return dateTime, nil
	case "L":
		// L values are stored as strings T or F, we only check for T, the rest is false...
		return string(raw) == "T", nil
	case "V":
		// V values just return the raw value
		return raw, nil
	case "Y":
		// Y values are currency values stored as ints with 4 decimal places
		return float64(float64(binary.LittleEndian.Uint64(raw)) / 10000), nil
	case "N":
		// N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		if dbf.fields[fieldPosition].Decimals == 0 {
			i, err := dbf.parseNumericInt(raw)
			if err != nil {
				return i, fmt.Errorf("dbase-reader-field-to-value-7:FAILED:%v", err)
			}
			return i, nil
		}
		fallthrough // same as "F"
	case "F":
		// F values are stored as string values
		f, err := dbf.parseFloat(raw)
		if err != nil {
			return f, fmt.Errorf("dbase-reader-field-to-value-8:FAILED:%v", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("dbase-reader-field-to-value-9:FAILED:Unsupported fieldtype: %s", dbf.fields[fieldPosition].FieldType())
	}
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

// Reads one or more blocks from the FPT file, called for each memo field.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readMemo(blockdata []byte) ([]byte, bool, error) {

	if dbf.memoFileHandle == nil {
		return nil, false, fmt.Errorf("dbase-reader-read-memo-1:FAILED:%v", ERROR_NO_FPT_FILE.AsError())
	}

	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	_, err := syscall.Seek(syscall.Handle(*dbf.memoFileHandle), int64(dbf.memoHeader.BlockSize)*int64(block), 0)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-reader-read-memo-2:FAILED:%v", err)
	}

	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo fields
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	read, err := syscall.Read(syscall.Handle(*dbf.memoFileHandle), hbuf)
	if err != nil {
		return nil, false, fmt.Errorf("dbase-reader-read-memo-3:FAILED:%v", err)
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
		return buf, false, fmt.Errorf("dbase-reader-read-memo-4:FAILED:%v", err)
	}
	if read != int(leng) {
		return buf, sign == 1, fmt.Errorf("dbase-reader-read-memo-5:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, sign == 1, nil
}

// Returns if the record at recordPosition is deleted
func (dbf *DBF) DeletedAt(recordPosition uint32) (bool, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return false, fmt.Errorf("dbase-reader-deleted-at-1:FAILED:%v", ERROR_EOF.AsError())
	}

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)), 0)
	if err != nil {
		return false, fmt.Errorf("dbase-reader-deleted-at-2:FAILED:%v", err)
	}

	buf := make([]byte, 1)
	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return false, fmt.Errorf("dbase-reader-deleted-at-3:FAILED:%v", err)
	}
	if read != 1 {
		return false, fmt.Errorf("dbase-reader-deleted-at-4:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf[0] == 0x2A, nil
}

// Returns if the record at the internal record pos is deleted
func (dbf *DBF) Deleted() (bool, error) {
	return dbf.DeletedAt(dbf.recordPointer)
}

// Converts raw record data to a Record struct
// If the data points to a memo (FPT) file this file is also read
func (dbf *DBF) bytesToRecord(data []byte) (*Record, error) {
	rec := &Record{}
	rec.DBF = dbf

	// a record should start with te delete flag, a space (0x20) or * (0x2A)
	rec.Deleted = data[0] == 0x2A
	if !rec.Deleted && data[0] != 0x20 {
		return nil, fmt.Errorf("dbase-reader-bytes-to-record-1:FAILED:invalid record data, no delete flag found at beginning of record")
	}

	rec.Data = make([]interface{}, dbf.FieldsCount())

	offset := uint16(1) // deleted flag already read
	for i := 0; i < len(rec.Data); i++ {
		fieldinfo := dbf.fields[i]
		val, err := dbf.FieldToValue(data[offset:offset+uint16(fieldinfo.Length)], i)
		if err != nil {
			return rec, fmt.Errorf("dbase-reader-bytes-to-record-2:FAILED:%v", err)
		}
		rec.Data[i] = val

		offset += uint16(fieldinfo.Length)
	}

	return rec, nil
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

/**
 *	################################################################
 *	#						Conversion helper
 *	################################################################
 */

// Returns all records as a slice of maps.
func (dbf *DBF) RecordsToMap(skipInvalid bool) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0)

	records, err := dbf.Records(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		rmap, err := record.ToMap()
		if err != nil {
			return nil, err
		}

		out = append(out, rmap)
	}

	return out, nil
}

// Returns all records as json
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (dbf *DBF) RecordsToJSON(skipInvalid bool, trimspaces bool) ([]byte, error) {
	records, err := dbf.RecordsToMap(skipInvalid)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-to-json-1:FAILED:%v", err)
	}

	mapRecords := make([]map[string]interface{}, 0)
	for _, record := range records {
		if trimspaces {
			for k, v := range record {
				if str, ok := v.(string); ok {
					record[k] = strings.TrimSpace(str)
				}
			}
		}
		mapRecords = append(mapRecords, record)
	}

	return json.Marshal(mapRecords)
}

// Returns all records as a slice of struct.
// Parses the record from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the record into a struct, json.Unmarshal matches incoming object keys to either the struct field name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
// v keeps the last converted struct.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (dbf *DBF) RecordsToStruct(v interface{}, skipInvalid bool, trimspaces bool) ([]interface{}, error) {
	out := make([]interface{}, 0)

	records, err := dbf.Records(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		err := record.ToStruct(v, trimspaces)
		if err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, nil
}

// Returns a complete record as a map.
func (rec *Record) ToMap() (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for i, fn := range rec.DBF.FieldNames() {
		val, err := rec.Field(i)
		if err != nil {
			return out, fmt.Errorf("dbase-reader-to-map-1:FAILED:error on field %s (column %d): %s", fn, i, err)
		}
		out[fn] = val
	}
	return out, nil
}

// Returns a complete record as a JSON object.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (rec *Record) ToJSON(trimspaces bool) ([]byte, error) {
	m, err := rec.ToMap()
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-to-json-1:FAILED:%v", err)
	}
	if trimspaces {
		for k, v := range m {
			if str, ok := v.(string); ok {
				m[k] = strings.TrimSpace(str)
			}
		}
	}
	return json.Marshal(m)
}

// Parses the record from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the record into a struct, json.Unmarshal matches incoming object keys to either the struct field name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
func (rec *Record) ToStruct(v interface{}, trimspaces bool) error {
	jsonRecord, err := rec.ToJSON(trimspaces)
	if err != nil {
		return fmt.Errorf("dbase-reader-to-struct-1:FAILED:%v", err)
	}

	err = json.Unmarshal(jsonRecord, v)
	if err != nil {
		return fmt.Errorf("dbase-reader-to-struct-2:FAILED:%v", err)
	}

	return nil
}

// Field gets a fields value by field pos (index)
func (r *Record) Field(pos int) (interface{}, error) {
	if pos < 0 || len(r.Data) < pos {
		return 0, fmt.Errorf("dbase-reader-field-1:FAILED:%v", ERROR_INVALID.AsError())
	}
	return r.Data[pos], nil
}

// FieldSlice gets all fields as a slice
func (r *Record) FieldSlice() []interface{} {
	return r.Data
}

/**
 *	################################################################
 *	#					Field data type helper
 *	################################################################
 */

func (dbf *DBF) parseDate(raw []byte) (time.Time, error) {
	if string(raw) == strings.Repeat(" ", 8) {
		return time.Time{}, nil
	}
	return time.Parse("20060102", string(raw))
}

func (dbf *DBF) parseDateTime(raw []byte) (time.Time, error) {
	if len(raw) != 8 {
		return time.Time{}, fmt.Errorf("dbase-reader-parse-date-time-1:FAILED:%v", ERROR_INVALID.AsError())
	}
	julDat := int(binary.LittleEndian.Uint32(raw[:4]))
	mSec := int(binary.LittleEndian.Uint32(raw[4:]))

	// Determine year, month, day
	y, m, d := JD2YMD(julDat)
	if y < 0 || y > 9999 {
		return time.Time{}, nil
	}

	// Calculate whole seconds and use the remainder as nanosecond resolution
	nSec := mSec / 1000
	mSec = mSec - (nSec * 1000)

	// Create time using ymd and nanosecond timestamp
	return time.Date(y, time.Month(m), d, 0, 0, nSec, mSec*int(time.Millisecond), time.UTC), nil
}

func (dbf *DBF) parseNumericInt(raw []byte) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return int64(0), nil
	}
	return strconv.ParseInt(trimmed, 10, 64)
}

func (dbf *DBF) parseFloat(raw []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return float64(0.0), nil
	}
	return strconv.ParseFloat(strings.TrimSpace(string(trimmed)), 64)
}

// toUTF8String converts a byte slice to a UTF8 string using the decoder in dbf
func (dbf *DBF) toUTF8String(raw []byte) (string, error) {
	utf8, err := dbf.decoder.Decode(raw)
	if err != nil {
		return string(raw), fmt.Errorf("dbase-reader-to-utf8-string-1:FAILED:%v", err)
	}
	return string(utf8), nil
}
