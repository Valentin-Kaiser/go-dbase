package dbase

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Used to open files from memory
type MemoryReader interface {
	io.ReadSeeker
	io.ReaderAt
}

type DBF struct {
	// Used with files loaded into memory
	dbaseReader MemoryReader
	memoReader  MemoryReader

	// Used with disk files
	dbaseFile *os.File
	memoFile  *os.File

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

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	dbf, err := prepareDBF(file, dec)
	if err != nil {
		return nil, err
	}

	dbf.dbaseFile = file

	// Check if there is an FPT according to the header.
	// If there is we will try to open it in the same dir (using the same filename and case).
	// If the FPT file does not exist an error is returned.
	if (dbf.dbaseHeader.TableFlags & 0x02) != 0 {
		ext := filepath.Ext(filename)
		fptExt := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptExt = ".FPT"
		}
		memoFile, err := os.Open(strings.TrimSuffix(filename, ext) + fptExt)
		if err != nil {
			return nil, err
		}

		err = dbf.prepareMemo(memoFile)
		if err != nil {
			return nil, err
		}

		dbf.memoFile = memoFile
	}

	return dbf, nil
}

// Creates a new DBF struct from a bytes stream.
// The memoFile parameter is optional, but if the DBF header has the FPT flag set, the memoFile must be provided.
func OpenStream(dbffile, memoFile MemoryReader, dec Decoder) (*DBF, error) {
	dbf, err := prepareDBF(dbffile, dec)
	if err != nil {
		return nil, err
	}

	if (dbf.dbaseHeader.TableFlags & 0x02) != 0 {
		if memoFile == nil {
			return nil, ERROR_NO_FPT_FILE.AsError()
		}
		err = dbf.prepareMemo(memoFile)
		if err != nil {
			return nil, err
		}
	}

	return dbf, nil
}

// Closes the file handlers.
func (dbf *DBF) Close() error {
	if dbf.dbaseFile != nil {
		err := dbf.dbaseFile.Close()
		if err != nil {
			return fmt.Errorf("closing DBF failed: %s", err)
		}
	}

	if dbf.memoFile != nil {
		err := dbf.memoFile.Close()
		if err != nil {
			return fmt.Errorf("closing FPT failed: %s", err)
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
func prepareDBF(dbfReader MemoryReader, dec Decoder) (*DBF, error) {
	header, err := readDBFHeader(dbfReader)
	if err != nil {
		return nil, err
	}

	// check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validateFileVersion(header.FileVersion); err != nil {
		return nil, err
	}

	// read fieldinfo
	fields, err := readFieldInfos(dbfReader)
	if err != nil {
		return nil, err
	}

	dbf := &DBF{
		dbaseHeader: header,
		dbaseReader: dbfReader,
		fields:      fields,
		decoder:     dec,
	}

	return dbf, nil
}

func readDBFHeader(r io.ReadSeeker) (*DBaseFileHeader, error) {
	h := &DBaseFileHeader{}
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	// integers in table files are stored with the least significant byte first.
	err := binary.Read(r, binary.LittleEndian, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// Reads field infos from DBF header, starting at pos 32, until it finds the Header record terminator (0x0D).
func readFieldInfos(r io.ReadSeeker) ([]FieldHeader, error) {
	fields := make([]FieldHeader, 0)

	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := r.Seek(offset, 0); err != nil {
			return nil, err
		}
		if _, err := r.Read(b); err != nil {
			return nil, err
		}
		if b[0] == 0x0D {
			break
		}
		// Position back one byte and read the field
		if _, err := r.Seek(-1, 1); err != nil {
			return nil, err
		}
		field := FieldHeader{}
		err := binary.Read(r, binary.LittleEndian, &field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		offset += 32
	}
	return fields, nil
}

// Reads field infos from DBF header, starting at pos 32.
// Reads fields until it finds the Header record terminator (0x0D).
func ReadHeaderFields(r io.ReadSeeker) ([]FieldHeader, error) {
	fields := make([]FieldHeader, 0)

	offset := int64(32)
	b := make([]byte, 1)
	for {
		// Check if we are at 0x0D by reading one byte ahead
		if _, err := r.Seek(offset, 0); err != nil {
			return nil, err
		}
		if _, err := r.Read(b); err != nil {
			return nil, err
		}
		if b[0] == 0x0D {
			break
		}
		// Position back one byte and read the field
		if _, err := r.Seek(-1, 1); err != nil {
			return nil, err
		}
		field := FieldHeader{}
		err := binary.Read(r, binary.LittleEndian, &field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		offset += 32
	}
	return fields, nil
}

func validateFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("untested DBF file version: %d (%x hex), try overriding ValidFileVersionFunc to open this file anyway", version, version)
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

func (dbf *DBF) prepareMemo(memoFileReader MemoryReader) error {
	memoHeader, err := readMemoHeader(memoFileReader)
	if err != nil {
		return err
	}

	dbf.memoReader = memoFileReader
	dbf.memoHeader = memoHeader
	return nil
}

func readMemoHeader(r io.ReadSeeker) (*MemoFileHeader, error) {
	h := &MemoFileHeader{}
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	err := binary.Read(r, binary.BigEndian, h)
	if err != nil {
		return nil, err
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

// Returns the os.FileInfo for the DBF file
func (dbf *DBF) DBaseFileStats() (os.FileInfo, error) {
	if dbf.dbaseFile == nil {
		return nil, ERROR_NO_DBF_FILE.AsError()
	}
	return dbf.dbaseFile.Stat()
}

// Returns the os.FileInfo for the FPT file
func (dbf *DBF) MemoFileStats() (os.FileInfo, error) {
	if dbf.memoFile == nil {
		return nil, ERROR_NO_FPT_FILE.AsError()
	}
	return dbf.memoFile.Stat()
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
	if recNumber >= dbf.dbaseHeader.RecordsCount {
		dbf.recordPointer = dbf.dbaseHeader.RecordsCount
		return ERROR_EOF.AsError()
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
		return ERROR_EOF.AsError()
	}
	if newval < 0 {
		dbf.recordPointer = 0
		return ERROR_BOF.AsError()
	}
	dbf.recordPointer = uint32(newval)
	return nil
}

// Returns all records
func (dbf *DBF) Records() ([]*Record, error) {
	records := make([]*Record, 0)
	for !dbf.EOF() {
		// This reads the complete record
		record, err := dbf.GetRecord()
		if err != nil {
			return nil, err
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
		return nil, err
	}

	return dbf.bytesToRecord(data)
}

// Reads field number fieldpos at the record number the internal pointer is pointing to and returns its Go value
func (dbf *DBF) Field(fieldPosition int) (interface{}, error) {
	data, err := dbf.readField(dbf.recordPointer, fieldPosition)
	if err != nil {
		return nil, err
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
		return nil, ERROR_EOF.AsError()
	}
	if fieldPosition < 0 || fieldPosition > int(dbf.FieldsCount()) {
		return nil, ERROR_INVALID.AsError()
	}
	buf := make([]byte, dbf.fields[fieldPosition].Length)
	pos := int64(dbf.dbaseHeader.FirstRecord) + (int64(recordPosition) * int64(dbf.dbaseHeader.RecordLength)) + int64(dbf.fields[fieldPosition].Position)
	read, err := dbf.dbaseReader.ReadAt(buf, pos)
	if err != nil {
		return buf, err
	}
	if read != int(dbf.fields[fieldPosition].Length) {
		return buf, ERROR_INCOMPLETE.AsError()
	}
	return buf, nil
}

// Reads raw record data of one record at recordPosition
func (dbf *DBF) readRecord(recordPosition uint32) ([]byte, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return nil, ERROR_EOF.AsError()
	}
	buf := make([]byte, dbf.dbaseHeader.RecordLength)
	read, err := dbf.dbaseReader.ReadAt(buf, int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)))
	if err != nil {
		return buf, err
	}
	if read != int(dbf.dbaseHeader.RecordLength) {
		return buf, ERROR_INCOMPLETE.AsError()
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
		return nil, ERROR_INVALID.AsError()
	}

	switch dbf.fields[fieldPosition].FieldType() {
	case "M":
		// M values contain the address in the FPT file from where to read data
		memo, isText, err := dbf.parseMemo(raw)
		if isText {
			return string(memo), err
		}
		return memo, err
	case "C":
		// C values are stored as strings, the returned string is not trimmed
		return dbf.toUTF8String(raw)
	case "I":
		// I values are stored as numeric values
		return int32(binary.LittleEndian.Uint32(raw)), nil
	case "B":
		// B (double) values are stored as numeric values
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
	case "D":
		// D values are stored as string in format YYYYMMDD, convert to time.Time
		return dbf.parseDate(raw)
	case "T":
		// T values are stores as two 4 byte integers
		//  integer one is the date in julian format
		//  integer two is the number of milliseconds since midnight
		// Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		return dbf.parseDateTime(raw)
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
			return dbf.parseNumericInt(raw)
		}
		fallthrough // same as "F"
	case "F":
		// F values are stored as string values
		return dbf.parseFloat(raw)
	default:
		return nil, fmt.Errorf("unsupported fieldtype: %s", dbf.fields[fieldPosition].FieldType())
	}
}

func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, err
	}
	if isText {
		memo, err = dbf.decoder.Decode(memo)
		if err != nil {
			return []byte{}, false, err
		}
	}
	return memo, isText, nil
}

// Reads one or more blocks from the FPT file, called for each memo field.
// the return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readMemo(blockdata []byte) ([]byte, bool, error) {

	if dbf.memoReader == nil {
		return nil, false, ERROR_NO_FPT_FILE.AsError()
	}

	// Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	// The position in the file is blocknumber*blocksize
	if _, err := dbf.memoReader.Seek(int64(dbf.memoHeader.BlockSize)*int64(block), 0); err != nil {
		return nil, false, err
	}

	// Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	// uints in one buffer and then convert, this saves seconds for large DBF files with many memo fields
	// as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err := dbf.memoReader.Read(hbuf)
	if err != nil {
		return nil, false, err
	}
	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])

	if leng == 0 {
		// No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}
	// Now read the actual data
	buf := make([]byte, leng)
	read, err := dbf.memoReader.Read(buf)
	if err != nil {
		return buf, false, err
	}
	if read != int(leng) {
		return buf, sign == 1, ERROR_INCOMPLETE.AsError()
	}
	return buf, sign == 1, nil
}

// Returns if the record at recordPosition is deleted
func (dbf *DBF) DeletedAt(recordPosition uint32) (bool, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return false, ERROR_EOF.AsError()
	}
	buf := make([]byte, 1)
	read, err := dbf.dbaseReader.ReadAt(buf, int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)))
	if err != nil {
		return false, err
	}
	if read != 1 {
		return false, ERROR_INCOMPLETE.AsError()
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
		return nil, errors.New("invalid record data, no delete flag found at beginning of record")
	}

	rec.Data = make([]interface{}, dbf.FieldsCount())

	offset := uint16(1) // deleted flag already read
	for i := 0; i < len(rec.Data); i++ {
		fieldinfo := dbf.fields[i]
		val, err := dbf.FieldToValue(data[offset:offset+uint16(fieldinfo.Length)], i)
		if err != nil {
			return rec, err
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
 *	#						Record helper
 *	################################################################
 */

// Returns a complete record as a map.
// If recordNumber > 0 it returns the record at recordNumber, if recordNumber <= 0 it returns the record at dbf.recordPointer
func (rec *Record) ToMap() (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for i, fn := range rec.DBF.FieldNames() {
		val, err := rec.Field(i)
		if err != nil {
			return out, fmt.Errorf("error on field %s (column %d): %s", fn, i, err)
		}
		out[fn] = val
	}
	return out, nil
}

// Returns a complete record as a JSON object.
// If recordNumber > 0 it returns the record at recordNumber, if recordNumber <= 0 it returns the record at dbf.recpointer.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (rec *Record) ToJSON(trimspaces bool) ([]byte, error) {
	m, err := rec.ToMap()
	if err != nil {
		return nil, err
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
func (rec *Record) ToStruct(v interface{}) error {
	jsonRecord, err := rec.ToJSON(true)
	if err != nil {
		return nil
	}

	err = json.Unmarshal(jsonRecord, v)
	if err != nil {
		return nil
	}

	return nil
}

// Field gets a fields value by field pos (index)
func (r *Record) Field(pos int) (interface{}, error) {
	if pos < 0 || len(r.Data) < pos {
		return 0, ERROR_INVALID.AsError()
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
		return time.Time{}, ERROR_INVALID.AsError()
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
		return string(raw), err
	}
	return string(utf8), nil
}
