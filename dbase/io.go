package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
)

/**
 *	################################################################
 *	#					Stream and File handler
 *	################################################################
 */

// Opens a dBase database file (and the memo file if needed) from disk.
// To close the embedded file handle(s) call DBF.Close().
func Open(filename string, dec Decoder) (*DBF, error) {
	filename = filepath.Clean(filename)

	// open file in non blocking mode with syscall
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

func validateFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("dbase-reader-validate-file-version-1:FAILED:untested DBF file version: %d (%x hex)", version, version)
	case 0x30, 0x31:
		return nil
	}
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
