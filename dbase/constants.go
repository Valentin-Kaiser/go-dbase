package dbase

import "errors"

var (
	// Returned when the end of a dBase database file is reached
	ErrEOF error = errors.New("EOF")
	// // Returned when the row pointer is attempted to be moved before the first row
	ErrBOF error = errors.New("BOF")
	// // Returned when the read of a row or column did not finish
	ErrIncomplete error = errors.New("INCOMPLETE")
	// // Returned when a file operation is attempted on a non existent file
	ErrNoFPT error = errors.New("FPT_FILE_NOT_FOUND")
	ErrNoDBF error = errors.New("DBF_FILE_NOT_FOUND")
	// // Returned when an invalid column position is used (x<1 or x>number of columns)
	ErrInvalidPosition error = errors.New("INVALID_Position")
	ErrInvalidEncoding error = errors.New("INVALID_ENCODING")
)

const (
	// Supported and testet file types - other file types may work but are not tested
	// The file type check has to be bypassed when opening, if the file type is not supported
	FoxPro              byte = 0x30
	FoxProAutoincrement byte = 0x31

	// Relevant byte marker
	Null      byte = 0x00
	Blank     byte = 0x20
	ColumnEnd byte = 0x0D
	Active         = Blank
	Deleted        = 0x2A
	EOFMarker byte = 0x1A

	// DBase Table flags
	Structural     byte = 0x01
	Memo           byte = 0x02
	StructuralMemo byte = 0x03
	Database       byte = 0x04
)
