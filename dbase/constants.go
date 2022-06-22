package dbase

import "fmt"

type DBaseError string

const (
	// returned when the end of a dBase database file is reached
	ERROR_EOF DBaseError = "EOF"
	// returned when the row pointer is attempted to be moved before the first row
	ERROR_BOF DBaseError = "BOF"
	// returned when the read of a row or column did not finish
	ERROR_INCOMPLETE DBaseError = "INCOMPLETE"
	// returned when an invalid column position is used (x<1 or x>number of columns)
	ERROR_INVALID DBaseError = "INVALID"
	// returned when a file operation is attempted on a non existent file
	ERROR_NO_DBF_FILE      DBaseError = "FPT_FILE_NOT_FOUND"
	ERROR_NO_FPT_FILE      DBaseError = "DBF_FILE_NOT_FOUND"
	ERROR_INVALID_ENCODING DBaseError = "INVALID_ENCODING"

	// Supported file types
	FOXPRO               byte = 0x30
	FOXPRO_AUTOINCREMENT byte = 0x31

	// Relevant byte marker
	NULL          byte = 0x00
	BLANK         byte = 0x20
	END_OF_COLUMN byte = 0x0D
	ACTIVE             = BLANK
	DELETED            = 0x2A
	EOF_MARKER    byte = 0x1A

	// dBase Table flags
	STRUCTURAL      byte = 0x01
	MEMO            byte = 0x02
	STRUCTURAL_MEMO byte = 0x03
	DATABASE        byte = 0x04
)

func (re DBaseError) AsError() error {
	return fmt.Errorf(string(re))
}
