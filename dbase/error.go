package dbase

import "errors"

var (
	// Returned when the end of a dBase database file is reached
	ErrEOF = errors.New("EOF")
	// // Returned when the row pointer is attempted to be moved before the first row
	ErrBOF = errors.New("BOF")
	// // Returned when the read of a row or column did not finish
	ErrIncomplete = errors.New("INCOMPLETE")
	// // Returned when a file operation is attempted on a non existent file
	ErrNoFPT = errors.New("FPT_FILE_NOT_FOUND")
	ErrNoDBF = errors.New("DBF_FILE_NOT_FOUND")
	// // Returned when an invalid column position is used (x<1 or x>number of columns)
	ErrInvalidPosition = errors.New("INVALID_Position")
	ErrInvalidEncoding = errors.New("INVALID_ENCODING")
)

type Error struct {
	context string
	err     error
}

func newError(context string, err error) Error {
	return Error{
		context: context,
		err:     err,
	}
}

func (e Error) Error() string {
	return e.err.Error()
}

func (e Error) Context() string {
	return e.context
}
