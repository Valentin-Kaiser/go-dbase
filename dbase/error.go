package dbase

import "errors"

var (
	// Returned when the end of a dBase database file is reached
	ErrEOF = errors.New("EOF")
	// Returned when the row pointer is attempted to be moved before the first row
	ErrBOF = errors.New("BOF")
	// Returned when the read of a row or column did not finish
	ErrIncomplete = errors.New("INCOMPLETE")
	// Returned when a file operation is attempted on a non existent file
	ErrNoFPT = errors.New("FPT_FILE_NOT_FOUND")
	ErrNoDBF = errors.New("DBF_FILE_NOT_FOUND")
	// Returned when an invalid column position is used (x<1 or x>number of columns)
	ErrInvalidPosition = errors.New("INVALID_Position")
	ErrInvalidEncoding = errors.New("INVALID_ENCODING")
)

// Error is a wrapper for errors that occur in the dbase package
type Error struct {
	context string
	err     error
}

// newError creates a new Error
func newError(context string, err error) Error {
	return Error{
		context: context,
		err:     err,
	}
}

// Error returns the error message
func (e Error) Error() string {
	return e.err.Error()
}

// Context returns the context of the error in the dbase package
func (e Error) Context() string {
	return e.context
}
