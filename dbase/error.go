package dbase

import (
	"errors"
	"fmt"
)

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
	ErrInvalidPosition = errors.New("INVALID_POSITION")
	ErrInvalidEncoding = errors.New("INVALID_ENCODING")
	// Returned when an invalid data type is used
	ErrUnknownDataType = errors.New("UNKNOWN_DATA_TYPE")
)

// Error is a wrapper for errors that occur in the dbase package
type Error struct {
	context []string
	err     error
}

func newError(context string, err error) Error {
	errorf("%s:%s", context, GetErrorTrace(err))
	if err != nil {
		var dbaseError Error
		if errors.As(err, &dbaseError) {
			ctx := dbaseError.Context()
			ctx = append(ctx, context)
			dbaseError = Error{
				context: ctx,
				err:     err,
			}
			return dbaseError
		}
	}
	return Error{
		context: []string{context},
		err:     err,
	}
}

// Error returns the error message of the underlying error
func (e Error) Error() string {
	return e.err.Error()
}

// Context returns the context of the error in the dbase package
func (e Error) Context() []string {
	return e.context
}

// trace returns the context of the error in the dbase package as a string
func (e Error) trace() string {
	trace := ""
	// append reverse order
	for i := len(e.context) - 1; i >= 0; i-- {
		trace += e.context[i] + ":"
	}
	return trace
}

// GetErrorTrace returns the context and the error produced by the dbase package as a string
func GetErrorTrace(err error) error {
	if err == nil {
		return nil
	}
	var dbaseError Error
	if errors.As(err, &dbaseError) {
		return fmt.Errorf("%s%w", dbaseError.trace(), dbaseError)
	}
	return err
}
