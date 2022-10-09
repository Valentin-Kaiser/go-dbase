package dbase

type DBaseError struct {
	context string
	err     error
}

func newError(context string, err error) DBaseError {
	return DBaseError{
		context: context,
		err:     err,
	}
}

func (e DBaseError) Error() string {
	return e.err.Error()
}

func (e DBaseError) Context() string {
	return e.context
}
