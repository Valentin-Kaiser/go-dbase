package dbase

import (
	"io"
	"log"
	"os"
)

var debug = false
var out = os.Stdout
var debugLogger = log.New(out, "[dbase] [DEBUG] ", log.LstdFlags)
var errorLogger = log.New(out, "[dbase] [ERROR] ", log.LstdFlags)

func SetDebug(enabled bool) {
	debug = enabled
}

func SetOutput(out io.Writer) {
	debugLogger.SetOutput(out)
	errorLogger.SetOutput(out)
}

func debugf(format string, v ...interface{}) {
	if debug {
		debugLogger.Printf(format, v...)
	}
}

func errorf(format string, v ...interface{}) {
	if debug {
		errorLogger.Printf(format, v...)
	}
}
