package dbase

import (
	"io"
	"log"
	"os"
)

var (
	debug       = false
	debugLogger = log.New(os.Stdout, "[dbase] [DEBUG] ", log.LstdFlags)
	errorLogger = log.New(os.Stdout, "[dbase] [ERROR] ", log.LstdFlags)
)

// Debug the dbase package
// If debug is true, debug messages will be printed to the defined io.Writter (default: os.Stdout)
func Debug(enabled bool, out io.Writer) {
	if out != nil {
		debugLogger.SetOutput(out)
		errorLogger.SetOutput(out)
	}
	debug = enabled
}

func debugf(format string, v ...interface{}) {
	if debug {
		debugLogger.Printf(format, v...)
	}
}
