package dbase

import (
	"bytes"
	"strings"
	"testing"
)

func TestDebug(t *testing.T) {
	buf := new(bytes.Buffer)

	// Enable debugging and redirect output to buffer.
	Debug(true, buf)

	// Test debugf.
	expectedDebugMsg := "This is a debug message"
	debugf(expectedDebugMsg)

	if !strings.Contains(buf.String(), expectedDebugMsg) {
		t.Errorf("debugf didn't log the expected message. Got: %s", buf.String())
	}

	// Test errorf.
	expectedErrorMsg := "This is an error message"
	errorf(expectedErrorMsg)

	if !strings.Contains(buf.String(), expectedErrorMsg) {
		t.Errorf("errorf didn't log the expected message. Got: %s", buf.String())
	}

	buf.Reset()

	// Disable debugging.
	Debug(false, buf)

	// Test debugf again.
	debugf(expectedDebugMsg)

	if strings.Contains(buf.String(), expectedDebugMsg) {
		t.Errorf("debugf should not log when debugging is disabled. Got: %s", buf.String())
	}

	// Test errorf after disabling debugging. It should not log.
	errorf(expectedErrorMsg)

	if strings.Contains(buf.String(), expectedErrorMsg) {
		t.Errorf("errorf should not log when debugging is disabled. Got: %s", buf.String())
	}
}
