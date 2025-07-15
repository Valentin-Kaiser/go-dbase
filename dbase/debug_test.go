package dbase

import (
	"bytes"
	"strings"
	"testing"
)

func TestDebug(t *testing.T) {
	// Test enabling debug mode with custom output
	var buf bytes.Buffer
	Debug(true, &buf)

	// Test debug output
	debugf("test debug message: %s", "hello")

	// Check if debug message was written
	output := buf.String()
	if !strings.Contains(output, "test debug message: hello") {
		t.Errorf("Debug message not found in output: %s", output)
	}
	if !strings.Contains(output, "[dbase] [DEBUG]") {
		t.Errorf("Debug prefix not found in output: %s", output)
	}

	// Test disabling debug mode
	buf.Reset()
	Debug(false, &buf)
	debugf("this should not appear")

	if buf.Len() > 0 {
		t.Errorf("Debug message appeared when debug was disabled: %s", buf.String())
	}
}

func TestDebugWithNilWriter(t *testing.T) {
	// Test that Debug() handles nil writer gracefully
	Debug(true, nil)

	// This should not panic
	debugf("test message")

	// Reset to default state
	Debug(false, nil)
}

func TestDebugf(t *testing.T) {
	var buf bytes.Buffer
	Debug(true, &buf)

	// Test various format strings
	testCases := []struct {
		format   string
		args     []interface{}
		expected string
	}{
		{"simple message", nil, "simple message"},
		{"message with %s", []interface{}{"string"}, "message with string"},
		{"message with %d", []interface{}{42}, "message with 42"},
		{"message with %v and %v", []interface{}{"first", "second"}, "message with first and second"},
	}

	for _, tc := range testCases {
		buf.Reset()
		debugf(tc.format, tc.args...)
		output := buf.String()

		if !strings.Contains(output, tc.expected) {
			t.Errorf("Expected '%s' in output, got: %s", tc.expected, output)
		}
	}
}
