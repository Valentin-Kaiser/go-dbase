package dbase

import (
	"errors"
	"strings"
	"testing"
)

func TestErrorHandling(t *testing.T) {
	tests := []struct {
		context     string
		underlying  error
		expectedMsg string
		description string
	}{
		{"sampleContext1", ErrEOF, "EOF", "EOF Error"},
		{"sampleContext2", ErrBOF, "BOF", "BOF Error"},
		// Add more tests as necessary
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			err := newError(tt.context, tt.underlying)
			if err.Error() != tt.expectedMsg {
				t.Errorf("got %s, want %s", err.Error(), tt.expectedMsg)
			}

			if len(err.Context()) != 1 || err.Context()[0] != tt.context {
				t.Errorf("got context %v, want %s", err.Context(), tt.context)
			}

			trace := err.trace()
			if !strings.Contains(trace, tt.context) {
				t.Errorf("trace %s does not contain context %s", trace, tt.context)
			}
		})
	}
}

func TestGetErrorTrace(t *testing.T) {
	tests := []struct {
		inputError  error
		expected    string
		description string
	}{
		{newError("sampleContext1", ErrEOF), "sampleContext1:EOF", "Custom Error with EOF"},
		{newError("sampleContext2", ErrBOF), "sampleContext2:BOF", "Custom Error with BOF"},
		{ErrIncomplete, "INCOMPLETE", "Package-level error"},
		{errors.New("generic error"), "generic error", "Generic error"},
		// Add more tests as necessary
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			trace := GetErrorTrace(tt.inputError)
			if trace.Error() != tt.expected {
				t.Errorf("got %s, want %s", trace.Error(), tt.expected)
			}
		})
	}
}
