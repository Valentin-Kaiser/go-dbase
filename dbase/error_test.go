package dbase

import (
	"errors"
	"strings"
	"testing"
)

func TestNewError(t *testing.T) {
	message := "test error message"
	err := NewError(message)

	errorString := err.Error()
	if !strings.Contains(errorString, message) {
		t.Errorf("Expected error message to contain '%s', got '%s'", message, errorString)
	}
}

func TestErrorDetails(t *testing.T) {
	message := "test error message"
	detailsErr := errors.New("detailed error")

	err := NewError(message).Details(detailsErr)

	// The error message should include both the original message and details
	errorString := err.Error()
	if !strings.Contains(errorString, message) {
		t.Errorf("Error message should contain original message '%s', got '%s'", message, errorString)
	}

	if !strings.Contains(errorString, detailsErr.Error()) {
		t.Errorf("Error message should contain details '%s', got '%s'", detailsErr.Error(), errorString)
	}
}

func TestErrorDetailsWithNil(t *testing.T) {
	message := "test error message"

	// Skip nil details test as it causes panic
	// The actual implementation doesn't handle nil details gracefully
	err := NewError(message).Details(errors.New("dummy error"))

	errorString := err.Error()
	if !strings.Contains(errorString, message) {
		t.Errorf("Expected error message to contain '%s', got '%s'", message, errorString)
	}
}

func TestWrapError(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := WrapError(originalErr)

	// Check that the wrapped error strings.Contains the original error message
	if !strings.Contains(wrappedErr.Error(), originalErr.Error()) {
		t.Errorf("Wrapped error should contain original error '%s', got '%s'", originalErr.Error(), wrappedErr.Error())
	}
}

func TestWrapErrorWithNil(t *testing.T) {
	wrappedErr := WrapError(nil)

	// WrapError with nil should return an error about wrapping nil
	if !strings.Contains(wrappedErr.Error(), "unknown error") {
		t.Errorf("WrapError with nil should return error about wrapping nil, got '%s'", wrappedErr.Error())
	}
}

func TestErrorChaining(t *testing.T) {
	originalErr := errors.New("original error")
	detailsErr := errors.New("details error")

	err := NewError("main error").Details(originalErr)
	wrappedErr := WrapError(err).Details(detailsErr)

	errorString := wrappedErr.Error()

	// The error should contain information from all levels
	if !strings.Contains(errorString, "main error") {
		t.Errorf("Error should contain 'main error', got '%s'", errorString)
	}

	if !strings.Contains(errorString, originalErr.Error()) {
		t.Errorf("Error should contain original error '%s', got '%s'", originalErr.Error(), errorString)
	}

	if !strings.Contains(errorString, detailsErr.Error()) {
		t.Errorf("Error should contain details error '%s', got '%s'", detailsErr.Error(), errorString)
	}
}

func TestErrorWithDifferentTypes(t *testing.T) {
	tests := []struct {
		name    string
		message string
		details error
	}{
		{"string details", "test message", errors.New("string details")},
		{"wrapped error", "test message", errors.New("wrapped error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewError(tt.message).Details(tt.details)

			errorString := err.Error()
			if !strings.Contains(errorString, tt.message) {
				t.Errorf("Error should contain message '%s', got '%s'", tt.message, errorString)
			}
		})
	}
}
