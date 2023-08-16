package dbase

import (
	"bytes"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

func TestDefaultConverter_Decode(t *testing.T) {
	tests := []struct {
		converter   DefaultConverter
		input       []byte
		expected    []byte
		hasError    bool
		description string
	}{
		{DefaultConverter{encoding: charmap.Windows1252}, []byte("sample"), []byte("sample"), false, "Windows1252 Encoding"},
		// Add tests for other encodings and edge cases
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := tt.converter.Decode(tt.input)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestDefaultConverter_Encode(t *testing.T) {
	tests := []struct {
		converter   DefaultConverter
		input       []byte
		expected    []byte
		hasError    bool
		description string
	}{
		{DefaultConverter{encoding: charmap.Windows1252}, []byte("sample"), []byte("sample"), false, "Windows1252 Encoding"},
		// Add tests for other encodings and edge cases
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := tt.converter.Encode(tt.input)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestDefaultConverter_CodePage(t *testing.T) {
	tests := []struct {
		converter   DefaultConverter
		expected    byte
		description string
	}{
		{DefaultConverter{encoding: charmap.Windows1252}, 0x03, "Windows1252 Encoding"},
		// Add tests for other encodings based on the switch-case in the CodePage function
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got := tt.converter.CodePage()
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}
