package dbase

import (
	"testing"
	"time"
)

func TestJulianDate(t *testing.T) {
	// Test the julian date conversion functions
	testCases := []struct {
		year, month, day int
		expected         int
	}{
		{2023, 12, 25, 2460304}, // Christmas 2023 (corrected)
		{2000, 1, 1, 2451545},   // Y2K
		{1900, 1, 1, 2415021},   // Start of 20th century
	}

	for _, tc := range testCases {
		result := julianDate(tc.year, tc.month, tc.day)
		if result != tc.expected {
			t.Errorf("julianDate(%d, %d, %d) = %d, expected %d", 
				tc.year, tc.month, tc.day, result, tc.expected)
		}

		// Test round-trip conversion
		y, m, d := julianToDate(result)
		if y != tc.year || m != tc.month || d != tc.day {
			t.Errorf("julianToDate(%d) = (%d, %d, %d), expected (%d, %d, %d)", 
				result, y, m, d, tc.year, tc.month, tc.day)
		}
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected time.Time
		hasError bool
	}{
		{"valid date", []byte("20231225"), time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC), false},
		{"empty date", []byte(""), time.Time{}, false},
		{"invalid date", []byte("invalid"), time.Time{}, true},
		{"short date", []byte("2023"), time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDate(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error for input %v, but got none", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %v: %v", tt.input, err)
				}
				if !result.Equal(tt.expected) {
					t.Errorf("Expected %v, got %v for input %v", tt.expected, result, tt.input)
				}
			}
		})
	}
}

func TestSanitizeEmptyBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{"normal string", []byte("hello"), []byte("hello")},
		{"with null bytes", []byte("hello\x00world"), []byte("helloworld")},
		{"with spaces", []byte("  hello  "), []byte("hello")},
		{"with null and spaces", []byte("  hello\x00world  "), []byte("helloworld")},
		{"empty", []byte(""), []byte("")},
		{"only nulls", []byte("\x00\x00"), []byte("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeEmptyBytes(tt.input)
			if string(result) != string(tt.expected) {
				t.Errorf("Expected %q, got %q for input %q", 
					string(tt.expected), string(result), string(tt.input))
			}
		})
	}
}

func TestSanitizeSpaces(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal string", "hello world", "hello world"},
		{"multiple spaces", "hello   world", "hello world"},
		{"tabs and spaces", "hello\t\t world", "hello world"},
		{"newlines", "hello\n\nworld", "hello world"},
		{"mixed whitespace", "hello \t\n world", "hello world"},
		{"empty", "", ""},
		{"only spaces", "   ", " "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeSpaces(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q for input %q", 
					tt.expected, result, tt.input)
			}
		})
	}
}

func TestAppendSpaces(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		length   int
		expected []byte
	}{
		{"normal append", []byte("hello"), 10, []byte("hello     ")},
		{"exact length", []byte("hello"), 5, []byte("hello")},
		{"shorter than length", []byte("hello"), 3, []byte("hello")},
		{"empty input", []byte(""), 5, []byte("     ")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendSpaces(tt.input, tt.length)
			if string(result) != string(tt.expected) {
				t.Errorf("Expected %q, got %q for input %q with length %d", 
					string(tt.expected), string(result), string(tt.input), tt.length)
			}
		})
	}
}

func TestGetNthBit(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		bitNum   int
		expected bool
	}{
		{"first bit set", []byte{0x01}, 0, true},
		{"first bit not set", []byte{0x00}, 0, false},
		{"second bit set", []byte{0x02}, 1, true},
		{"out of range", []byte{0x01}, 10, false},
		{"multiple bytes", []byte{0x00, 0x01}, 8, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getNthBit(tt.input, tt.bitNum)
			if result != tt.expected {
				t.Errorf("Expected %t, got %t for input %v bit %d", 
					tt.expected, result, tt.input, tt.bitNum)
			}
		})
	}
}

func TestSetNthBit(t *testing.T) {
	tests := []struct {
		name     string
		input    byte
		bitNum   int
		expected byte
	}{
		{"set first bit", 0x00, 0, 0x01},
		{"set second bit", 0x00, 1, 0x02},
		{"set already set bit", 0x01, 0, 0x01},
		{"set higher bit", 0x00, 7, 0x80},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := setNthBit(tt.input, tt.bitNum)
			if result != tt.expected {
				t.Errorf("Expected 0x%02x, got 0x%02x for input 0x%02x bit %d", 
					tt.expected, result, tt.input, tt.bitNum)
			}
		})
	}
}