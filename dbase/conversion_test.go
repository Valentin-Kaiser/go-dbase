package dbase

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/text/encoding/charmap"
)

func TestJulianDate(t *testing.T) {
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

func TestParseNumericInt(t *testing.T) {
	testCases := []struct {
		input    []byte
		expected int64
		hasError bool
	}{
		{[]byte("12345"), 12345, false},
		{[]byte("  123  "), 123, false},
		{[]byte("-456"), -456, false},
		{[]byte("0"), 0, false},
		{[]byte(""), 0, false}, // Empty string returns 0, no error
		{[]byte("invalid"), 0, true},
		{[]byte("12.34"), 0, true}, // Should fail for float in int parsing
	}

	for _, tc := range testCases {
		result, err := parseNumericInt(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("Expected error for input %s, got none", string(tc.input))
		}
		if !tc.hasError && err != nil {
			t.Errorf("Unexpected error for input %s: %v", string(tc.input), err)
		}
		if !tc.hasError && result != tc.expected {
			t.Errorf("For input %s, expected %d, got %d", string(tc.input), tc.expected, result)
		}
	}
}

func TestParseFloat(t *testing.T) {
	testCases := []struct {
		input    []byte
		expected float64
		hasError bool
	}{
		{[]byte("123.456"), 123.456, false},
		{[]byte("  12.34  "), 12.34, false},
		{[]byte("-45.67"), -45.67, false},
		{[]byte("0.0"), 0.0, false},
		{[]byte("123"), 123.0, false},
		{[]byte(""), 0.0, false}, // Empty string returns 0, no error
		{[]byte("invalid"), 0.0, true},
	}

	for _, tc := range testCases {
		result, err := parseFloat(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("Expected error for input %s, got none", string(tc.input))
		}
		if !tc.hasError && err != nil {
			t.Errorf("Unexpected error for input %s: %v", string(tc.input), err)
		}
		if !tc.hasError && result != tc.expected {
			t.Errorf("For input %s, expected %f, got %f", string(tc.input), tc.expected, result)
		}
	}
}

func TestToUTF8String(t *testing.T) {
	input := []byte("test")
	converter := NewDefaultConverter(charmap.Windows1252)
	result, err := toUTF8String(input, converter)
	if err != nil {
		t.Errorf("Unexpected error with converter: %v", err)
	}
	if result != "test" {
		t.Errorf("Expected 'test', got '%s'", result)
	}

	input = []byte{0xE4} // 'ä' in Windows1252
	result, err = toUTF8String(input, converter)
	if err != nil {
		t.Errorf("Unexpected error with special character: %v", err)
	}
	if len(result) == 0 {
		t.Errorf("Expected non-empty result for special character")
	}
}

func TestFromUtf8String(t *testing.T) {
	input := []byte("test")
	converter := NewDefaultConverter(charmap.Windows1252)
	result, err := fromUtf8String(input, converter)
	if err != nil {
		t.Errorf("Unexpected error with converter: %v", err)
	}
	if string(result) != "test" {
		t.Errorf("Expected 'test', got '%s'", string(result))
	}

	input = []byte("café") // Contains UTF-8 encoded characters
	result, err = fromUtf8String(input, converter)
	if err != nil {
		t.Errorf("Unexpected error with UTF-8 input: %v", err)
	}
	if len(result) == 0 {
		t.Errorf("Expected non-empty result for UTF-8 input")
	}
}

func TestToBinary(t *testing.T) {
	testCases := []struct {
		input    interface{}
		expected []byte
		hasError bool
	}{
		{int32(1234), []byte{0xD2, 0x04, 0x00, 0x00}, false}, // Little endian
		{uint32(1234), []byte{0xD2, 0x04, 0x00, 0x00}, false},
		{int64(1234), []byte{0xD2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, false},
		{float64(1.5), nil, false}, // 8 bytes for float64
		{"unsupported", nil, true},
	}

	for _, tc := range testCases {
		result, err := toBinary(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("Expected error for input %v, got none", tc.input)
		}
		if !tc.hasError && err != nil {
			t.Errorf("Unexpected error for input %v: %v", tc.input, err)
		}
		if !tc.hasError && tc.expected != nil {
			if len(result) != len(tc.expected) {
				t.Errorf("For input %v, expected length %d, got %d", tc.input, len(tc.expected), len(result))
			} else {
				for i, b := range tc.expected {
					if result[i] != b {
						t.Errorf("For input %v, expected byte %d at position %d, got %d", tc.input, b, i, result[i])
						break
					}
				}
			}
		}
	}
}

func TestPrependSpaces(t *testing.T) {
	testCases := []struct {
		input    []byte
		length   int
		expected []byte
	}{
		{[]byte("test"), 8, []byte("    test")},
		{[]byte("test"), 4, []byte("test")},
		{[]byte(""), 3, []byte("   ")},
		{[]byte("hello"), 2, []byte("hello")}, // Should not truncate
	}

	for _, tc := range testCases {
		result := prependSpaces(tc.input, tc.length)
		if string(result) != string(tc.expected) {
			t.Errorf("prependSpaces(%s, %d) = %s, expected %s",
				string(tc.input), tc.length, string(result), string(tc.expected))
		}
	}
}

func TestPrependBytes(t *testing.T) {
	testCases := []struct {
		input    []byte
		length   int
		value    byte
		expected []byte
	}{
		{[]byte("test"), 8, 'X', []byte("XXXXtest")},
		{[]byte("test"), 4, 'X', []byte("test")},
		{[]byte(""), 3, 'A', []byte("AAA")},
		{[]byte("hello"), 3, 'Z', []byte("hello")}, // Should not truncate
	}

	for _, tc := range testCases {
		result := prependBytes(tc.input, tc.length, tc.value)
		if string(result) != string(tc.expected) {
			t.Errorf("prependBytes(%s, %d, %c) = %s, expected %s",
				string(tc.input), tc.length, tc.value, string(result), string(tc.expected))
		}
	}
}

func TestSetStructField(t *testing.T) {
	type TestStruct struct {
		Name string
		Age  int
	}

	ts := &TestStruct{}
	tags := map[string]string{"NAME": "Name", "AGE": "Age"}

	err := setStructField(tags, ts, "Name", "John Doe")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if ts.Name != "John Doe" {
		t.Errorf("Expected 'John Doe', got %s", ts.Name)
	}

	err = setStructField(tags, ts, "Age", 30)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if ts.Age != 30 {
		t.Errorf("Expected 30, got %d", ts.Age)
	}
}

func TestGetStructTags(t *testing.T) {
	type TaggedStruct struct {
		Name string `dbase:"name_field"`
		Age  int    `dbase:"age_field"`
	}

	ts := &TaggedStruct{}
	tags := getStructTags(ts)

	if _, exists := tags["NAME_FIELD"]; !exists {
		t.Error("Expected tags to contain 'NAME_FIELD'")
	}
	if _, exists := tags["AGE_FIELD"]; !exists {
		t.Error("Expected tags to contain 'AGE_FIELD'")
	}
	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(tags))
	}
}

func TestExtractTags(t *testing.T) {
	type TaggedStruct struct {
		Name string `dbase:"name_field"`
		Age  int    `dbase:"age_field"`
	}

	ts := TaggedStruct{}
	tags := make(map[string]string)

	extractTags(reflect.ValueOf(ts), tags)

	if _, exists := tags["NAME_FIELD"]; !exists {
		t.Error("Expected tags to contain 'NAME_FIELD'")
	}
	if _, exists := tags["AGE_FIELD"]; !exists {
		t.Error("Expected tags to contain 'AGE_FIELD'")
	}
	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(tags))
	}
}

func TestCast(t *testing.T) {
	result := cast(42, reflect.TypeOf(42))
	if result != 42 {
		t.Errorf("Expected 42, got %v", result)
	}

	result = cast(int32(42), reflect.TypeOf(int64(0)))
	if result != int64(42) {
		t.Errorf("Expected int64(42), got %v", result)
	}

	result = cast(nil, reflect.TypeOf(42))
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}

	result = cast("string", reflect.TypeOf(42))
	if result != "string" {
		t.Errorf("Expected 'string', got %v", result)
	}
}
