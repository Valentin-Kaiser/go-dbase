package dbase

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"golang.org/x/text/encoding/charmap"
)

func TestJulianDate(t *testing.T) {
	tests := []struct {
		year, month, day int
		expected         int
	}{
		{2023, 8, 16, 2460173},
		{2000, 1, 1, 2451545},
		{1970, 1, 1, 2440588},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Year:%d, Month:%d, Day:%d", tt.year, tt.month, tt.day), func(t *testing.T) {
			got := julianDate(tt.year, tt.month, tt.day)
			if got != tt.expected {
				t.Errorf("got %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestJulianToDate(t *testing.T) {
	tests := []struct {
		julianDate       int
		year, month, day int
	}{
		{2460173, 2023, 8, 16},
		{2451545, 2000, 1, 1},
		{2440588, 1970, 1, 1},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("JulianDate:%d", tt.julianDate), func(t *testing.T) {
			y, m, d := julianToDate(tt.julianDate)
			if y != tt.year || m != tt.month || d != tt.day {
				t.Errorf("got Year:%d Month:%d Day:%d, want Year:%d Month:%d Day:%d", y, m, d, tt.year, tt.month, tt.day)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		raw      []byte
		expected time.Time
		hasError bool
	}{
		{[]byte("20230816"), time.Date(2023, 8, 16, 0, 0, 0, 0, time.UTC), false},
		{[]byte("20000101"), time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{[]byte(""), time.Time{}, false},
		{[]byte("invalid"), time.Time{}, true},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Raw:%s", tt.raw), func(t *testing.T) {
			got, err := parseDate(tt.raw)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}
func TestParseDateTime(t *testing.T) {
	tests := []struct {
		raw      []byte
		expected time.Time
	}{
		// Use known valid byte slices and their corresponding date-time values.
		// The example below is just a placeholder; actual byte slice values will need to be provided based on the specific implementation of the function.
		{[]byte{0x0D, 0x8A, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00}, time.Date(2023, 8, 16, 0, 0, 0, 0, time.UTC)},
		{[]byte("invalidLength"), time.Time{}},
		{[]byte(""), time.Time{}},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Raw:%s", tt.raw), func(t *testing.T) {
			got := parseDateTime(tt.raw)
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseNumericInt(t *testing.T) {
	tests := []struct {
		raw      []byte
		expected int64
		hasError bool
	}{
		{[]byte("123456"), 123456, false},
		{[]byte("0"), 0, false},
		{[]byte("-123456"), -123456, false},
		{[]byte(""), 0, false},
		{[]byte("     "), 0, false},
		{[]byte("invalid"), 0, true},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Raw:%s", tt.raw), func(t *testing.T) {
			got, err := parseNumericInt(tt.raw)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if got != tt.expected {
				t.Errorf("got %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		raw      []byte
		expected float64
		hasError bool
	}{
		{[]byte("123.456"), 123.456, false},
		{[]byte("0.0"), 0.0, false},
		{[]byte("-123.456"), -123.456, false},
		{[]byte("invalid"), 0.0, true},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Raw:%s", tt.raw), func(t *testing.T) {
			got, err := parseFloat(tt.raw)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if got != tt.expected {
				t.Errorf("got %f, want %f", got, tt.expected)
			}
		})
	}
}

func TestToUTF8String(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte("test"), "test"},
		{[]byte{0x8A}, "Š"},
		{[]byte{0xE4}, "ä"},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%s", tt.input), func(t *testing.T) {
			got, err := toUTF8String(tt.input, NewDefaultConverter(charmap.Windows1250))
			if err != nil {
				t.Errorf("got error %v", err)
			}
			if got != tt.expected {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestFromUTF8String(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte("test"), []byte("test")},
		{[]byte("Š"), []byte{0x8A}},
		{[]byte("ä"), []byte{0xE4}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%s", tt.input), func(t *testing.T) {
			got, err := fromUTF8String(tt.input, NewDefaultConverter(charmap.Windows1250))
			if err != nil {
				t.Errorf("got error %v", err)
			}
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestToBinary(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected []byte
	}{
		{int32(123), []byte{0x7B, 0x00, 0x00, 0x00}},
		{int64(123), []byte{0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{float32(123.456), []byte{0x79, 0xe9, 0xf6, 0x42}},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%v", tt.input), func(t *testing.T) {
			got, err := toBinary(tt.input)
			if err != nil {
				t.Errorf("got error %v", err)
			}

			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestAppendSpaces(t *testing.T) {
	tests := []struct {
		input    []byte
		length   int
		expected []byte
	}{
		{[]byte("test"), 8, []byte("test    ")},
		{[]byte("test"), 4, []byte("test")},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%s, Length:%d", tt.input, tt.length), func(t *testing.T) {
			got := appendSpaces(tt.input, tt.length)
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestPrependSpaces(t *testing.T) {
	tests := []struct {
		input    []byte
		length   int
		expected []byte
	}{
		{[]byte("test"), 8, []byte("    test")},
		{[]byte("test"), 4, []byte("test")},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%s, Length:%d", tt.input, tt.length), func(t *testing.T) {
			got := prependSpaces(tt.input, tt.length)
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00, 0x74, 0x65, 0x73, 0x74}, []byte{0x74, 0x65, 0x73, 0x74}},
		{[]byte{0x00, 0x74, 0x65, 0x73, 0x74, 0x00}, []byte{0x74, 0x65, 0x73, 0x74}},
		{[]byte{0x00, 0x00, 0x00, 0x00}, []byte{}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Input:%s", tt.input), func(t *testing.T) {
			got := sanitizeString(tt.input)
			if !bytes.Equal(got, tt.expected) {
				t.Errorf("got %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestGetNthBit(t *testing.T) {
	tests := []struct {
		bytes    []byte
		n        int
		expected bool
	}{
		{[]byte{0x01}, 0, true},
		{[]byte{0x02}, 1, true},
		{[]byte{0x00}, 0, false},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Bytes:%v, Bit:%d", tt.bytes, tt.n), func(t *testing.T) {
			got := getNthBit(tt.bytes, tt.n)
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSetNthBit(t *testing.T) {
	tests := []struct {
		byteVal  byte
		n        int
		expected byte
	}{
		{0x00, 0, 0x01},
		{0x02, 0, 0x03},
		{0x00, 1, 0x02},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Byte:%v, Bit:%d", tt.byteVal, tt.n), func(t *testing.T) {
			got := setNthBit(tt.byteVal, tt.n)
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSetStructField(t *testing.T) {
	type SampleStruct struct {
		Name  string `dbase:"name"`
		Value int    `dbase:"value"`
	}

	tests := []struct {
		name     string
		value    interface{}
		expected SampleStruct
		hasError bool
	}{
		{"name", "TestName", SampleStruct{Name: "TestName"}, false},
		{"value", 123, SampleStruct{Value: 123}, false},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Name:%s, Value:%v", tt.name, tt.value), func(t *testing.T) {
			var s SampleStruct
			err := setStructField(structTags(&s), &s, tt.name, tt.value)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}
			if s != tt.expected {
				t.Errorf("got %v, want %v", s, tt.expected)
			}
		})
	}
}

func TestStructTags(t *testing.T) {
	type SampleStruct struct {
		Name  string `dbase:"name"`
		Value int    `dbase:"value"`
	}

	expected := map[string]string{
		"name":  "Name",
		"value": "Value",
	}

	got := structTags(&SampleStruct{})
	for k, v := range expected {
		if got[k] != v {
			t.Errorf("for key %s, got %s, want %s", k, got[k], v)
		}
	}
}

func TestCast(t *testing.T) {
	tests := []struct {
		value    interface{}
		toType   reflect.Type
		expected interface{}
	}{
		{int(123), reflect.TypeOf(uint8(0)), uint8(123)},
		{int(123), reflect.TypeOf(uint16(0)), uint16(123)},
		{int(123), reflect.TypeOf(uint32(0)), uint32(123)},
		{int(123), reflect.TypeOf(uint64(0)), uint64(123)},
		{uint(123), reflect.TypeOf(int8(0)), int8(123)},
		{uint(123), reflect.TypeOf(int16(0)), int16(123)},
		{uint(123), reflect.TypeOf(int32(0)), int32(123)},
		{uint(123), reflect.TypeOf(int64(0)), int64(123)},
		{uint(123), reflect.TypeOf(float32(0)), float32(123)},
		{uint(123), reflect.TypeOf(float64(0)), float64(123)},
		{float32(123), reflect.TypeOf(uint8(0)), uint8(123)},
		{float32(123), reflect.TypeOf(uint16(0)), uint16(123)},
		{float32(123), reflect.TypeOf(uint32(0)), uint32(123)},
		{float32(123), reflect.TypeOf(uint64(0)), uint64(123)},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Value:%v, Type:%s", tt.value, tt.toType), func(t *testing.T) {
			got := cast(tt.value, tt.toType)
			if got != tt.expected {
				t.Errorf("got %v of type %v, want %v of type %s", got, reflect.TypeOf(got), tt.expected, tt.toType)
			}
		})
	}
}
