package dbase

import (
	"testing"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func TestDefaultConverter_Decode(t *testing.T) {
	converter := NewDefaultConverter(charmap.Windows1252)

	// Test valid UTF-8 input (should return as-is)
	utf8Input := []byte("Hello, World!")
	result, err := converter.Decode(utf8Input)
	if err != nil {
		t.Errorf("Unexpected error decoding UTF-8: %v", err)
	}
	if string(result) != "Hello, World!" {
		t.Errorf("Expected 'Hello, World!', got '%s'", string(result))
	}

	// Test Windows-1252 encoded input
	// Create a byte slice with Windows-1252 encoded characters
	cp1252Input := []byte{0xC0, 0xE9, 0xFC} // À é ü in Windows-1252
	result, err = converter.Decode(cp1252Input)
	if err != nil {
		t.Errorf("Unexpected error decoding Windows-1252: %v", err)
	}
	if !utf8.Valid(result) {
		t.Error("Result is not valid UTF-8")
	}
}

func TestDefaultConverter_Encode(t *testing.T) {
	converter := NewDefaultConverter(charmap.Windows1252)

	// Test encoding UTF-8 to Windows-1252
	utf8Input := []byte("Hello")
	result, err := converter.Encode(utf8Input)
	if err != nil {
		t.Errorf("Unexpected error encoding: %v", err)
	}
	if len(result) == 0 {
		t.Error("Encoded result is empty")
	}
}

func TestDefaultConverter_CodePage(t *testing.T) {
	testCases := []struct {
		encoding         encoding.Encoding
		expectedCodePage byte
	}{
		{charmap.CodePage437, 0x01},
		{charmap.CodePage850, 0x02},
		{charmap.CodePage852, 0x64},
		{charmap.CodePage865, 0x66},
		{charmap.CodePage866, 0x65},
		{charmap.Windows874, 0x7C},
		{charmap.Windows1250, 0xc8},
		{charmap.Windows1251, 0xc9},
		{charmap.Windows1252, 0x03},
		{charmap.Windows1253, 0xCB},
		{charmap.Windows1254, 0xCA},
		{charmap.Windows1255, 0x7D},
		{charmap.Windows1256, 0x7E},
		{simplifiedchinese.GBK, 0x7A},
	}

	for _, tc := range testCases {
		converter := NewDefaultConverter(tc.encoding)
		result := converter.CodePage()
		if result != tc.expectedCodePage {
			t.Errorf("Expected code page %02x for encoding, got %02x", tc.expectedCodePage, result)
		}
	}
}

func TestDefaultConverter_UnknownEncoding(t *testing.T) {
	// Test with an encoding that doesn't have a code page mapping
	converter := NewDefaultConverter(charmap.ISO8859_1)
	result := converter.CodePage()
	if result != 0x00 {
		t.Errorf("Expected code page 0x00 for unknown encoding, got %02x", result)
	}
}

func TestConverterFromCodePage(t *testing.T) {
	testCases := []struct {
		codePage byte
		encoding encoding.Encoding
	}{
		{0x01, charmap.CodePage437},
		{0x02, charmap.CodePage850},
		{0x64, charmap.CodePage852},
		{0x66, charmap.CodePage865},
		{0x65, charmap.CodePage866},
		{0x7C, charmap.Windows874},
		{0xc8, charmap.Windows1250},
		{0xc9, charmap.Windows1251},
		{0x03, charmap.Windows1252},
		{0xCB, charmap.Windows1253},
		{0xCA, charmap.Windows1254},
		{0x7D, charmap.Windows1255},
		{0x7E, charmap.Windows1256},
		{0x7A, simplifiedchinese.GBK},
	}

	for _, tc := range testCases {
		converter := ConverterFromCodePage(tc.codePage)
		if converter.encoding != tc.encoding {
			t.Errorf("Expected encoding %v for code page %02x, got %v", tc.encoding, tc.codePage, converter.encoding)
		}
	}
}

func TestConverterFromCodePage_Unknown(t *testing.T) {
	// Test with unknown code page
	converter := ConverterFromCodePage(0xFF)
	if converter.encoding != charmap.Windows1250 {
		t.Errorf("Expected Windows1250 for unknown code page, got %v", converter.encoding)
	}
}

func TestRegisterCustomEncoding(t *testing.T) {
	// Test registering a custom encoding
	customCodePage := byte(0xAA)
	customEncoding := charmap.ISO8859_1

	RegisterCustomEncoding(customCodePage, customEncoding)

	// Test that the custom encoding is returned
	converter := ConverterFromCodePage(customCodePage)
	if converter.encoding != customEncoding {
		t.Errorf("Expected custom encoding %v, got %v", customEncoding, converter.encoding)
	}

	// Test that CodePage() returns the custom code page
	converter = NewDefaultConverter(customEncoding)
	result := converter.CodePage()
	if result != customCodePage {
		t.Errorf("Expected custom code page %02x, got %02x", customCodePage, result)
	}
}

func TestNewDefaultConverter(t *testing.T) {
	encoding := charmap.Windows1252
	converter := NewDefaultConverter(encoding)

	if converter.encoding != encoding {
		t.Errorf("Expected encoding %v, got %v", encoding, converter.encoding)
	}
}

func TestEncodingConverter_Interface(t *testing.T) {
	// Verify that DefaultConverter implements EncodingConverter interface
	var _ EncodingConverter = DefaultConverter{}
}
