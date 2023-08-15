package dbase

import (
	"bytes"
	"unicode/utf8"

	"io"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// EncodingConverter is the interface as passed to Open
type EncodingConverter interface {
	Decode(in []byte) ([]byte, error)
	Encode(in []byte) ([]byte, error)
	CodePage() byte
}

type DefaultConverter struct {
	encoding *charmap.Charmap
}

// Decode decodes a specified encoding to byte slice to a UTF8 byte slice
func (c DefaultConverter) Decode(in []byte) ([]byte, error) {
	if utf8.Valid(in) {
		return in, nil
	}
	r := transform.NewReader(bytes.NewReader(in), c.encoding.NewDecoder())
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, newError("dbase-encoding-decode-1", err)
	}
	return data, nil
}

// Decode decodes a UTF8 byte slice to the specified encoding byte slice
func (c DefaultConverter) Encode(in []byte) ([]byte, error) {
	out := make([]byte, len(in))
	enc := c.encoding.NewEncoder()
	nDst, _, err := enc.Transform(out, in, false)
	if err != nil {
		return nil, newError("dbase-encoding-encode-1", err)
	}
	return out[:nDst], nil
}

// CodePageMark returns corresponding code page mark for the encoding
func (c DefaultConverter) CodePage() byte {
	switch c.encoding {
	case charmap.CodePage437: // U.S. MS-DOS
		return 0x01
	case charmap.CodePage850: // International MS-DOS
		return 0x02
	case charmap.CodePage852: // Eastern European MS-DOS
		return 0x64
	case charmap.CodePage865: // Nordic MS-DOS
		return 0x66
	case charmap.CodePage866: // Russian MS-DOS
		return 0x65
	case charmap.Windows874: // Thai Windows
		return 0x7C
	case charmap.Windows1250: // Central European Windows
		return 0xc8
	case charmap.Windows1251: // Russian Windows
		return 0xc9
	case charmap.Windows1252: // Windows ANSI
		return 0x03
	case charmap.Windows1253: // Greek Windows
		return 0xCB
	case charmap.Windows1254: // Turkish Windows
		return 0xCA
	case charmap.Windows1255: // Hebrew Windows
		return 0x7D
	case charmap.Windows1256: // Arabic Windows
		return 0x7E
	default:
		return 0x00
	}
}

func NewDefaultConverter(encoding *charmap.Charmap) DefaultConverter {
	return DefaultConverter{encoding: encoding}
}

// NewDefaultConverterFromCodePage returns a new EncodingConverter from a code page mark
func ConverterFromCodePage(codePageMark byte) DefaultConverter {
	switch codePageMark {
	case 0x01: // U.S. MS-DOS
		return NewDefaultConverter(charmap.CodePage437)
	case 0x02: // International MS-DOS
		return NewDefaultConverter(charmap.CodePage850)
	case 0x64: // Eastern European MS-DOS
		return NewDefaultConverter(charmap.CodePage852)
	case 0x66: // Nordic MS-DOS
		return NewDefaultConverter(charmap.CodePage865)
	case 0x65: // Russian MS-DOS
		return NewDefaultConverter(charmap.CodePage866)
	case 0x7C: // Thai Windows
		return NewDefaultConverter(charmap.Windows874)
	case 0xC8: // Central European Windows
		return NewDefaultConverter(charmap.Windows1250)
	case 0xC9: // Russian Windows
		return NewDefaultConverter(charmap.Windows1251)
	case 0x03: // Windows ANSI
		return NewDefaultConverter(charmap.Windows1252)
	case 0xCB: // Greek Windows
		return NewDefaultConverter(charmap.Windows1253)
	case 0xCA: // Turkish Windows
		return NewDefaultConverter(charmap.Windows1254)
	case 0x7D: // Hebrew Windows
		return NewDefaultConverter(charmap.Windows1255)
	case 0x7E: // Arabic Windows
		return NewDefaultConverter(charmap.Windows1256)
	default: // Default to Central European Windows
		return NewDefaultConverter(charmap.Windows1250)
	}
}
