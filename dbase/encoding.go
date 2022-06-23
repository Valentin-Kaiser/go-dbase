package dbase

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// Converter is the interface as passed to Open
type EncodingConverter interface {
	Decode(in []byte) ([]byte, error)
	Encode(in []byte) ([]byte, error)
}

// Win1250Decoder translates a Windows-1250 DBF to UTF8 and back
type Win1250Converter struct{}

// Decode decodes a Windows1250 byte slice to a UTF8 byte slice
func (d *Win1250Converter) Decode(in []byte) ([]byte, error) {
	if utf8.Valid(in) {
		return in, nil
	}
	r := transform.NewReader(bytes.NewReader(in), charmap.Windows1250.NewDecoder())
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("dbase-encoding-decode-1:FAILED:%v", err)
	}
	return data, nil
}

// Decode decodes a UTF8 byte slice to a Windows1250 byte slice
func (d *Win1250Converter) Encode(in []byte) ([]byte, error) {
	out := make([]byte, len(in))
	enc := charmap.Windows1250.NewEncoder()
	nDst, _, err := enc.Transform(out, in, false)
	if err != nil {
		return nil, fmt.Errorf("dbase-encoding-encode-1:FAILED:%v", err)
	}

	return out[:nDst], nil
}
