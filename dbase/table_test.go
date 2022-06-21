package dbase

import (
	"fmt"
	"testing"
)

func TestFieldHeader(t *testing.T) {
	want := "{Name:[73 68 0 0 0 0 0 0 0 0 0] Type:73 Position:1 Length:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", dBaseFile.table.fields[0])
	if have != want {
		t.Errorf("[TEST] OpenStream failed #1 - Error: First field from header does not match signature >> Want %s, have %s", want, have)
	}
}

// Tests if field headers have been parsed, fails if there are no fields
func TestFieldNames(t *testing.T) {
	fieldnames := dBaseFile.FieldNames()
	want := 13
	if len(fieldnames) != want {
		t.Errorf("[TEST] FieldNames failed #1 - Error: Expected %d fields, have %d", want, len(fieldnames))
	}
}

func TestFieldsCount(t *testing.T) {
	header := dBaseFile.FieldsCount()
	headerCalc := dBaseFile.Header().FieldsCount()
	if header != headerCalc {
		t.Errorf("[TEST] FieldsCount failed #1 - Error: FieldsCount not equal. DBF FieldsCount: %d, DBF Header FieldsCount: %d", header, headerCalc)
	}
}

func TestFieldPos(t *testing.T) {

}

func TestField(t *testing.T) {

}
