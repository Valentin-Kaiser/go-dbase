package dbase

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

var testFilePath = filepath.Join("test_data", "TEST")
var dBaseFile *DBF
var dBaseStream *DBF

func TestOpenFile(t *testing.T) {
	var err error
	dBaseFile, err = OpenFile(testFilePath+".DBF", new(Win1250Decoder))
	if err != nil {
		t.Fatalf("[TEST] OpenFile failed #1 - Error: %v", err.Error())
	}
}

func TestFieldHeader(t *testing.T) {
	want := "{Name:[73 68 0 0 0 0 0 0 0 0 0] Type:73 Position:1 Length:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", dBaseFile.fields[0])
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

func TestGoTo(t *testing.T) {
	err := dBaseFile.GoTo(0)
	if err != nil {
		t.Errorf("[TEST] GoTo failed #1 - Error: %v", err)
	}
	if !dBaseFile.BOF() {
		t.Error("[TEST] GoTo failed #2 - Error: Expected to be at BOF")
	}
	err = dBaseFile.GoTo(1)
	if err != nil {
		t.Errorf("[TEST] GoTo failed #3 - Error: %v", err)
	}
	if dBaseFile.EOF() {
		t.Error("[TEST] GoTo failed #4 - Error: Did not expect to be at EOF")
	}
	err = dBaseFile.GoTo(4)
	if err != nil {
		if DBaseError(err.Error()) != ERROR_EOF {
			t.Errorf("[TEST] GoTo failed #5 - Error: %v", err)
		}
	}
	if !dBaseFile.EOF() {
		t.Error("[TEST] GoTo failed #6 - Error: Expected to be at EOF")
	}
}

func TestSkip(t *testing.T) {
	dBaseFile.GoTo(0)

	err := dBaseFile.Skip(1)
	if err != nil {
		t.Errorf("[TEST] Skip failed #1 - Error: %v", err)
	}
	if dBaseFile.EOF() {
		t.Error("[TEST] Skip failed #2 - Error: Did not expect to be at EOF")
	}
	err = dBaseFile.Skip(3)
	if err != nil {
		if DBaseError(strings.Split(err.Error(), ":")[len(strings.Split(err.Error(), ":"))-1]) != ERROR_EOF {
			t.Errorf("[TEST] Skip failed #3- Error: %v", err)
		}
	}
	if !dBaseFile.EOF() {
		t.Error("[TEST] Skip failed #4 - Error: Expected to be at EOF")
	}
	err = dBaseFile.Skip(-20)
	if err != nil {
		if DBaseError(strings.Split(err.Error(), ":")[len(strings.Split(err.Error(), ":"))-1]) != ERROR_BOF {
			t.Errorf("[TEST] Skip failed #5 - Error: %v", err)
		}
	}
	if !dBaseFile.BOF() {
		t.Error("[TEST] Skip failed #6 - Error: Expected to be at BOF")
	}
}

func TestFieldPos(t *testing.T) {

}

func TestRecord(t *testing.T) {

}

func TestField(t *testing.T) {

}

func TestRecordToJson(t *testing.T) {

}

// Close file handles
func TestClose(t *testing.T) {
	err := dBaseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
}
