package dbase

import (
	"path/filepath"
	"strings"
	"testing"
)

var testFilePath = filepath.Join("../test_data", "TEST")
var dBaseFile *DBF
var dBaseStream *DBF

func TestOpen(t *testing.T) {
	var err error
	dBaseFile, err = Open(testFilePath+".DBF", new(Win1250Converter))
	if err != nil {
		t.Fatalf("[TEST] OpenFile failed #1 - Error: %v", err.Error())
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

func TestPrepareDBF(t *testing.T) {

}

func TestReadDBFHeader(t *testing.T) {

}

func TestReadColumn(t *testing.T) {

}

func TestReadColumnInfos(t *testing.T) {

}

func TestReadMemo(t *testing.T) {

}

func TestValidateFileVersion(t *testing.T) {

}

// Close file handles
func TestClose(t *testing.T) {
	err := dBaseFile.Close()
	if err != nil {
		t.Fatal(err)
	}
}
