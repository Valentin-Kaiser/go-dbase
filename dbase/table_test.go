package dbase

import (
	"fmt"
	"testing"
)

func TestModified(t *testing.T) {

}

func TestFileSize(t *testing.T) {

}

func TestEOF(t *testing.T) {

}

func TestBOF(t *testing.T) {

}

func TestHeader(t *testing.T) {

}

func TestRowsCount(t *testing.T) {

}

func TestColumns(t *testing.T) {

}

func TestColumnsCount(t *testing.T) {
	header := dbf.ColumnsCount()
	headerCalc := dbf.Header().ColumnsCount()
	if header != headerCalc {
		t.Errorf("[TEST] ColumnsCount failed #1 - Error: ColumnsCount not equal. DBF ColumnsCount: %d, DBF Header ColumnsCount: %d", header, headerCalc)
	}
}

// Tests if column headers have been parsed, fails if there are no columns
func TestColumnNames(t *testing.T) {
	columnnames := dbf.ColumnNames()
	want := 13
	if len(columnnames) != want {
		t.Errorf("[TEST] ColumnNames failed #1 - Error: Expected %d columns, have %d", want, len(columnnames))
	}
}

func TestColumnPos(t *testing.T) {

}

func TestValue(t *testing.T) {

}

func TestName(t *testing.T) {

}

func TestType(t *testing.T) {

}

func TestReadRow(t *testing.T) {

}

func TestBytesToRow(t *testing.T) {
	var err error
	dbf, err = Open(testFilePath+".DBF", new(Win1250Converter))
	if err != nil {
		t.Fatalf("[TEST] OpenFile failed #1 - Error: %v", err.Error())
	}

	want := "TEST_COMPANY_1                          "
	have, err := dbf.BytesToRow([]byte{0x20, 0x01, 0x00, 0x00, 0x00, 0x30, 0x32, 0x30, 0x32, 0x30, 0x30, 0x31, 0x30, 0x33, 0x31, 0x35, 0x3a, 0x30, 0x30, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x33, 0x7b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x54, 0x45, 0x53, 0x54, 0x5f, 0x43, 0x4f, 0x4d, 0x50, 0x41, 0x4e, 0x59, 0x5f, 0x31, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73, 0x20, 0x31, 0x31, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x08, 0x00, 0x00, 0x00, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31, 0x2e, 0x36, 0x36, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31, 0x46})
	if err != nil {
		t.Errorf("ERROR: %v", err.Error())
	}
	if want != fmt.Sprintf("%+v", have.Data[7]) {
		t.Errorf("Want %v, have \"%v\"", want, have.Data[7])
	}
}

func TestRows(t *testing.T) {

}

func TestRow(t *testing.T) {

}

func TestRowToMap(t *testing.T) {

}

func TestRowsToJSON(t *testing.T) {

}

func TestRowsToStruct(t *testing.T) {

}

func TestToMap(t *testing.T) {

}

func TestToJSON(t *testing.T) {

}

func TestToStruct(t *testing.T) {

}

func TestColumn(t *testing.T) {
	want := "&{ColumnName:[73 68 0 0 0 0 0 0 0 0 0] DataType:73 Position:1 Length:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", dbf.table.columns[0])
	if have != want {
		t.Errorf("[TEST] OpenStream failed #1 - Error: First column from header does not match signature >> Want %s, have %s", want, have)
	}
}

func TestColumnSlice(t *testing.T) {

}
