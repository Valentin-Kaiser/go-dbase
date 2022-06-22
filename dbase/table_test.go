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
	header := dBaseFile.ColumnsCount()
	headerCalc := dBaseFile.Header().ColumnsCount()
	if header != headerCalc {
		t.Errorf("[TEST] ColumnsCount failed #1 - Error: ColumnsCount not equal. DBF ColumnsCount: %d, DBF Header ColumnsCount: %d", header, headerCalc)
	}
}

// Tests if column headers have been parsed, fails if there are no columns
func TestColumnNames(t *testing.T) {
	columnnames := dBaseFile.ColumnNames()
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
	want := "{ColumnName:[73 68 0 0 0 0 0 0 0 0 0] DataType:73 Position:1 Length:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", dBaseFile.table.columns[0])
	if have != want {
		t.Errorf("[TEST] OpenStream failed #1 - Error: First column from header does not match signature >> Want %s, have %s", want, have)
	}
}

func TestColumnSlice(t *testing.T) {

}
