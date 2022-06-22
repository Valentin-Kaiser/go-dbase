package dbase

import (
	"fmt"
	"testing"
)

func TestColumnHeader(t *testing.T) {
	want := "{ColumnName:[73 68 0 0 0 0 0 0 0 0 0] DataType:73 Position:1 Length:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", dBaseFile.table.columns[0])
	if have != want {
		t.Errorf("[TEST] OpenStream failed #1 - Error: First column from header does not match signature >> Want %s, have %s", want, have)
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

func TestColumnsCount(t *testing.T) {
	header := dBaseFile.ColumnsCount()
	headerCalc := dBaseFile.Header().ColumnsCount()
	if header != headerCalc {
		t.Errorf("[TEST] ColumnsCount failed #1 - Error: ColumnsCount not equal. DBF ColumnsCount: %d, DBF Header ColumnsCount: %d", header, headerCalc)
	}
}

func TestColumnPos(t *testing.T) {

}

func TestColumn(t *testing.T) {

}
