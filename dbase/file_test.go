package dbase

import (
	"testing"
)

// Helper function to create a column with a name
func createColumn(name string) *Column {
	col := &Column{}
	copy(col.FieldName[:], name)
	return col
}

func TestFile_TableName(t *testing.T) {
	// Create a mock file structure
	file := &File{
		table: &Table{
			name: "test_table",
		},
	}

	result := file.TableName()
	if result != "test_table" {
		t.Errorf("Expected 'test_table', got '%s'", result)
	}
}

func TestFile_EOF(t *testing.T) {
	// Test EOF when row pointer equals rows count
	file := &File{
		header: &Header{
			RowsCount: 10,
		},
		table: &Table{
			rowPointer: 10,
		},
	}

	if !file.EOF() {
		t.Error("Expected EOF to be true when rowPointer >= RowsCount")
	}

	// Test EOF when row pointer is less than rows count
	file.table.rowPointer = 5
	if file.EOF() {
		t.Error("Expected EOF to be false when rowPointer < RowsCount")
	}
}

func TestFile_BOF(t *testing.T) {
	// Test BOF when row pointer is 0
	file := &File{
		table: &Table{
			rowPointer: 0,
		},
	}

	if !file.BOF() {
		t.Error("Expected BOF to be true when rowPointer is 0")
	}

	// Test BOF when row pointer is greater than 0
	file.table.rowPointer = 1
	if file.BOF() {
		t.Error("Expected BOF to be false when rowPointer > 0")
	}
}

func TestFile_Pointer(t *testing.T) {
	file := &File{
		table: &Table{
			rowPointer: 42,
		},
	}

	result := file.Pointer()
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

func TestFile_Header(t *testing.T) {
	header := &Header{
		RowsCount: 100,
	}
	file := &File{
		header: header,
	}

	result := file.Header()
	if result != header {
		t.Error("Header() should return the same header instance")
	}
}

func TestFile_RowsCount(t *testing.T) {
	file := &File{
		header: &Header{
			RowsCount: 25,
		},
	}

	result := file.RowsCount()
	if result != 25 {
		t.Errorf("Expected 25, got %d", result)
	}
}

func TestFile_Columns(t *testing.T) {
	col1 := createColumn("col1")
	col2 := createColumn("col2")

	columns := []*Column{col1, col2}
	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	result := file.Columns()
	if len(result) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result))
	}
	if result[0].Name() != "col1" || result[1].Name() != "col2" {
		t.Error("Columns not returned correctly")
	}
}

func TestFile_Column(t *testing.T) {
	col1 := createColumn("col1")
	col2 := createColumn("col2")

	columns := []*Column{col1, col2}
	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	// Test valid position
	result := file.Column(0)
	if result == nil || result.Name() != "col1" {
		t.Error("Expected to get first column")
	}

	result = file.Column(1)
	if result == nil || result.Name() != "col2" {
		t.Error("Expected to get second column")
	}

	// Test invalid positions
	result = file.Column(-1)
	if result != nil {
		t.Error("Expected nil for negative position")
	}

	result = file.Column(2)
	if result != nil {
		t.Error("Expected nil for position beyond range")
	}
}

func TestFile_ColumnsCount(t *testing.T) {
	columns := []*Column{
		createColumn("col1"),
		createColumn("col2"),
		createColumn("col3"),
	}
	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	result := file.ColumnsCount()
	if result != 3 {
		t.Errorf("Expected 3, got %d", result)
	}
}

func TestFile_ColumnNames(t *testing.T) {
	columns := []*Column{
		createColumn("first"),
		createColumn("second"),
		createColumn("third"),
	}
	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	result := file.ColumnNames()
	expected := []string{"first", "second", "third"}

	if len(result) != len(expected) {
		t.Errorf("Expected %d names, got %d", len(expected), len(result))
	}

	for i, name := range expected {
		if result[i] != name {
			t.Errorf("Expected '%s' at position %d, got '%s'", name, i, result[i])
		}
	}
}

func TestFile_ColumnPosByName(t *testing.T) {
	columns := []*Column{
		createColumn("first"),
		createColumn("second"),
		createColumn("third"),
	}
	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	// Test existing column names
	pos := file.ColumnPosByName("first")
	if pos != 0 {
		t.Errorf("Expected position 0 for 'first', got %d", pos)
	}

	pos = file.ColumnPosByName("second")
	if pos != 1 {
		t.Errorf("Expected position 1 for 'second', got %d", pos)
	}

	pos = file.ColumnPosByName("third")
	if pos != 2 {
		t.Errorf("Expected position 2 for 'third', got %d", pos)
	}

	// Test non-existing column name
	pos = file.ColumnPosByName("nonexistent")
	if pos != -1 {
		t.Errorf("Expected -1 for non-existent column, got %d", pos)
	}
}

func TestFile_ColumnPos(t *testing.T) {
	col1 := createColumn("first")
	col2 := createColumn("second")
	col3 := createColumn("third")
	columns := []*Column{col1, col2, col3}

	file := &File{
		table: &Table{
			columns: columns,
		},
	}

	// Test existing columns
	pos := file.ColumnPos(col1)
	if pos != 0 {
		t.Errorf("Expected position 0 for first column, got %d", pos)
	}

	pos = file.ColumnPos(col2)
	if pos != 1 {
		t.Errorf("Expected position 1 for second column, got %d", pos)
	}

	pos = file.ColumnPos(col3)
	if pos != 2 {
		t.Errorf("Expected position 2 for third column, got %d", pos)
	}

	// Test non-existing column
	otherCol := createColumn("other")
	pos = file.ColumnPos(otherCol)
	if pos != -1 {
		t.Errorf("Expected -1 for non-existent column, got %d", pos)
	}
}

func TestFile_SetColumnModification(t *testing.T) {
	col1 := createColumn("first")
	col2 := createColumn("second")

	columns := []*Column{col1, col2}
	file := &File{
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, 2),
		},
	}

	mod := &Modification{
		TrimSpaces:  true,
		ExternalKey: "external_key",
	}

	file.SetColumnModification(0, mod)

	if file.table.mods[0] != mod {
		t.Error("Modification not set correctly")
	}
}

func TestFile_SetColumnModificationByName(t *testing.T) {
	col1 := createColumn("first")
	col2 := createColumn("second")

	columns := []*Column{col1, col2}
	file := &File{
		table: &Table{
			columns: columns,
			mods:    make([]*Modification, 2),
		},
	}

	mod := &Modification{
		TrimSpaces:  true,
		ExternalKey: "external_key",
	}

	// Test with existing column
	err := file.SetColumnModificationByName("first", mod)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if file.table.mods[0] != mod {
		t.Error("Modification not set correctly")
	}

	// Test with non-existing column
	err = file.SetColumnModificationByName("nonexistent", mod)
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

func TestFile_GetColumnModification(t *testing.T) {
	mod := &Modification{
		TrimSpaces:  true,
		ExternalKey: "external_key",
	}

	col1 := createColumn("first")
	col2 := createColumn("second")

	columns := []*Column{col1, col2}
	file := &File{
		table: &Table{
			columns: columns,
			mods:    []*Modification{mod, nil},
		},
	}

	// Test getting existing modification
	result := file.GetColumnModification(0)
	if result != mod {
		t.Error("Expected to get the same modification instance")
	}

	// Test getting nil modification
	result = file.GetColumnModification(1)
	if result != nil {
		t.Error("Expected nil for column without modification")
	}
}

func TestFile_NewRow(t *testing.T) {
	col1 := createColumn("first")
	col2 := createColumn("second")

	columns := []*Column{col1, col2}
	file := &File{
		header: &Header{
			RowsCount: 10,
		},
		table: &Table{
			columns: columns,
		},
	}

	row := file.NewRow()
	if row == nil {
		t.Error("NewRow() returned nil")
	}

	if len(row.fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(row.fields))
	}

	// Check that the new row has the correct position
	if row.Position != 11 { // RowsCount + 1
		t.Errorf("Expected position 11, got %d", row.Position)
	}

	// Check that fields are properly initialized
	for i, field := range row.fields {
		if field == nil {
			t.Errorf("Field %d is nil", i)
		}
		if field.column != columns[i] {
			t.Errorf("Field %d has wrong column reference", i)
		}
		if field.value != nil {
			t.Errorf("Field %d should have nil value initially", i)
		}
	}
}
