package dbase

import (
	"os"
	"testing"
	"time"
)

const testFile = "../examples/test_data/table/TEST.DBF"

func TestOpenTable(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	if table.TableName() == "" {
		t.Error("Table name should not be empty")
	}

	if table.RowsCount() == 0 {
		t.Error("Table should have rows")
	}

	if !table.BOF() {
		t.Error("Table should be at beginning of file initially")
	}

	if table.EOF() {
		t.Error("Table should not be at end of file initially")
	}
}

func TestTableColumns(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	columns := table.Columns()
	if len(columns) == 0 {
		t.Error("Table should have columns")
	}

	if len(columns) < 10 {
		t.Errorf("Expected at least 10 columns, got %d", len(columns))
	}

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name()
		if col.Name() == "" {
			t.Errorf("Column %d should have a name", i)
		}
	}
}

func TestTableNavigation(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	if table.Pointer() != 0 {
		t.Error("Initial pointer should be 0")
	}

	table.Skip(1)
	if table.Pointer() != 1 {
		t.Error("Pointer should be 1 after skipping to first row")
	}

	err = table.GoTo(1)
	if err != nil {
		t.Errorf("Should be able to go to first row: %v", err)
	}

	if table.Pointer() != 1 {
		t.Error("Pointer should be 1 after going to first row")
	}
}

func TestTableReadRow(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	table.Skip(1)

	row, err := table.Row()
	if err != nil {
		t.Fatalf("Failed to read row: %v", err)
	}

	if len(row.Fields()) == 0 {
		t.Error("Row should have fields")
	}

	if row.Position == 0 {
		t.Error("Row position should not be 0")
	}

	fields := row.Fields()
	for _, field := range fields {
		if field.Name() == "" {
			t.Error("Field name should not be empty")
		}
	}
}

func TestTableRowAsMap(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	table.Skip(1)

	row, err := table.Row()
	if err != nil {
		t.Fatalf("Failed to get row: %v", err)
	}

	rowMap, err := row.ToMap()
	if err != nil {
		t.Fatalf("Failed to get row as map: %v", err)
	}

	if len(rowMap) == 0 {
		t.Error("Row map should not be empty")
	}

	expectedKeys := []string{"PRODUCTID", "PRODNAME", "PRICE", "ACTIVE"}
	found := 0
	for _, key := range expectedKeys {
		if _, exists := rowMap[key]; exists {
			found++
		}
	}

	if found == 0 {
		t.Error("Row map should contain some expected keys")
	}
}

func TestTableHeader(t *testing.T) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	header := table.Header()
	if header == nil {
		t.Error("Header should not be nil")
	}

	if header.RowsCount == 0 {
		t.Error("Header should show rows count > 0")
	}

	if header.FirstRow == 0 {
		t.Error("Header should show first row position > 0")
	}

	if header.RowLength == 0 {
		t.Error("Header should show row length > 0")
	}

	modifiedDate := header.Modified(2000)
	if modifiedDate.IsZero() {
		t.Error("Modified date should not be zero")
	}

	if modifiedDate.After(time.Now().AddDate(1, 0, 0)) {
		t.Error("Modified date should not be too far in the future")
	}
}

func TestTableConfigValidation(t *testing.T) {
	_, err := OpenTable(&Config{Filename: ""})
	if err == nil {
		t.Error("Should fail with empty filename")
	}

	_, err = OpenTable(&Config{Filename: "/non/existent/file.dbf"})
	if err == nil {
		t.Error("Should fail with non-existent file")
	}
}

func TestRow_Values(t *testing.T) {
	row := &Row{
		fields: []*Field{
			{value: "hello"},
			{value: 123},
			{value: true},
			nil, // Test with nil field
		},
	}

	values := row.Values()
	expected := []interface{}{"hello", 123, true}

	if len(values) != len(expected) {
		t.Errorf("Expected %d values, got %d", len(expected), len(values))
		return
	}

	for i, expected := range expected {
		if values[i] != expected {
			t.Errorf("Expected value %v at index %d, got %v", expected, i, values[i])
		}
	}
}

// Helper function to create a test file with columns
func createTestFile(columnNames ...string) *File {
	columns := make([]*Column, len(columnNames))
	for i, name := range columnNames {
		column := &Column{}
		copy(column.FieldName[:], name)
		columns[i] = column
	}

	return &File{
		table: &Table{
			columns: columns,
		},
	}
}

func TestRow_MustValueByName(t *testing.T) {
	file := createTestFile("test")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: "test_value"},
		},
	}

	result := row.MustValueByName("test")
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got %v", result)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustValueByName("nonexistent")
}

func TestRow_StringValueByName(t *testing.T) {
	file := createTestFile("str", "bytes")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: "hello"},
			{value: []byte("world")},
		},
	}

	result, err := row.StringValueByName("str")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}

	result, err = row.StringValueByName("bytes")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != "world" {
		t.Errorf("Expected 'world', got '%s'", result)
	}

	_, err = row.StringValueByName("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent field")
	}
}

func TestRow_MustStringValueByName(t *testing.T) {
	file := createTestFile("test")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: "test_value"},
		},
	}

	result := row.MustStringValueByName("test")
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got %s", result)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustStringValueByName("nonexistent")
}

func TestRow_IntValueByName(t *testing.T) {
	file := createTestFile("int_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: int64(123)},
		},
	}

	value, err := row.IntValueByName("int_field")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != int64(123) {
		t.Errorf("Expected int64(123), got %v", value)
	}

	_, err = row.IntValueByName("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent field")
	}
}

func TestRow_MustIntValueByName(t *testing.T) {
	file := createTestFile("int_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: int64(456)},
		},
	}

	value := row.MustIntValueByName("int_field")
	if value != int64(456) {
		t.Errorf("Expected int64(456), got %v", value)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustIntValueByName("nonexistent")
}

func TestRow_FloatValueByName(t *testing.T) {
	file := createTestFile("float_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: 123.45},
		},
	}

	value, err := row.FloatValueByName("float_field")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != 123.45 {
		t.Errorf("Expected 123.45, got %v", value)
	}

	_, err = row.FloatValueByName("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent field")
	}
}

func TestRow_MustFloatValueByName(t *testing.T) {
	file := createTestFile("float_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: 678.90},
		},
	}

	value := row.MustFloatValueByName("float_field")
	if value != 678.90 {
		t.Errorf("Expected 678.90, got %v", value)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustFloatValueByName("nonexistent")
}

func TestRow_BoolValueByName(t *testing.T) {
	file := createTestFile("bool_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: true},
		},
	}

	value, err := row.BoolValueByName("bool_field")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != true {
		t.Errorf("Expected true, got %v", value)
	}

	_, err = row.BoolValueByName("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent field")
	}
}

func TestRow_MustBoolValueByName(t *testing.T) {
	file := createTestFile("bool_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: false},
		},
	}

	value := row.MustBoolValueByName("bool_field")
	if value != false {
		t.Errorf("Expected false, got %v", value)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustBoolValueByName("nonexistent")
}

func TestRow_BytesValueByName(t *testing.T) {
	file := createTestFile("bytes_field")
	testBytes := []byte("test bytes")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: testBytes},
		},
	}

	value, err := row.BytesValueByName("bytes_field")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if string(value) != string(testBytes) {
		t.Errorf("Expected %v, got %v", testBytes, value)
	}

	_, err = row.BytesValueByName("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent field")
	}
}

func TestRow_MustBytesValueByName(t *testing.T) {
	file := createTestFile("bytes_field")
	testBytes := []byte("must bytes")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: testBytes},
		},
	}

	value := row.MustBytesValueByName("bytes_field")
	if string(value) != string(testBytes) {
		t.Errorf("Expected %v, got %v", testBytes, value)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent field")
		}
	}()
	row.MustBytesValueByName("nonexistent")
}

func TestRow_Field(t *testing.T) {
	file := createTestFile("field1")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: "test"},
		},
	}

	field := row.Field(0)
	if field.GetValue() != "test" {
		t.Errorf("Expected 'test', got %v", field.GetValue())
	}
}

func TestRow_FieldByName(t *testing.T) {
	file := createTestFile("named_field")

	row := &Row{
		handle: file,
		fields: []*Field{
			{value: "test"},
		},
	}

	field := row.FieldByName("named_field")
	if field.GetValue() != "test" {
		t.Errorf("Expected 'test', got %v", field.GetValue())
	}
}
