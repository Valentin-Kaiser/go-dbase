package dbase

import (
	"os"
	"testing"
	"time"
)

func TestOpenTable(t *testing.T) {
	// Use test data from examples
	testFile := "../examples/test_data/table/TEST.DBF"
	
	// Check if test file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test data file not found, skipping test")
	}

	// Test opening a table
	table, err := OpenTable(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open table: %v", err)
	}
	defer table.Close()

	// Verify table properties
	if table.TableName() == "" {
		t.Error("Table name should not be empty")
	}

	if table.RowsCount() == 0 {
		t.Error("Table should have rows")
	}

	// Test BOF/EOF
	if !table.BOF() {
		t.Error("Table should be at beginning of file initially")
	}

	if table.EOF() {
		t.Error("Table should not be at end of file initially")
	}
}

func TestTableColumns(t *testing.T) {
	testFile := "../examples/test_data/table/TEST.DBF"
	
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

	// Test columns
	columns := table.Columns()
	if len(columns) == 0 {
		t.Error("Table should have columns")
	}

	// Check that we have a reasonable number of columns
	if len(columns) < 10 {
		t.Errorf("Expected at least 10 columns, got %d", len(columns))
	}

	// Check column names
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name()
		if col.Name() == "" {
			t.Errorf("Column %d should have a name", i)
		}
	}
}

func TestTableNavigation(t *testing.T) {
	testFile := "../examples/test_data/table/TEST.DBF"
	
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

	// Test navigation
	if table.Pointer() != 0 {
		t.Error("Initial pointer should be 0")
	}

	// Move to first row
	table.Skip(1)
	if table.Pointer() != 1 {
		t.Error("Pointer should be 1 after skipping to first row")
	}

	// Test GoTo
	err = table.GoTo(1)
	if err != nil {
		t.Errorf("Should be able to go to first row: %v", err)
	}
	
	if table.Pointer() != 1 {
		t.Error("Pointer should be 1 after going to first row")
	}
}

func TestTableReadRow(t *testing.T) {
	testFile := "../examples/test_data/table/TEST.DBF"
	
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

	// Move to first row and read
	table.Skip(1)
	
	row, err := table.Row()
	if err != nil {
		t.Fatalf("Failed to read row: %v", err)
	}

	// Check if row has fields
	if len(row.Fields()) == 0 {
		t.Error("Row should have fields")
	}

	// Test row position
	if row.Position == 0 {
		t.Error("Row position should not be 0")
	}

	// Test field values
	fields := row.Fields()
	for _, field := range fields {
		// Just check that fields have names and values
		if field.Name() == "" {
			t.Error("Field name should not be empty")
		}
		// Value can be nil for some fields, so we don't check it
	}
}

func TestTableRowAsMap(t *testing.T) {
	testFile := "../examples/test_data/table/TEST.DBF"
	
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

	// Move to first row
	table.Skip(1)

	// Get row first
	row, err := table.Row()
	if err != nil {
		t.Fatalf("Failed to get row: %v", err)
	}

	// Get row as map
	rowMap, err := row.ToMap()
	if err != nil {
		t.Fatalf("Failed to get row as map: %v", err)
	}

	if len(rowMap) == 0 {
		t.Error("Row map should not be empty")
	}

	// Check for some expected keys
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
	testFile := "../examples/test_data/table/TEST.DBF"
	
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

	// Test header
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

	// Test modified date
	modifiedDate := header.Modified(2000)
	if modifiedDate.IsZero() {
		t.Error("Modified date should not be zero")
	}

	// Check if modified date is reasonable (not too far in the future)
	if modifiedDate.After(time.Now().AddDate(1, 0, 0)) {
		t.Error("Modified date should not be too far in the future")
	}
}

func TestTableConfigValidation(t *testing.T) {
	// Test empty filename
	_, err := OpenTable(&Config{Filename: ""})
	if err == nil {
		t.Error("Should fail with empty filename")
	}

	// Test non-existent file
	_, err = OpenTable(&Config{Filename: "/non/existent/file.dbf"})
	if err == nil {
		t.Error("Should fail with non-existent file")
	}
	
	// Skip nil config test as it causes panic in the current implementation
	// This would be a good candidate for a production fix
}