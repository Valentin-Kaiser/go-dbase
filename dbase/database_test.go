package dbase

import (
	"os"
	"testing"
)

func TestOpenDatabase(t *testing.T) {
	// Use test data from examples
	testFile := "../examples/test_data/database/EXPENSES.DBC"
	
	// Check if test file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test database file not found, skipping test")
	}

	// Test opening a database
	db, err := OpenDatabase(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test getting tables
	tables := db.Tables()
	if len(tables) == 0 {
		t.Error("Database should have tables")
	}

	// Test table names
	tableNames := db.Names()
	if len(tableNames) == 0 {
		t.Error("Database should have table names")
	}

	// Test schema
	schema := db.Schema()
	if len(schema) == 0 {
		t.Error("Database should have schema")
	}

	// Verify schema has columns for each table
	for tableName, columns := range schema {
		if len(columns) == 0 {
			t.Errorf("Table %s should have columns", tableName)
		}
	}
}

func TestDatabaseTableAccess(t *testing.T) {
	testFile := "../examples/test_data/database/EXPENSES.DBC"
	
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test database file not found, skipping test")
	}

	db, err := OpenDatabase(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get table names
	tableNames := db.Names()
	if len(tableNames) == 0 {
		t.Skip("No tables found in database")
	}

	// Get tables map
	tables := db.Tables()
	if len(tables) != len(tableNames) {
		t.Errorf("Expected %d tables, got %d", len(tableNames), len(tables))
	}

	// Test accessing first table
	firstTableName := tableNames[0]
	table, exists := tables[firstTableName]
	if !exists {
		t.Fatalf("Table %s not found in tables map", firstTableName)
	}

	if table == nil {
		t.Error("Table should not be nil")
	}

	// Test that table has expected properties
	if table.TableName() == "" {
		t.Error("Table name should not be empty")
	}

	if table.Header() == nil {
		t.Error("Table header should not be nil")
	}

	if len(table.Columns()) == 0 {
		t.Error("Table should have columns")
	}
}

func TestDatabaseConfigValidation(t *testing.T) {
	// Test nil config
	_, err := OpenDatabase(nil)
	if err == nil {
		t.Error("Should fail with nil config")
	}

	// Test empty filename
	_, err = OpenDatabase(&Config{Filename: ""})
	if err == nil {
		t.Error("Should fail with empty filename")
	}

	// Test wrong file extension
	_, err = OpenDatabase(&Config{Filename: "test.dbf"})
	if err == nil {
		t.Error("Should fail with wrong file extension")
	}

	// Test non-existent file
	_, err = OpenDatabase(&Config{Filename: "/non/existent/file.dbc"})
	if err == nil {
		t.Error("Should fail with non-existent file")
	}
}

func TestDatabaseClose(t *testing.T) {
	testFile := "../examples/test_data/database/EXPENSES.DBC"
	
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Test database file not found, skipping test")
	}

	db, err := OpenDatabase(&Config{
		Filename:   testFile,
		TrimSpaces: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Test that close doesn't panic
	err = db.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}