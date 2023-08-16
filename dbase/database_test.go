package dbase

import (
	"fmt"
	"reflect"
	"testing"
)

func TestOpenDatabase(t *testing.T) {
	tests := []struct {
		config      *Config
		expected    *Database
		hasError    bool
		description string
	}{
		{&Config{Filename: "../examples/test_data/database/EXPENSES.dbc"}, &Database{}, false, "Valid Config"},
		{&Config{Filename: ""}, nil, true, "Empty Filename"},
		{&Config{Filename: "invalid_extension.txt"}, nil, true, "Invalid File Extension"},
		// Add more tests as necessary.
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := OpenDatabase(tt.config)
			if (err != nil) != tt.hasError {
				t.Errorf("expected error=%v, got %v", tt.hasError, err)
			}

			if reflect.TypeOf(got) != reflect.TypeOf(tt.expected) {
				t.Errorf("expected type=%v, got %v", reflect.TypeOf(tt.expected), reflect.TypeOf(got))
			}

			if got != nil {
				err = got.Close()
				if err != nil {
					t.Errorf("expected error=%v, got %v", nil, err)
				}
			}
		})
	}
}

func TestClose(t *testing.T) {
	tests := []*Config{
		{Filename: "../examples/test_data/database/EXPENSES.dbc"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Close %s", tt.Filename), func(t *testing.T) {
			db, err := OpenDatabase(tt)
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}

			err = db.Close()
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}
		})
	}
}

func TestTables(t *testing.T) {
	tests := []struct {
		config   *Config
		expected []string
	}{
		{&Config{Filename: "../examples/test_data/database/EXPENSES.dbc"}, []string{
			"employees",
			"expense_categories",
			"expense_details",
			"expense_reports",
		}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Talbes of %s", tt.config.Filename), func(t *testing.T) {
			db, err := OpenDatabase(tt.config)
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}

			got := db.Tables()
			for _, table := range tt.expected {
				if _, ok := got[table]; !ok {
					t.Errorf("expected table=%s, got %v", table, got)
				}
			}

			err = db.Close()
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}
		})
	}
}

func TestNames(t *testing.T) {
	tests := []struct {
		config   *Config
		expected []string
	}{
		{&Config{Filename: "../examples/test_data/database/EXPENSES.dbc"}, []string{
			"employees",
			"expense_categories",
			"expense_details",
			"expense_reports",
		}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Names of %s", tt.config.Filename), func(t *testing.T) {
			db, err := OpenDatabase(tt.config)
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}

			got := db.Names()
			if reflect.DeepEqual(got, tt.expected) {
				t.Errorf("expected names=%v, got %v", tt.expected, got)
			}

			err = db.Close()
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}
		})
	}
}

func TestSchema(t *testing.T) {
	tests := []struct {
		config   *Config
		expected map[string][]string
	}{
		{&Config{Filename: "../examples/test_data/database/EXPENSES.dbc"}, map[string][]string{
			"employees": {
				"EMPLOYEEID",
				"DEPARTMENT",
				"SOCIALSECU",
				"EMPLOYEENU",
				"ADDRESS",
				"CITY",
				"STATEORPRO",
				"POSTALCODE",
				"COUNTRY",
				"EMAILNAME",
				"EXTENSION",
				"FIRSTNAME",
				"LASTNAME",
				"NOTES",
				"TITLE",
				"WORKPHONE",
			},
			"expense_categories": {
				"EXPENSECA2",
				"EXPENSECA3",
				"EXPENSECAT",
			},
			"expense_details": {
				"EXPENSECAT",
				"EXPENSEDAT",
				"EXPENSEIT2",
				"EXPENSEITE",
				"EXPENSEREP",
			},
			"expense_reports": {
				"ADVANCEAMO",
				"DATESUBMIT",
				"DEPARTMENT",
				"EMPLOYEEID",
				"EXPENSEREP",
				"EXPENSERP2",
				"EXPENSERPT",
				"EXPENSETYP",
				"PAID",
			},
		}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Schema of %s", tt.config.Filename), func(t *testing.T) {
			db, err := OpenDatabase(tt.config)
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}

			got := db.Schema()
			for table, columns := range tt.expected {
				if _, ok := got[table]; !ok {
					t.Errorf("expected table=%s, got %v", table, got)
				}

				cols := make(map[string]bool)
				for _, col := range got[table] {
					cols[col.Name()] = true
				}

				for _, column := range columns {
					if _, ok := cols[column]; !ok {
						t.Errorf("expected column=%s not found in table=%s", column, table)
					}
				}
			}

			err = db.Close()
			if err != nil {
				t.Errorf("unexpected error=%v", err)
			}
		})
	}
}
