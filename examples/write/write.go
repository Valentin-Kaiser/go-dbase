package main

import (
	"fmt"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Product struct {
	ID          int32     `json:"PRODUCTID"`
	Name        string    `json:"PRODNAME"`
	Price       float64   `json:"PRICE"`
	Tax         float64   `json:"TAX"`
	Stock       int64     `json:"INSTOCK"`
	Date        time.Time `json:"DATE"`
	DateTime    time.Time `json:"DATETIME"`
	Description string    `json:"DESC"`
	Active      bool      `json:"ACTIVE"`
	Float       float64   `json:"FLOAT"`
	Integer     int64     `json:"INTEGER"`
	Double      float64   `json:"DOUBLE"`
}

func main() {
	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename:   "../test_data/table/TEST.DBF",
		TrimSpaces: true,
		WriteLock:  true,
	})
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(),
		table.Header().ColumnsCount(),
		table.Header().RecordsCount(),
		table.Header().FileSize(),
	)

	// Read the first row (rowPointer start at the first row).
	row, err := table.Row()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Get the company name field by column name.
	err = row.FieldByName("PRODNAME").SetValue("CHANGED_PRODUCT_NAME")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Change a memo field value.
	err = row.FieldByName("DESC").SetValue("MEMO_TEST_VALUE")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Write the changed row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// === Modifications ===

	// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
	table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
	table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})

	// Create a new row with the same structure as the database table.
	p := Product{
		ID:          99,
		Name:        "NEW_PRODUCT",
		Price:       99.99,
		Tax:         19.99,
		Stock:       999,
		Date:        time.Now(),
		DateTime:    time.Now(),
		Description: "NEW_PRODUCT_DESCRIPTION",
		Active:      true,
		Float:       105.67,
		Integer:     104,
		Double:      103.45,
	}

	row, err = table.RowFromStruct(p)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Add the new row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Print all rows.
	for !table.EOF() {
		row, err := table.Row()
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		// Increment the row pointer.
		table.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}

		// Print the current row values.
		fmt.Println(row.Values()...)
	}
}
