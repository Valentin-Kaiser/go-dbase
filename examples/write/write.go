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
	// Open the example database file.
	dbf, err := dbase.Open(&dbase.Config{
		Filename:   "../test_data/TEST.DBF",
		Converter:  new(dbase.Win1250Converter),
		TrimSpaces: true,
		WriteLock:  true,
	})
	if err != nil {
		panic(err)
	}
	defer dbf.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		dbf.Header().Modified(),
		dbf.Header().ColumnsCount(),
		dbf.Header().RecordsCount(),
		dbf.Header().FileSize(),
	)

	// Read the first row (rowPointer start at the first row).
	row, err := dbf.Row()
	if err != nil {
		panic(err)
	}

	// Get the company name field by column name.
	field, err := row.FieldByName("PRODNAME")
	if err != nil {
		panic(err)
	}

	// Change the field value
	field.SetValue("CHANGED_PRODUCT_NAME")

	// Apply the changed field value to the row.
	err = row.ChangeField(field)
	if err != nil {
		panic(err)
	}

	// Change a memo field value.
	field, err = row.FieldByName("DESC")
	if err != nil {
		panic(err)
	}

	// Change the field value
	field.SetValue("MEMO_TEST_VALUE")

	// Apply the changed field value to the row.
	err = row.ChangeField(field)
	if err != nil {
		panic(err)
	}

	// Write the changed row to the database file.
	err = row.Write()
	if err != nil {
		panic(err)
	}

	// === Modifications ===

	// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
	dbf.SetColumnModificationByName("INTEGER", true, "FLOAT", nil)
	dbf.SetColumnModificationByName("FLOAT", true, "INTEGER", nil)

	// Create a new row with the same structure as the database file.
	t := Product{
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

	row, err = dbf.RowFromStruct(t)
	if err != nil {
		panic(err)
	}

	// Add the new row to the database file.
	err = row.Write()
	if err != nil {
		panic(err)
	}

	// Print all rows.
	for !dbf.EOF() {
		row, err := dbf.Row()
		if err != nil {
			panic(err)
		}

		// Increment the row pointer.
		dbf.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}

		// Print the current row values.
		fmt.Println(row.Values()...)
	}
}
