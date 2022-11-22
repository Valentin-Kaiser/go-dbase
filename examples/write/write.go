package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Product struct {
	ID          int32     `dbase:"PRODUCTID"`
	Name        string    `dbase:"PRODNAME"`
	Price       float64   `dbase:"PRICE"`
	Double      float64   `dbase:"DOUBLE"`
	Date        time.Time `dbase:"DATE"`
	DateTime    time.Time `dbase:"DATETIME"`
	Integer     int32     `dbase:"INTEGER"`
	Float       float64   `dbase:"FLOAT"`
	Active      bool      `dbase:"ACTIVE"`
	Description string    `dbase:"DESC"`
	Tax         float64   `dbase:"TAX"`
	Stock       int64     `dbase:"INSTOCK"`
	Blob        []byte    `dbase:"BLOB"`
	Varbinary   []byte    `dbase:"VARBIN_NIL"`
	Varchar     string    `dbase:"VAR_NIL"`
	Var         string    `dbase:"VAR"`
}

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.Debug(true, io.MultiWriter(os.Stdout, f))

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename:   "../test_data/table/TEST.DBF",
		TrimSpaces: true,
		WriteLock:  true,
	}, nil)
	if err != nil {
		panic(dbase.GetErrorTrace(err))
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
		panic(dbase.GetErrorTrace(err))
	}

	// Get the company name field by column name.
	err = row.FieldByName("PRODNAME").SetValue("CHANGED_PRODUCT_NAME")
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// Change a memo field value.
	err = row.FieldByName("DESC").SetValue("MEMO_TEST_VALUE")
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// Write the changed row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// === Modifications ===

	// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
	err = table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	err = table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

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
		Varchar:     "VARCHAR",
	}

	row, err = table.RowFromStruct(p)
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// Add the new row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// Print all rows.
	for !table.EOF() {
		row, err := table.Row()
		if err != nil {
			panic(dbase.GetErrorTrace(err))
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
