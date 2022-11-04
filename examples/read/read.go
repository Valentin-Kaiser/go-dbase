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
	Tax         float64   `dbase:"TAX"`
	Stock       int64     `dbase:"STOCK"`
	Date        time.Time `dbase:"DATE"`
	DateTime    time.Time `dbase:"DATETIME"`
	Description string    `dbase:"DESCRIPTION"`
	Active      bool      `dbase:"ACTIVE"`
	Float       float64   `dbase:"FLOAT"`
	Integer     int32     `dbase:"INTEGER"`
	Double      float64   `dbase:"DOUBLE"`
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
	})
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

	// Print all database column infos.
	for _, column := range table.Columns() {
		fmt.Printf("Name: %v - Type: %v \n", column.Name(), column.Type())
	}

	// Loop through all rows using rowPointer in DBF struct.
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

		// Get the first field by column position
		field := row.Field(0)
		if field == nil {
			panic("Field not found")
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.GetValue())

		// Get value by column name
		field = row.FieldByName("PRODNAME")
		if field == nil {
			panic("Field not found")
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.GetValue())

		// === Modifications ===

		// Disable space trimming for the company name
		err = table.SetColumnModificationByName("PRODNAME", &dbase.Modification{TrimSpaces: false})
		if err != nil {
			panic(dbase.GetErrorTrace(err))
		}

		// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
		err = table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
		if err != nil {
			panic(dbase.GetErrorTrace(err))
		}

		err = table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})
		if err != nil {
			panic(dbase.GetErrorTrace(err))
		}

		// === Struct Conversion ===

		// Read the row into a struct.
		p := &Product{}
		err = row.Struct(p)
		if err != nil {
			panic(dbase.GetErrorTrace(err))
		}

		fmt.Printf("Product: %+v \n", p)
	}
}
