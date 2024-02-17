package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Product struct {
	// The dbase tag contains the table name and column name separated by a dot.
	// The column name is case insensitive.
	ID          int32     `dbase:"TEST.PRODUCTID"`
	Name        string    `dbase:"TEST.PRODNAME"`
	Price       float64   `dbase:"TEST.PRICE"`
	Double      float64   `dbase:"TEST.DOUBLE"`
	Date        time.Time `dbase:"TEST.DATE"`
	DateTime    time.Time `dbase:"TEST.DATETIME"`
	Integer     int32     `dbase:"TEST.INTEGER"`
	Float       float64   `dbase:"TEST.FLOAT"`
	Active      bool      `dbase:"TEST.ACTIVE"`
	Description string    `dbase:"TEST.DESC"`
	Tax         float64   `dbase:"TEST.TAX"`
	Stock       int64     `dbase:"TEST.INSTOCK"`
	Blob        []byte    `dbase:"TEST.BLOB"`
	Varbinary   []byte    `dbase:"TEST.VARBIN_NIL"`
	Varchar     string    `dbase:"TEST.VAR_NIL"`
	Var         string    `dbase:"TEST.VAR"`
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
		Filename:   "../test_data/table/TEST123.DBF",
		TrimSpaces: true,
	})
	if err != nil {
		panic(err)
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(0),
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
		row, err := table.Next()
		if err != nil {
			panic(err)
		}

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
			panic(err)
		}

		// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
		err = table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
		if err != nil {
			panic(err)
		}

		err = table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})
		if err != nil {
			panic(err)
		}

		// === Struct Conversion ===

		// Read the row into a struct.
		p := &Product{}
		err = row.ToStruct(p)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Product: %+v \n", p)
	}
}
