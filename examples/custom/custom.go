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
	logf, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	dbase.Debug(true, io.MultiWriter(os.Stdout, logf))

	dbf, err := os.OpenFile("../test_data/table/TEST.DBF", os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	memo, err := os.OpenFile("../test_data/table/TEST.FPT", os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{TrimSpaces: true, IO: dbase.GenericIO{Handle: dbf, RelatedHandle: memo}})
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

		fmt.Printf("Row at position: %v \n", row.Position)
		fmt.Println(row.Values())
	}

	err = table.GoTo(0)
	if err != nil {
		panic(err)
	}

	// Read the first row.
	row, err := table.Row()
	if err != nil {
		panic(err)
	}

	// Get the company name field by column name and change it.
	err = row.FieldByName("PRODNAME").SetValue("CHANGED_PRODUCT_NAME")
	if err != nil {
		panic(err)
	}

	// Change a memo field value.
	err = row.FieldByName("DESC").SetValue("MEMO_TEST_VALUE")
	if err != nil {
		panic(err)
	}

	// Write the changed row to the database table.
	err = row.Write()
	if err != nil {
		panic(err)
	}
}
