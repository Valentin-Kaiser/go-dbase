package main

import (
	"fmt"
	"io"
	"math"
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
	Integer     float64   `dbase:"INTEGER"`
	Float       int32     `dbase:"FLOAT"`
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
	dbase.Debug(false, io.MultiWriter(os.Stdout, f))

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename:   "../test_data/table/TEST.DBF",
		TrimSpaces: true,
		WriteLock:  true,
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

	// A struct with the maximum possible field values.
	p := Product{
		Name:        "PRODUCT_NAME_12345678901234567890123456789012345678901234567890123456789012345678901234567890",
		Price:       math.MaxFloat64,
		Double:      math.MaxFloat64,
		Date:        time.Now(),
		DateTime:    time.Now(),
		Integer:     math.MaxFloat64,
		Float:       math.MaxInt32,
		Active:      true,
		Description: "PRODUCT_DESCRIPTION_12345678901234567890123456789012345678901234567890123456789012345678901234567890",
		Tax:         math.MaxFloat64,
		Stock:       math.MaxInt64,
	}

	// Write the struct to the table.
	row, err := table.RowFromStruct(p)
	if err != nil {
		panic(err)
	}

	table.Skip(int64(table.RowsCount()))
	err = row.Write()
	if err != nil {
		panic(err)
	}

	// Read the last row.
	raw, err := table.ReadRow(table.Pointer())
	if err != nil {
		panic(err)
	}

	fmt.Println(len(raw), raw)

	row, err = table.Row()
	if err != nil {
		panic(err)
	}

	var p2 Product
	err = row.ToStruct(&p2)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v \n", p2)

	// // Get the company name field by column name.
	// err = row.FieldByName("PRODNAME").SetValue("CHANGED_PRODUCT_NAME")
	// if err != nil {
	// 	panic(err)
	// }

	// // Change a memo field value.
	// err = row.FieldByName("DESC").SetValue("MEMO_TEST_VALUE")
	// if err != nil {
	// 	panic(err)
	// }

	// // Write the changed row to the database table.
	// err = row.Write()
	// if err != nil {
	// 	panic(err)
	// }

	// // === Modifications ===

	// // Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
	// err = table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
	// if err != nil {
	// 	panic(err)
	// }

	// err = table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})
	// if err != nil {
	// 	panic(err)
	// }

	// // Create a new row with the same structure as the database table.
	// p := Product{
	// 	ID:          99,
	// 	Name:        "NEW_PRODUCT",
	// 	Price:       99.99,
	// 	Tax:         19.99,
	// 	Stock:       999,
	// 	Date:        time.Now(),
	// 	DateTime:    time.Now(),
	// 	Description: "NEW_PRODUCT_DESCRIPTION",
	// 	Active:      true,
	// 	Float:       105.67,
	// 	Integer:     104,
	// 	Double:      103.45,
	// 	Varchar:     "VARCHAR",
	// }

	// row, err = table.RowFromStruct(p)
	// if err != nil {
	// 	panic(err)
	// }

	// // Add the new row to the database table.
	// err = row.Write()
	// if err != nil {
	// 	panic(err)
	// }

	// // Print all rows.
	// for !table.EOF() {
	// 	row, err := table.Next()
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	// Skip deleted rows.
	// 	if row.Deleted {
	// 		fmt.Printf("Deleted row at position: %v \n", row.Position)
	// 		continue
	// 	}

	// 	// Print the current row values.
	// 	fmt.Println(row.Values()...)
	// }
}
