package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

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
		Filename: "../test_data/table/TEST.DBF",
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

	// Init the field we want to search for.
	// Search for a product containing the word "test" in the name.
	field, err := table.NewFieldByName("PRODNAME", "TEST")
	if err != nil {
		panic(err)
	}

	// Execute the search with an exact match.
	records, err := table.Search(field, false)
	if err != nil {
		panic(err)
	}

	// Print all found records.
	fmt.Println("Found records with match:")
	for _, record := range records {
		field = record.FieldByName("PRODNAME")
		if field == nil {
			panic("Field 'PRODNAME' not found")
		}

		fmt.Printf("%v \n", field.GetValue())
	}

	// Execute the search without exact match.
	records, err = table.Search(field, true)
	if err != nil {
		panic(err)
	}

	// Print all found records.
	fmt.Println("Found records with exact match:")
	for _, record := range records {
		field = record.FieldByName("PRODNAME")
		if field == nil {
			panic("Field 'PRODNAME' not found")
		}

		fmt.Printf("%v \n", field.GetValue())
	}
}
