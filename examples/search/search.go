package main

import (
	"fmt"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

func main() {
	// Open the example database file.
	dbf, err := dbase.Open(&dbase.Config{
		Filename: "../test_data/TEST.DBF",
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

	// Init the field we want to search for.
	// Search for a product containing the word "test" in the name.
	field, err := dbf.NewFieldByName("PRODNAME", "TEST")
	if err != nil {
		panic(err)
	}

	// Execute the search with an exact match.
	records, err := dbf.Search(field, false)
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
	records, err = dbf.Search(field, true)
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
