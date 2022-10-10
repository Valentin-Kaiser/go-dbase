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
	Stock       int64     `json:"STOCK"`
	Date        time.Time `json:"DATE"`
	DateTime    time.Time `json:"DATETIME"`
	Description string    `json:"DESCRIPTION"`
	Active      bool      `json:"ACTIVE"`
	Float       float64   `json:"FLOAT"`
	Integer     int64     `json:"INTEGER"`
	Double      float64   `json:"DOUBLE"`
}

func main() {
	// Open the example database file.
	dbf, err := dbase.Open("../test_data/TEST.DBF", new(dbase.Win1250Converter), false)
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
	// Search for the product with the ID 1.
	field, err := dbf.NewField(dbf.ColumnPosByName("PRODUCTID"), int32(1))
	if err != nil {
		panic(err)
	}

	// Execute the search.
	records, err := dbf.Search(field)
	if err != nil {
		panic(err)
	}

	// Print all found records.
	fmt.Println("Found records:")
	for _, record := range records {
		field, err := record.Field(dbf.ColumnPosByName("PRODNAME"))
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v \n", field.GetValue())
	}
}
