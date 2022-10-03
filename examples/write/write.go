package main

import (
	"fmt"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Test struct {
	ID          int32     `json:"ID"`
	Niveau      int32     `json:"NIVEAU"`
	Date        time.Time `json:"DATUM"`
	TIJD        string    `json:"TIJD"`
	SOORT       float64   `json:"SOORT"`
	IDNR        int32     `json:"ID_NR"`
	UserNR      int32     `json:"USERNR"`
	CompanyName string    `json:"COMP_NAME"`
	CompanyOS   string    `json:"COMP_OS"`
	Melding     string    `json:"MELDING"`
	Number      float64   `json:"NUMBER"`
	Float       int64     `json:"FLOAT"`
	Bool        bool      `json:"BOOL"`
}

func main() {
	// Open the example database file.
	dbf, err := dbase.Open("../test_data/TEST.DBF", new(dbase.Win1250Converter))
	if err != nil {
		panic(err)
	}
	defer dbf.Close()

	// Read the first row (rowPointer start at the first row).
	row, err := dbf.Row()
	if err != nil {
		panic(err)
	}

	// Get the company name field by column name.
	field, err := row.Field(dbf.ColumnPosByName("COMP_NAME"))
	if err != nil {
		panic(err)
	}

	// Change the field value
	field.SetValue("CHANGED_COMPANY_NAME")

	// Apply the changed field value to the row.
	err = row.ChangeField(field)
	if err != nil {
		panic(err)
	}

	// Change a memo field value.
	field, err = row.Field(dbf.ColumnPosByName("MELDING"))
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

	// Create a new row with the same structure as the database file.
	t := Test{
		ID:          99,
		Niveau:      100,
		Date:        time.Now(),
		TIJD:        "00:00",
		SOORT:       101.23,
		IDNR:        102,
		UserNR:      103,
		CompanyName: "NEW_COMPANY_NAME",
		CompanyOS:   "NEW_COMPANY_OS",
		Melding:     "NEW_MEMO_TEST_VALUE",
		Number:      104,
		Float:       105,
		Bool:        true,
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

		// Get the first field by column position
		fmt.Println(row.Values()...)
	}
}
