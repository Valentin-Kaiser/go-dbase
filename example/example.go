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
	Number      int64     `json:"NUMBER"`
	Float       float64   `json:"FLOAT"`
	Bool        bool      `json:"BOOL"`
}

func main() {
	// Open the example database file.
	dbf, err := dbase.Open("./test_data/TEST.DBF", new(dbase.Win1250Converter))
	if err != nil {
		panic(err)
	}
	defer dbf.Close()

	// Print all database column infos.
	for _, column := range dbf.Columns() {
		fmt.Printf("Name: %v - Type: %v \n", column.Name(), column.Type())
	}

	// Read the complete first row.
	row, err := dbf.Row()
	if err != nil {
		panic(err)
	}

	// Print all the columns in their Go values as slice.
	fmt.Printf("%+v", row.Values())

	// Go back to start.
	dbf.Skip(0)

	// Loop through all rows using rowPointer in DBF struct.
	for !dbf.EOF() {
		fmt.Printf("EOF: %v - Pointer: %v \n", dbf.EOF(), dbf.Pointer())

		// This reads the complete row.
		row, err := dbf.Row()
		if err != nil {
			panic(err)
		}

		// Increase the pointer.
		dbf.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			continue
		}

		// Get value by column position
		_, err = row.Value(0)
		if err != nil {
			panic(err)
		}

		// Get value by column name
		_, err = row.Value(dbf.ColumnPos("COMP_NAME"))
		if err != nil {
			panic(err)
		}

		// Enable space trimming per default
		dbf.SetTrimspacesDefault(true)
		// Disable space trimming for the company name
		dbf.SetColumnModification(dbf.ColumnPos("COMP_NAME"), false, "", nil)
		// Add a column modification to switch the names of "NUMBER" and "Float" to match the data types
		dbf.SetColumnModification(dbf.ColumnPos("NUMBER"), true, "FLOAT", nil)
		dbf.SetColumnModification(dbf.ColumnPos("FLOAT"), true, "NUMBER", nil)

		// Read the row into a struct.
		t := &Test{}
		err = row.ToStruct(t)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Company: %v", t.CompanyName)
	}

	// Read only the third column of rows 1, 2 and 3
	for _, row := range []uint32{1, 2, 3} {
		err := dbf.GoTo(row)
		if err != nil {
			panic(err)
		}

		// Check if the row is deleted
		deleted, err := dbf.Deleted()
		if err != nil {
			panic(err)
		}
		if deleted {
			fmt.Printf("Row %v deleted \n", row)
			continue
		}

		// Read the entire row
		r, err := dbf.Row()
		if err != nil {
			panic(err)
		}

		// Read the seventh column
		column, err := r.Value(7)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Row %v column 7: %v \n", row, column)
	}
}
