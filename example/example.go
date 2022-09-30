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

	// Read the first row (rowPointer start at the first row).
	row, err := dbf.Row()
	if err != nil {
		panic(err)
	}

	// Print all the row fields as interface{} slice.
	fmt.Printf("%+v \n", row.Values())

	// Go back to start to read the file again.
	err = dbf.GoTo(0)
	if err != nil {
		panic(err)
	}

	// Loop through all rows using rowPointer in DBF struct.
	for !dbf.EOF() {
		fmt.Printf("EOF: %v - Pointer: %v \n", dbf.EOF(), dbf.Pointer())

		row, err := dbf.Row()
		if err != nil {
			panic(err)
		}

		// Increment the row pointer.
		dbf.Skip(1)
		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row %v \n", row.Position)
			continue
		}

		// Get the first field by column position
		field, err := row.Field(0)
		if err != nil {
			panic(err)
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.Value())

		// Get value by column name
		field, err = row.Field(dbf.ColumnPos("COMP_NAME"))
		if err != nil {
			panic(err)
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.Value())

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
}
