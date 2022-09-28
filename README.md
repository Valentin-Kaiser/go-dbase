# go-dbase - Microsoft Visual FoxPro DBF 

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/Valentin-Kaiser/go-dbase)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![golangci-lint](https://github.com/Valentin-Kaiser/go-dbase/workflows/golangci-lint/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![goreport](https://goreportcard.com/badge/github.com/Valentin-Kaiser/go-dbase)](https://goreportcard.com/report/github.com/Valentin-Kaiser/go-dbase)
[![Lines code](https://img.shields.io/tokei/lines/github.com/Valentin-Kaiser/go-dbase)](https://github.com/Valentin-Kaiser/go-dbase)


Golang package for reading FoxPro dBase database files.
This package provides a reader for reading FoxPro database files.

Since these files are almost always used on Windows platforms the default encoding is from Windows-1250 to UTF8 but a universal encoder will be provided for other code pages.
# Features 

There are several similar packages like the [go-foxpro-dbf](https://github.com/SebastiaanKlippert/go-foxpro-dbf) package but they are not suited for our use case, this package implemented:

* Support for FPT (memo) files
* Full support for Windows-1250 encoding to UTF8
* File readers for scanning files (instead of reading the entire file to memory)
* Conversion to map, json and struct
* Non blocking IO operation with syscall

We also aim to support the following features:

* Writing to dBase database files

The focus is on performance while also trying to keep the code readable and easy to use.

# Supported column types

At this moment not all FoxPro column types are supported.
When reading column values, the value returned by this package is always `interface{}`. 
If you need to cast this to the correct value helper functions are provided.

The supported column types with their return Go types are: 

| Column Type | Column Type Name | Golang type |
|------------|-----------------|-------------|
| B | Double | float64 |
| C | Character | string |
| D | Date | time.Time |
| F | Float | float64 |
| I | Integer | int32 |
| L | Logical | bool |
| M | Memo  | string |
| M | Memo (Binary) | []byte |
| N | Numeric (0 decimals) | int64 |
| N | Numeric (with decimals) | float64 |
| T | DateTime | time.Time |
| Y | Currency | float64 |

If you need more information about dbase data types take a look here: [Microsoft Visual Studio Foxpro](https://learn.microsoft.com/en-us/previous-versions/visualstudio/foxpro/74zkxe2k(v=vs.80))

# Installation
``` 
go get github.com/Valentin-Kaiser/go-dbase/dbase
```

# Example

```go
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
```
