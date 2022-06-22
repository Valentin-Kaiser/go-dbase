# go-dbase

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
	ID_NR       int32     `json:"ID_NR"`
	UserNR      int32     `json:"USERNR"`
	CompanyName string    `json:"COMP_NAME"`
	CompanyOS   string    `json:"COMP_OS"`
	Melding     string    `json:"MELDING"`
	Number      float64   `json:"NUMBER"`
	Float       int64     `json:"FLOAT"`
	Bool        bool      `json:"BOOL"`
}

func main() {
	// Open file
	dbf, err := dbase.Open("./test_data/TEST.DBF", new(dbase.Win1250Converter))
	if err != nil {
		panic(err)
	}
	defer dbf.Close()

	// Print all the columnnames
	for _, name := range dbf.ColumnNames() {
		fmt.Println(name)
	}

	fmt.Println("--- database file columns --- \n")

	// Get columninfo for all columns
	for _, column := range dbf.Columns() {
		fmt.Println(column.ColumnName(), column.ColumnType(), column.Decimals)
	}

	err = dbf.GoTo(1)
	if err != nil {
		panic(err)
	}

	// Read the complete second row
	row, err := dbf.GetRow()
	if err != nil {
		panic(err)
	}

	fmt.Println("--- database row as slice --- \n")

	// Print all the columns in their Go values
	fmt.Println(row.ColumnSlice())

	// Go back to start
	err = dbf.GoTo(0)
	if err != nil {
		panic(err)
	}

	// Loop through all rows using rowPointer in DBF struct
	// Reads the complete row
	for !dbf.EOF() {
		// This reads the complete row
		row, err := dbf.GetRow()
		if err != nil {
			panic(err)
		}

		dbf.Skip(1)
		// skip deleted rows
		if row.Deleted {
			continue
		}

		// get column by position
		_, err = row.Column(0)
		if err != nil {
			panic(err)
		}

		// get column by name
		_, err = row.Column(dbf.ColumnPos("COMP_NAME"))
		if err != nil {
			panic(err)
		}

		fmt.Println("\n --- converted to struct --- \n")

		// convert row into struct
		t := &Test{}
		err = row.ToStruct(t, true)
		if err != nil {
			panic(err)
		}
		fmt.Printf("TESTDATA Company: %+v \n", t.CompanyName)
	}

	fmt.Println("\n --- row specific column --- \n")
	// Read only the third column of rows 1, 2 and 3
	recnumbers := []uint32{1, 2, 3}
	for _, rec := range recnumbers {
		err := dbf.GoTo(rec)
		if err != nil {
			panic(err)
		}

		deleted, err := dbf.Deleted()
		if err != nil {
			panic(err)
		}

		if deleted {
			fmt.Printf("Row %v deleted \n", rec)
			continue
		}

		column3, err := dbf.Column(3)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Row %v column 3: %v \n", rec, column3)

	}
}

```