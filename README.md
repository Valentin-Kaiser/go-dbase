# Microsoft Visual FoxPro DBF for Go

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/Valentin-Kaiser/go-dbase)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://github.com/Valentin-Kaiser/go-dbase/blob/main/LICENSE)
[![Linters](https://github.com/Valentin-Kaiser/go-dbase/workflows/Linters/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![CodeQL](https://github.com/Valentin-Kaiser/go-dbase/workflows/CodeQL/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![Examples](https://github.com/Valentin-Kaiser/go-dbase/workflows/Examples/badge.svg)](https://github.com/Valentin-Kaiser/go-dbase)
[![Go Report](https://goreportcard.com/badge/github.com/Valentin-Kaiser/go-dbase)](https://goreportcard.com/report/github.com/Valentin-Kaiser/go-dbase)

**Golang package for reading and writing FoxPro dBase table and memo files.**

## Features 

There are several similar packages but they are not suited for our use case, this package implements the following features:

| Feature | [go-dbase](https://github.com/Valentin-Kaiser/go-dbase) | [go-dbf](https://github.com/LindsayBradford/go-dbf) | [go-foxpro-dbf](https://github.com/SebastiaanKlippert/go-foxpro-dbf) | 
| --- | --- | --- | --- |
| Encoding support ¹ | ✅ | ✅[*](https://github.com/LindsayBradford/go-dbf/issues/3) | ✅ |
| Read | ✅ | ✅ | ✅ |
| Write | ✅  | ✅ | ❌ |
| FPT (memo) file support | ✅ | ❌ | ✅ |
| Struct, json, map conversion | ✅ | ❌ | ✅ |
| IO efficiency ² | ✅ | ❌ | ✅ |
| Full data type support | ✅ | ❌ | ❌ |
| Exclusive Read/Write³ | ✅ | ❌ | ❌ |
| Search  | ✅ | ❌ | ❌ |
| Create new tables, including schema | ✅ | ❌ | ❌ |
| Open database | ✅ | ❌ | ❌ |

> ¹ This package currently supports 13 of the 25 possible encodings, but a universal encoder will be provided for other code pages that can be extended at will. A list of supported encodings can be found [here](#supported-encodings). The conversion in the go-foxpro-dbf package is extensible, but only Windows-1250 as default and the code page is not interpreted. 

> ² IO efficiency is achieved by using one file handle for the DBF file and one file handle for the FPT file. This allows for non blocking IO and the ability to read files while other processes are accessing these. In addition, only the required positions in the file are read instead of keeping a copy of the entire file in memory.

> ³ The files can be opened completely exclusively and when writing a file, the data block to be written can be locked during the process. This is done to prevent other processes from writing the same data block. When reading, this is not a concern as the data is not changed.

> **Disclaimer:** _This library should never be used to develop new software solutions with dbase tables. The creation of new tables only serves to transfer old databases or to remove faulty data._

### Supported column types

At this moment not all FoxPro column types are supported. 
When reading column values, the value returned by this package is always `interface{}`. 
If you need to cast this to the correct value, helper functions are provided.

The supported column types with their return Go types are: 

| Column Type | Column Type Name | Golang type |
|------------|-----------------|-------------|
| C | Character | string |
| Y | Currency | float64 |
| B | Double | float64 |
| D | Date | time.Time |
| T | DateTime | time.Time | 	
| F | Float | float64 |
| I | Integer | int32 |
| L | Logical | bool |
| M | Memo  | string |
| M | Memo (Binary) | []byte |
| N | Numeric (0 decimals) | int64 |
| N | Numeric (with decimals) | float64 |
| Q | Varbinary | []byte |
| V | Varchar | []byte |
| W | Blob | []byte |
| G | General | []byte |
| P | Picture | []byte |


> If you need more information about dbase data types take a look here: [Microsoft Visual Studio Foxpro](https://learn.microsoft.com/en-us/previous-versions/visualstudio/foxpro/74zkxe2k(v=vs.80))

### Supported encodings

The following encodings are supported by this package:

| Code page | Platform | Code page identifier |
| --- | --- | --- |
| 437 | U.S. MS-DOS | x01 |
| 850 | International MS-DOS | x02 | 
| 852 | Eastern European MS-DOS	| x64| 
| 865 | Nordic MS-DOS | x66 | 
| 866 | Russian MS-DOS | x65 | 
| 874 | Thai Windows | x7C | 
| 1250 | Central European Windows | xC8 | 
| 1251 | Russian Windows | xC9 | 
| 1252 | Windows ANSI | x03 | 
| 1253 | Greek Windows	| xCB | 
| 1254 | Turkish Windows| xCA | 
| 1255 | Hebrew Windows | x7D | 
| 1256 | Arabic Windows	| x7E | 


> All encodings are converted from and to UTF-8.

## Installation
``` 
go get github.com/Valentin-Kaiser/go-dbase/dbase
```

## Projects

Projects using this package:

[👻 G(h)oST](https://github.com/Plaenkler/GoST)

## Examples

<details open>
  <summary>Read</summary>
  
```go
package main

import (
	"fmt"
	"io"
	"os"
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
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.SetDebug(true)
	dbase.SetDebugOutput(io.MultiWriter(os.Stdout, f))

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename:   "../test_data/table/TEST.DBF",
		TrimSpaces: true,
	})
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(),
		table.Header().ColumnsCount(),
		table.Header().RecordsCount(),
		table.Header().FileSize(),
	)

	// Print all database column infos.
	for _, column := range table.Columns() {
		fmt.Printf("Name: %v - Type: %v \n", column.Name(), column.Type())
	}

	// Loop through all rows using rowPointer in DBF struct.
	for !table.EOF() {
		row, err := table.Row()
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		// Increment the row pointer.
		table.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}

		// Get the first field by column position
		field := row.Field(0)
		if field == nil {
			panic("Field not found")
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.GetValue())

		// Get value by column name
		field = row.FieldByName("PRODNAME")
		if field == nil {
			panic("Field not found")
		}

		// Print the field value.
		fmt.Printf("Field: %v [%v] => %v \n", field.Name(), field.Type(), field.GetValue())

		// === Modifications ===

		// Disable space trimming for the company name
		table.SetColumnModificationByName("PRODNAME", &dbase.Modification{TrimSpaces: false})
		// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
		table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
		table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})

		// === Struct Conversion ===

		// Read the row into a struct.
		p := &Product{}
		err = row.ToStruct(p)
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		fmt.Printf("Product: %v \n", p.Name)
	}
}
```
</details>

<details open>
  <summary>Write</summary>

```go
package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Product struct {
	ID          int32     `json:"PRODUCTID"`
	Name        string    `json:"PRODNAME"`
	Price       float64   `json:"PRICE"`
	Tax         float64   `json:"TAX"`
	Stock       int64     `json:"INSTOCK"`
	Date        time.Time `json:"DATE"`
	DateTime    time.Time `json:"DATETIME"`
	Description string    `json:"DESC"`
	Active      bool      `json:"ACTIVE"`
	Float       float64   `json:"FLOAT"`
	Integer     int64     `json:"INTEGER"`
	Double      float64   `json:"DOUBLE"`
}

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.SetDebug(true)
	dbase.SetDebugOutput(io.MultiWriter(os.Stdout, f))

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename:   "../test_data/table/TEST.DBF",
		TrimSpaces: true,
		WriteLock:  true,
	})
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(),
		table.Header().ColumnsCount(),
		table.Header().RecordsCount(),
		table.Header().FileSize(),
	)

	// Read the first row (rowPointer start at the first row).
	row, err := table.Row()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Get the company name field by column name.
	err = row.FieldByName("PRODNAME").SetValue("CHANGED_PRODUCT_NAME")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Change a memo field value.
	err = row.FieldByName("DESC").SetValue("MEMO_TEST_VALUE")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Write the changed row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// === Modifications ===

	// Add a column modification to switch the names of "INTEGER" and "Float" to match the data types
	table.SetColumnModificationByName("INTEGER", &dbase.Modification{TrimSpaces: true, ExternalKey: "FLOAT"})
	table.SetColumnModificationByName("FLOAT", &dbase.Modification{TrimSpaces: true, ExternalKey: "INTEGER"})

	// Create a new row with the same structure as the database table.
	p := Product{
		ID:          99,
		Name:        "NEW_PRODUCT",
		Price:       99.99,
		Tax:         19.99,
		Stock:       999,
		Date:        time.Now(),
		DateTime:    time.Now(),
		Description: "NEW_PRODUCT_DESCRIPTION",
		Active:      true,
		Float:       105.67,
		Integer:     104,
		Double:      103.45,
	}

	row, err = table.RowFromStruct(p)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Add the new row to the database table.
	err = row.Write()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Print all rows.
	for !table.EOF() {
		row, err := table.Row()
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		// Increment the row pointer.
		table.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}

		// Print the current row values.
		fmt.Println(row.Values()...)
	}
}
```
</details>

<details open>
  <summary>Database</summary>

```go
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
	dbase.SetDebug(true)
	dbase.SetDebugOutput(io.MultiWriter(os.Stdout, f))

	db, err := dbase.OpenDatabase(&dbase.Config{
		Filename:   "../test_data/database/EXPENSES.DBC",
		TrimSpaces: true,
	})
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer db.Close()

	tables := db.Tables()
	for name, table := range tables {
		fmt.Printf(
			"Table: %v Last modified: %v Columns count: %v Record count: %v File size: %v First Row: %v Length: %v \n",
			name,
			table.Header().Modified(),
			table.Header().ColumnsCount(),
			table.Header().RecordsCount(),
			table.Header().FileSize(),
			table.Header().FirstRow,
			table.Header().RowLength,
		)
	}

	// Print the database schema
	for name, columns := range db.Schema() {
		fmt.Printf("# Table: %v  \n", name)
		for _, column := range columns {
			fmt.Printf("	=> Column: %v Data Type: %v Length: %v \n", column.Name(), column.Type(), column.Length)
		}
	}
}
```
</details>

<details open>
  <summary>Create</summary>
  
```go
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
	"golang.org/x/text/encoding/charmap"
)

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.SetDebug(true)
	dbase.SetDebugOutput(io.MultiWriter(os.Stdout, f))

	// Integer are allways 4 bytes long
	idCol, err := dbase.NewColumn("ID", dbase.Integer, 0, 0, false)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Field name are always saved uppercase
	nameCol, err := dbase.NewColumn("Name", dbase.Character, 20, 0, false)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Memo fields need no length the memo block size is defined as last parameter when calling New()
	memoCol, err := dbase.NewColumn("Memo", dbase.Memo, 0, 0, false)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Some fields can be null this is defined by the last parameter
	varCol, err := dbase.NewColumn("Var", dbase.Varchar, 64, 0, true)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// When creating a new table you need to define table type
	// For more information about table types see the constants.go file
	file, err := dbase.New(
		dbase.FoxProVar,
		&dbase.Config{
			Filename:   "test.dbf",
			Converter:  dbase.NewDefaultConverter(charmap.Windows1250),
			TrimSpaces: true,
		},
		[]*dbase.Column{
			idCol,
			nameCol,
			memoCol,
			varCol,
		},
		64,
	)
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer file.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		file.Header().Modified(),
		file.Header().ColumnsCount(),
		file.Header().RecordsCount(),
		file.Header().FileSize(),
	)

	// Print all database column infos.
	for _, column := range file.Columns() {
		fmt.Printf("Name: %v - Type: %v \n", column.Name(), column.Type())
	}

	// Write a new record
	row := file.NewRow()

	err = row.FieldByName("ID").SetValue(int32(1))
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	err = row.FieldByName("NAME").SetValue("TOTALLY_NEW_ROW")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	err = row.FieldByName("MEMO").SetValue("This is a memo field")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	err = row.FieldByName("VAR").SetValue("This is a varchar field")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	err = row.Add()
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Read all records
	for !file.EOF() {
		row, err := file.Row()
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		// Increment the row pointer.
		file.Skip(1)

		// Skip deleted rows.
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}

		name, err := row.ValueByName("NAME")
		if err != nil {
			panic(dbase.ErrorDetails(err))
		}

		fmt.Printf("Row at position: %v => %v \n", row.Position, name)
	}
}
```
</details>


<details open>
  <summary>Search</summary>
  
```go
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
	dbase.SetDebug(true)
	dbase.SetDebugOutput(io.MultiWriter(os.Stdout, f))

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{
		Filename: "../test_data/table/TEST.DBF",
	})
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(),
		table.Header().ColumnsCount(),
		table.Header().RecordsCount(),
		table.Header().FileSize(),
	)

	// Init the field we want to search for.
	// Search for a product containing the word "test" in the name.
	field, err := table.NewFieldByName("PRODNAME", "TEST")
	if err != nil {
		panic(dbase.ErrorDetails(err))
	}

	// Execute the search with an exact match.
	records, err := table.Search(field, false)
	if err != nil {
		panic(dbase.ErrorDetails(err))
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
		panic(dbase.ErrorDetails(err))
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
```
</details>
