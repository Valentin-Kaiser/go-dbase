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
	dbase.SetOutput(io.MultiWriter(os.Stdout, f))

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
