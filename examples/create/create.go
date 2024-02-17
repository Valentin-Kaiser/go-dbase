package main

import (
	"fmt"
	"io"
	"os"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
	"golang.org/x/text/encoding/charmap"
)

type Test struct {
	ID   int32  `dbase:"ID"`
	Name string `dbase:"NAME"`
	Memo string `dbase:"MEMO"`
	Var  string `dbase:"VAR"`
}

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.Debug(true, io.MultiWriter(os.Stdout, f))

	// When creating a new table you need to define table type
	// For more information about table types see the constants.go file
	file, err := dbase.NewTable(
		dbase.FoxProVar,
		&dbase.Config{
			Filename:   "test.dbf",
			Converter:  dbase.NewDefaultConverter(charmap.Windows1250),
			TrimSpaces: true,
		},
		columns(),
		64,
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		file.Header().Modified(0),
		file.Header().ColumnsCount(),
		file.Header().RecordsCount(),
		file.Header().FileSize(),
	)

	// Print all database column infos.
	for _, column := range file.Columns() {
		fmt.Printf("Name: %v - Type: %v \n", column.Name(), column.Type())
	}

	row, err := file.RowFromStruct(&Test{
		ID:   1,
		Name: "Test",
		Memo: "Memo",
		Var:  "Var",
	})
	if err != nil {
		panic(err)
	}

	err = row.Add()
	if err != nil {
		panic(err)
	}
}

func columns() []*dbase.Column {
	// Integer are allways 4 bytes long
	idCol, err := dbase.NewColumn("ID", dbase.Integer, 0, 0, false)
	if err != nil {
		panic(err)
	}

	// Field name are always saved uppercase
	nameCol, err := dbase.NewColumn("Name", dbase.Character, 20, 0, false)
	if err != nil {
		panic(err)
	}

	// Memo fields need no length the memo block size is defined as last parameter when calling New()
	memoCol, err := dbase.NewColumn("Memo", dbase.Memo, 0, 0, false)
	if err != nil {
		panic(err)
	}

	// Some fields can be null this is defined by the last parameter
	varCol, err := dbase.NewColumn("Var", dbase.Varchar, 64, 0, true)
	if err != nil {
		panic(err)
	}

	return []*dbase.Column{
		idCol,
		nameCol,
		memoCol,
		varCol,
	}
}
