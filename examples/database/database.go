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
	dbase.SetOutput(io.MultiWriter(os.Stdout, f))

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
