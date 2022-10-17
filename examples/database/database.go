package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.Debug(true, io.MultiWriter(os.Stdout, f))

	start := time.Now()
	db, err := dbase.OpenDatabase(&dbase.Config{
		Filename: "../test_data/database/EXPENSES.DBC",
	})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}
	defer db.Close()

	// Print the database schema
	output := make([]string, 0)
	schema := db.Schema()
	tables := db.Tables()
	keys := make([]string, 0)
	for table := range schema {
		keys = append(keys, table)
	}
	sort.Strings(keys)
	for _, name := range keys {
		output = append(output, fmt.Sprintf("## %v \n\n", name))
		output = append(output, fmt.Sprintf(
			"- Fields: `%v` \n- Records: `%v` \n- File size: `%v B`  \n- First Row at: `%v B`  \n- Record Length: `%v` \n- Last modified: `%v` \n\n",
			tables[name].Header().ColumnsCount(),
			tables[name].Header().RecordsCount(),
			tables[name].Header().FileSize(),
			tables[name].Header().FirstRow,
			tables[name].Header().RowLength,
			tables[name].Header().Modified(),
		))
		output = append(output, "| Field | Type | Length | \n")
		output = append(output, "| --- | --- | --- | \n")
		for _, column := range schema[name] {
			output = append(output, fmt.Sprintf("| *%v* | %v | %v | \n", column.Name(), column.Type(), column.Length))
		}
		output = append(output, "\n")
	}
	duration := time.Since(start)

	// Open schema output file
	schemaFile, err := os.OpenFile("schema.md", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	// Write file headline
	_, err = schemaFile.WriteString(fmt.Sprintf("## Database schema \n\n Generated in %v \n\n", duration))
	if err != nil {
		panic(err)
	}

	// Write schema
	for _, line := range output {
		_, err = schemaFile.WriteString(line)
		if err != nil {
			panic(err)
		}
	}
}
