package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
	duration := time.Since(start)
	length := len(tables)

	fmt.Println("Generating schema...")
	fmt.Printf("Total tables: %v", len(tables))

	// Open schema output file
	schemaFile, err := os.OpenFile("documentation.md", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	// Write file headline
	_, err = schemaFile.WriteString(fmt.Sprintf("## Database documentation \n\n Exracted in %s \n\n", duration))
	if err != nil {
		panic(err)
	}

	keys := make([]string, 0)
	for table := range schema {
		keys = append(keys, table)
	}
	sort.Strings(keys)

	// Print table infos
	_, err = schemaFile.WriteString("| Table | Columns | Records | First record | Row size | File size | Modified |\n|---|---|---|---|---|---|---|\n")
	if err != nil {
		panic(err)
	}

	for _, name := range keys {
		_, err = schemaFile.WriteString(fmt.Sprintf(
			"| [%v](#%v) | %v | %v | %v | %v | %v | %v |\n",
			name,
			name,
			tables[name].Header().ColumnsCount(),
			tables[name].Header().RecordsCount(),
			int(tables[name].Header().FirstRow),
			ToByteString(int(tables[name].Header().RowLength)),
			ToByteString(int(tables[name].Header().FileSize())),
			tables[name].Header().Modified(0),
		))
		if err != nil {
			panic(err)
		}
	}
	_, err = schemaFile.WriteString("\n")
	if err != nil {
		panic(err)
	}

	// Print table schemas
	for i, name := range keys {
		_, err = schemaFile.WriteString(fmt.Sprintf("## %v \n\n", strings.ToUpper(name)))
		if err != nil {
			panic(err)
		}

		_, err = schemaFile.WriteString("| Name | Type | Golang type | Length | Comment | \n| --- | --- | --- | --- | --- | \n")
		if err != nil {
			panic(err)
		}

		for _, column := range schema[name] {
			typ, err := column.Reflect()
			if err != nil {
				panic(err)
			}

			_, err = schemaFile.WriteString(fmt.Sprintf("| *%v* | %v | %v | %v |  | \n", column.Name(), column.Type(), typ, column.Length))
			if err != nil {
				panic(err)
			}
		}
		fmt.Printf("Generated %v/%v table schemas \n", i+1, length)
	}

	// Write table schema
	for _, line := range output {
		_, err = schemaFile.WriteString(line)
		if err != nil {
			panic(err)
		}
	}
}

// ToByteString returns the number of bytes as a string with a unit
func ToByteString(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
