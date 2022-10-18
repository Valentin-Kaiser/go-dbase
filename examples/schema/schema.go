package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.Debug(true, f)

	db, err := dbase.OpenDatabase(&dbase.Config{
		Filename: "../test_data/database/EXPENSES.DBC",
	})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}
	defer db.Close()

	// Print the database schema
	schema := db.Schema()
	tables := db.Tables()
	length := len(tables)

	fmt.Println("Generating schema...")
	fmt.Printf("Total tables: %v", len(tables))

	// Open schema output file
	schemaFile, err := os.OpenFile("schema.gen.go", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	// Write file headline
	_, err = schemaFile.WriteString("package main \n\n")
	if err != nil {
		panic(err)
	}

	keys := make([]string, 0)
	for table := range schema {
		keys = append(keys, table)
	}
	sort.Strings(keys)

	timeImport := false
	tablesStructs := make([]string, 0)
	for i, name := range keys {
		tableStructSchema := fmt.Sprintf("// Auto generated table struct: %v \n", name)
		tableStructSchema += fmt.Sprintf("type %v struct {\n", strings.ToUpper(name))

		for _, column := range schema[name] {
			if column.DataType == byte(dbase.Date) || column.DataType == byte(dbase.DateTime) {
				timeImport = true
			}
			tableStructSchema += fmt.Sprintf("\t%v %v `json:\"%v\"`\n", column.Name(), column.Reflect(), column.Name())
		}
		tableStructSchema += "}\n\n"
		tablesStructs = append(tablesStructs, tableStructSchema)

		fmt.Printf("Generated %v/%v table schemas \n", i+1, length)
	}

	if timeImport {
		_, err = schemaFile.WriteString("import \"time\"\n\n")
		if err != nil {
			panic(err)
		}
	}

	for _, tableStruct := range tablesStructs {
		_, err = schemaFile.WriteString(tableStruct)
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
