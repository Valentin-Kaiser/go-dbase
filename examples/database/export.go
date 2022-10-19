package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

const path = "../test_data/database/EXPENSES.DBC"

const exportPath = "export/"

type DatabaseSchema struct {
	Name      string
	Tables    map[string]string
	Generated time.Duration
}

type Table struct {
	Name      string
	Columns   uint16
	Records   uint32
	FirstRow  int
	RowLength string
	FileSize  int64
	Modified  time.Time
	Fields    map[string]Field
	Data      []map[string]interface{}
}

type Field struct {
	Name   string
	Type   string
	GoType string
	Length int
}

func main() {
	// Open debug log file so we see what's going on
	f, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	dbase.Debug(true, f)

	start := time.Now()
	db, err := dbase.OpenDatabase(&dbase.Config{
		Filename:   path,
		TrimSpaces: true,
	})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}
	defer db.Close()

	schema := db.Schema()
	tables := db.Tables()

	// length := len(tables)
	databaseSchema := DatabaseSchema{
		Name:   strings.Trim(filepath.Base(path), filepath.Ext(path)),
		Tables: make(map[string]string),
	}

	keys := make([]string, 0)
	for table := range schema {
		keys = append(keys, table)
	}
	sort.Strings(keys)

	for it, tablename := range keys {
		fmt.Printf("Exporting table %v (%v/%v)...\n", tablename, it+1, len(keys))
		t := Table{
			Name:     strings.ToUpper(tablename),
			Columns:  tables[tablename].Header().ColumnsCount(),
			Records:  tables[tablename].Header().RecordsCount(),
			FileSize: tables[tablename].Header().FileSize(),
			Modified: tables[tablename].Header().Modified(),
			Fields:   make(map[string]Field),
			Data:     make([]map[string]interface{}, 0),
		}

		for _, field := range schema[tablename] {
			t.Fields[field.Name()] = Field{
				Name:   field.Name(),
				Type:   field.Type(),
				GoType: field.Reflect().String(),
				Length: int(field.Length),
			}
		}

		// Print table data
		rows, err := tables[tablename].Rows(true, true)
		if err != nil {
			panic(err)
		}

		for ir, row := range rows {
			m, err := row.ToMap()
			if err != nil {
				panic(err)
			}
			t.Data = append(t.Data, m)
			fmt.Printf("Exported %v/%v tables %v/%v records \n", it+1, len(keys), ir+1, t.Records)
		}

		// Write table to file
		b, err := json.MarshalIndent(t, "", "  ")
		if err != nil {
			panic(err)
		}
		err = os.WriteFile(fmt.Sprintf("%v%v.json", exportPath, t.Name), b, 0644)
		if err != nil {
			panic(err)
		}

		databaseSchema.Tables[strings.ToUpper(tablename)] = t.Name
		fmt.Printf("Export %v/%v table completed \n", it+1, len(tables))
	}
	duration := time.Since(start)
	databaseSchema.Generated = duration

	// JSON encoding
	b, err := json.MarshalIndent(databaseSchema, "", "    ")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Open schema output file
	schemaFile, err := os.OpenFile(fmt.Sprintf("%v%v.json", exportPath, databaseSchema.Name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	_, err = schemaFile.Write(b)
	if err != nil {
		panic(err)
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
