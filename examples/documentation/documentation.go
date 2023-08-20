package main

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type TableInfo struct {
	Name        string
	Columns     uint16
	Records     uint32
	FirstRecord uint16
	RowSize     uint16
	FileSize    int64
	Modified    time.Time
	ColumnsInfo []ColumnInfo
}

type ColumnInfo struct {
	Name       string
	Type       string
	GolangType reflect.Type
	Length     uint8
	Comment    string
}

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

	schema := db.Schema()
	tables := db.Tables()

	fmt.Println("Generating schema...")
	fmt.Printf("Total tables: %v", len(tables))

	tableInfos := make([]TableInfo, 0)
	for name := range schema {
		tableInfos = append(tableInfos, TableInfo{
			Name:        name,
			Columns:     tables[name].Header().ColumnsCount(),
			Records:     tables[name].Header().RecordsCount(),
			FirstRecord: tables[name].Header().FirstRow,
			RowSize:     tables[name].Header().RowLength,
			FileSize:    tables[name].Header().FileSize(),
			Modified:    tables[name].Header().Modified(0),
			ColumnsInfo: make([]ColumnInfo, 0),
		})

		for _, column := range schema[name] {
			typ, err := column.Reflect()
			if err != nil {
				panic(err)
			}

			tableInfos[len(tableInfos)-1].ColumnsInfo = append(tableInfos[len(tableInfos)-1].ColumnsInfo, ColumnInfo{
				Name:       column.Name(),
				Type:       column.Type(),
				GolangType: typ,
				Length:     column.Length,
			})
		}
	}
	duration := time.Since(start)

	// Open schema output file
	schemaFile, err := os.OpenFile("documentation.md", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	output := []string{
		fmt.Sprintf("## Database documentation \n\n Exracted in %s \n\n", duration),
		"| Table | Columns | Records | First record | Row size | File size | Modified |\n|---|---|---|---|---|---|---|\n",
	}
	for _, info := range tableInfos {
		output = append(output, info.String())
	}

	output = append(output, "\n\n## Columns\n\n")
	for _, info := range tableInfos {
		output = append(output, fmt.Sprintf("### %v\n\n", info.Name))
		output = append(output, "| Column | Type | Length | Comment |\n|---|---|---|---|\n")
		for _, column := range info.ColumnsInfo {
			output = append(output, column.String())
		}
	}

	_, err = schemaFile.WriteString(strings.Join(output, ""))
	if err != nil {
		panic(err)
	}
}

func (t TableInfo) String() string {
	return fmt.Sprintf(
		"| [%v](#%v) | %v | %v | %v | %v | %v | %v |\n",
		t.Name,
		strings.ToLower(t.Name),
		t.Columns,
		t.Records,
		t.FirstRecord,
		t.RowSize,
		ToByteString(int(t.FileSize)),
		t.Modified.Format("2006-01-02 15:04:05"),
	)
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf(
		"| %v | %v | %v | %v |\n",
		c.Name,
		c.Type,
		c.Length,
		c.Comment,
	)
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
