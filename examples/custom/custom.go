package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Product struct {
	ID          int32     `dbase:"PRODUCTID"`
	Name        string    `dbase:"PRODNAME"`
	Price       float64   `dbase:"PRICE"`
	Double      float64   `dbase:"DOUBLE"`
	Date        time.Time `dbase:"DATE"`
	DateTime    time.Time `dbase:"DATETIME"`
	Integer     int32     `dbase:"INTEGER"`
	Float       float64   `dbase:"FLOAT"`
	Active      bool      `dbase:"ACTIVE"`
	Description string    `dbase:"DESC"`
	Tax         float64   `dbase:"TAX"`
	Stock       int64     `dbase:"INSTOCK"`
	Blob        []byte    `dbase:"BLOB"`
	Varbinary   []byte    `dbase:"VARBIN_NIL"`
	Varchar     string    `dbase:"VAR_NIL"`
	Var         string    `dbase:"VAR"`
}

func main() {
	// Open debug log file so we see what's going on
	logf, err := os.OpenFile("debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}

	dbase.Debug(true, io.MultiWriter(os.Stdout, logf))

	f, err := os.Open("../test_data/table/TEST.DBF")
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}

	// Open the example database table.
	table, err := dbase.OpenTable(&dbase.Config{TrimSpaces: true}, dbase.GenericIO{Handle: f})
	if err != nil {
		panic(dbase.GetErrorTrace(err))
	}
	defer table.Close()

	fmt.Printf(
		"Last modified: %v Columns count: %v Record count: %v File size: %v \n",
		table.Header().Modified(),
		table.Header().ColumnsCount(),
		table.Header().RecordsCount(),
		table.Header().FileSize(),
	)
}
