package main

import "time"

// Auto generated table struct: employees
type EMPLOYEES struct {
	EMPLOYEEID int32   `dbase:"EMPLOYEEID"`
	DEPARTMENT string  `dbase:"DEPARTMENT"`
	SOCIALSECU string  `dbase:"SOCIALSECU"`
	EMPLOYEENU string  `dbase:"EMPLOYEENU"`
	FIRSTNAME  string  `dbase:"FIRSTNAME"`
	LASTNAME   string  `dbase:"LASTNAME"`
	TITLE      string  `dbase:"TITLE"`
	EMAILNAME  string  `dbase:"EMAILNAME"`
	EXTENSION  string  `dbase:"EXTENSION"`
	ADDRESS    []uint8 `dbase:"ADDRESS"`
	CITY       string  `dbase:"CITY"`
	STATEORPRO string  `dbase:"STATEORPRO"`
	POSTALCODE string  `dbase:"POSTALCODE"`
	COUNTRY    string  `dbase:"COUNTRY"`
	WORKPHONE  string  `dbase:"WORKPHONE"`
	NOTES      []uint8 `dbase:"NOTES"`
}

// Auto generated table struct: expense_categories
type EXPENSE_CATEGORIES struct {
	EXPENSECAT int32  `dbase:"EXPENSECAT"`
	EXPENSECA2 string `dbase:"EXPENSECA2"`
	EXPENSECA3 int32  `dbase:"EXPENSECA3"`
}

// Auto generated table struct: expense_details
type EXPENSE_DETAILS struct {
	EXPENSEDET int32     `dbase:"EXPENSEDET"`
	EXPENSEREP int32     `dbase:"EXPENSEREP"`
	EXPENSECAT int32     `dbase:"EXPENSECAT"`
	EXPENSEITE float64   `dbase:"EXPENSEITE"`
	EXPENSEIT2 string    `dbase:"EXPENSEIT2"`
	EXPENSEDAT time.Time `dbase:"EXPENSEDAT"`
}

// Auto generated table struct: expense_reports
type EXPENSE_REPORTS struct {
	EXPENSEREP int32     `dbase:"EXPENSEREP"`
	EMPLOYEEID int32     `dbase:"EMPLOYEEID"`
	EXPENSETYP string    `dbase:"EXPENSETYP"`
	EXPENSERPT string    `dbase:"EXPENSERPT"`
	EXPENSERP2 []uint8   `dbase:"EXPENSERP2"`
	DATESUBMIT time.Time `dbase:"DATESUBMIT"`
	ADVANCEAMO float64   `dbase:"ADVANCEAMO"`
	DEPARTMENT string    `dbase:"DEPARTMENT"`
	PAID       bool      `dbase:"PAID"`
}
