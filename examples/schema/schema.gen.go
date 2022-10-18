package main

import "time"

// Auto generated table struct: employees
type EMPLOYEES struct {
	EMPLOYEEID int32   `json:"EMPLOYEEID"`
	DEPARTMENT string  `json:"DEPARTMENT"`
	SOCIALSECU string  `json:"SOCIALSECU"`
	EMPLOYEENU string  `json:"EMPLOYEENU"`
	FIRSTNAME  string  `json:"FIRSTNAME"`
	LASTNAME   string  `json:"LASTNAME"`
	TITLE      string  `json:"TITLE"`
	EMAILNAME  string  `json:"EMAILNAME"`
	EXTENSION  string  `json:"EXTENSION"`
	ADDRESS    []uint8 `json:"ADDRESS"`
	CITY       string  `json:"CITY"`
	STATEORPRO string  `json:"STATEORPRO"`
	POSTALCODE string  `json:"POSTALCODE"`
	COUNTRY    string  `json:"COUNTRY"`
	WORKPHONE  string  `json:"WORKPHONE"`
	NOTES      []uint8 `json:"NOTES"`
}

// Auto generated table struct: expense_categories
type EXPENSE_CATEGORIES struct {
	EXPENSECAT int32  `json:"EXPENSECAT"`
	EXPENSECA2 string `json:"EXPENSECA2"`
	EXPENSECA3 int32  `json:"EXPENSECA3"`
}

// Auto generated table struct: expense_details
type EXPENSE_DETAILS struct {
	EXPENSEDET int32     `json:"EXPENSEDET"`
	EXPENSEREP int32     `json:"EXPENSEREP"`
	EXPENSECAT int32     `json:"EXPENSECAT"`
	EXPENSEITE float64   `json:"EXPENSEITE"`
	EXPENSEIT2 string    `json:"EXPENSEIT2"`
	EXPENSEDAT time.Time `json:"EXPENSEDAT"`
}

// Auto generated table struct: expense_reports
type EXPENSE_REPORTS struct {
	EXPENSEREP int32     `json:"EXPENSEREP"`
	EMPLOYEEID int32     `json:"EMPLOYEEID"`
	EXPENSETYP string    `json:"EXPENSETYP"`
	EXPENSERPT string    `json:"EXPENSERPT"`
	EXPENSERP2 []uint8   `json:"EXPENSERP2"`
	DATESUBMIT time.Time `json:"DATESUBMIT"`
	ADVANCEAMO float64   `json:"ADVANCEAMO"`
	DEPARTMENT string    `json:"DEPARTMENT"`
	PAID       bool      `json:"PAID"`
}
