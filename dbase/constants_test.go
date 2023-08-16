package dbase

import (
	"reflect"
	"testing"
	"time"
)

func TestDataTypeString(t *testing.T) {
	dataTypes := []DataType{
		Character, Currency, Double, Date, DateTime, Float,
		Integer, Logical, Memo, Numeric, Blob, General, Picture, Varbinary, Varchar,
	}
	expectedResults := []string{
		"C", "Y", "B", "D", "T", "F", "I", "L", "M", "N", "W", "G", "P", "Q", "V",
	}
	for i, dt := range dataTypes {
		if result := dt.String(); result != expectedResults[i] {
			t.Errorf("Expected %s, got %s for DataType %v", expectedResults[i], result, dt)
		}
	}
}

func TestDataTypeReflect(t *testing.T) {
	cases := []struct {
		input    DataType
		expected reflect.Type
		isError  bool
	}{
		{Character, reflect.TypeOf(""), false},
		{Currency, reflect.TypeOf(float64(0)), false},
		{Double, reflect.TypeOf(float64(0)), false},
		{Date, reflect.TypeOf(time.Time{}), false},
		{DateTime, reflect.TypeOf(time.Time{}), false},
		{Float, reflect.TypeOf(float64(0)), false},
		{Integer, reflect.TypeOf(int32(0)), false},
		{Logical, reflect.TypeOf(false), false},
		{Memo, reflect.TypeOf([]byte{}), false},
		{Blob, reflect.TypeOf([]byte{}), false},
		{Varchar, reflect.TypeOf([]byte{}), false},
		{Varbinary, reflect.TypeOf([]byte{}), false},
		{General, reflect.TypeOf([]byte{}), false},
		{Picture, reflect.TypeOf([]byte{}), false},
		{Numeric, reflect.TypeOf(float64(0)), false},
		{DataType(0xFF), nil, true}, // Invalid DataType to test error case
	}
	for _, c := range cases {
		result, err := c.input.Reflect()
		if (err != nil) != c.isError {
			t.Errorf("Expected error=%v, got error=%v for DataType %v", c.isError, (err != nil), c.input)
			continue
		}
		if err == nil && result != c.expected {
			t.Errorf("Expected %v, got %v for DataType %v", c.expected, result, c.input)
		}
	}
}
