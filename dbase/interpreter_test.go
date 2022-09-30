package dbase_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

// Test the conversion of the single data types to the corresponding binary representation
func TestValueToData(t *testing.T) {
	dbf := dbase.NewDBF(&dbase.Win1250Converter{})

	// Test (I) int32
	int32Val := int32(1234)
	int32Data, err := dbf.ValueToData(int32Val, &dbase.Column{DataType: []byte("I")[0], Length: 4})
	if err != nil {
		t.Errorf("dbase-interpreter-test-1:FAILED:%v", err)
	}

	if !reflect.DeepEqual(int32Data, []byte{0xd2, 0x04, 0x00, 0x00}) {
		t.Errorf("dbase-interpreter-test-2:FAILED:invalid data %x != %x", int32Data, []byte{0xd2, 0x04, 0x00, 0x00})
	}

	// Test (B) float64
	float64Val := float64(1234.5678)
	float64Data, err := dbf.ValueToData(float64Val, &dbase.Column{DataType: []byte("B")[0], Length: 8})
	if err != nil {
		t.Errorf("dbase-interpreter-test-3:FAILED:%v", err)
	}

	if !reflect.DeepEqual(float64Data, []byte{0xad, 0xfa, 0x5c, 0x6d, 0x45, 0x4a, 0x93, 0x40}) {
		t.Errorf("dbase-interpreter-test-4:FAILED:invalid data %x != %x", float64Data, []byte{0xad, 0xfa, 0x5c, 0x6d, 0x45, 0x4a, 0x93, 0x40})
	}

	// Test (L) bool
	boolVal := true
	boolData, err := dbf.ValueToData(boolVal, &dbase.Column{DataType: []byte("L")[0], Length: 1})
	if err != nil {
		t.Errorf("dbase-interpreter-test-5:FAILED:%v", err)
	}

	if !reflect.DeepEqual(boolData, []byte{0x54}) {
		t.Errorf("dbase-interpreter-test-6:FAILED:invalid data %x != %x", boolData, []byte{0x54})
	}

	boolVal = false
	boolData, err = dbf.ValueToData(boolVal, &dbase.Column{DataType: []byte("L")[0], Length: 1})
	if err != nil {
		t.Errorf("dbase-interpreter-test-7:FAILED:%v", err)
	}

	if !reflect.DeepEqual(boolData, []byte{0x46}) {
		t.Errorf("dbase-interpreter-test-8:FAILED:invalid data %x != %x", boolData, []byte{0x46})
	}

	// Test (N) float64 or int64
	float64Val = float64(1234.5678)
	float64Data, err = dbf.ValueToData(float64Val, &dbase.Column{DataType: []byte("N")[0], Length: 8})
	if err != nil {
		t.Errorf("dbase-interpreter-test-9:FAILED:%v", err)
	}

	if !reflect.DeepEqual(float64Data, []byte{0xad, 0xfa, 0x5c, 0x6d, 0x45, 0x4a, 0x93, 0x40}) {
		t.Errorf("dbase-interpreter-test-10:FAILED:invalid data %x != %x", float64Data, []byte{0xad, 0xfa, 0x5c, 0x6d, 0x45, 0x4a, 0x93, 0x40})
	}

	int64Val := int64(1234)
	int64Data, err := dbf.ValueToData(int64Val, &dbase.Column{DataType: []byte("N")[0], Length: 8})
	if err != nil {
		t.Errorf("dbase-interpreter-test-11:FAILED:%v", err)
	}

	if !reflect.DeepEqual(int64Data, []byte{0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) {
		t.Errorf("dbase-interpreter-test-12:FAILED:invalid data %x != %x", int64Data, []byte{0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	}

	// Test (C) string
	stringVal := "Test"
	stringData, err := dbf.ValueToData(stringVal, &dbase.Column{DataType: []byte("C")[0], Length: 6})
	if err != nil {
		t.Errorf("dbase-interpreter-test-13:FAILED:%v", err)
	}

	if !reflect.DeepEqual(stringData, []byte{0x54, 0x65, 0x73, 0x74, 0x00, 0x00}) {
		t.Errorf("dbase-interpreter-test-14:FAILED:invalid data %x != %x", stringData, []byte{0x54, 0x65, 0x73, 0x74, 0x00, 0x00})
	}

	// Test (D) time.Time
	timeVal := time.Date(2020, 9, 30, 12, 30, 15, 0, time.UTC)
	timeData, err := dbf.ValueToData(timeVal, &dbase.Column{DataType: []byte("D")[0], Length: 8})
	if err != nil {
		t.Errorf("dbase-interpreter-test-15:FAILED:%v", err)
	}

	if !reflect.DeepEqual(timeData, []byte{0xf2, 0x85, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00}) {
		t.Errorf("dbase-interpreter-test-16:FAILED:invalid data %x != %x", timeData, []byte{0xf2, 0x85, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00})
	}

	// Test (T) time.Time
	datetimeVal := time.Date(2020, 9, 30, 12, 30, 15, 0, time.UTC)
	datetimeData, err := dbf.ValueToData(datetimeVal, &dbase.Column{DataType: []byte("T")[0], Length: 8})
	if err != nil {
		t.Errorf("dbase-interpreter-test-17:FAILED:%v", err)
	}

	if !reflect.DeepEqual(datetimeData, []byte{0xf2, 0x85, 0x25, 0x00, 0xd8, 0xdf, 0xae, 0x02}) {
		t.Errorf("dbase-interpreter-test-18:FAILED:invalid data %x != %x", datetimeData, []byte{0xf2, 0x85, 0x25, 0x00, 0xd8, 0xdf, 0xae, 0x02})
	}
}
