package dbase

import (
	"reflect"
	"testing"
	"time"
)

func TestFileVersion_Constants(t *testing.T) {
	// Test that file version constants have expected values
	expectedValues := map[FileVersion]byte{
		FoxPro:              0x30,
		FoxProAutoincrement: 0x31,
		FoxProVar:           0x32,
		FoxBase:             0x02,
		FoxBase2:            0xFB,
		FoxBasePlus:         0x03,
		DBaseSQLTable:       0x43,
		FoxBasePlusMemo:     0x83,
		DBaseMemo:           0x8B,
		DBaseSQLMemo:        0xCB,
		FoxPro2Memo:         0xF5,
	}

	for version, expected := range expectedValues {
		if byte(version) != expected {
			t.Errorf("Expected FileVersion %v to have value %02x, got %02x", version, expected, byte(version))
		}
	}
}

func TestFileExtension_Constants(t *testing.T) {
	// Test that file extension constants have expected values
	expectedValues := map[FileExtension]string{
		DBC: ".DBC",
		DCT: ".DCT",
		DBF: ".DBF",
		FPT: ".FPT",
		SCX: ".SCX",
		LBX: ".LBX",
		MNX: ".MNX",
		PJX: ".PJX",
		RPX: ".RPX",
		VCX: ".VCX",
	}

	for ext, expected := range expectedValues {
		if string(ext) != expected {
			t.Errorf("Expected FileExtension %v to have value %s, got %s", ext, expected, string(ext))
		}
	}
}

func TestDataType_Reflect(t *testing.T) {
	testCases := []struct {
		dataType     DataType
		expectedType reflect.Type
	}{
		{Character, reflect.TypeOf("")},
		{Date, reflect.TypeOf(time.Time{})},
		{Float, reflect.TypeOf(float64(0))},
		{Integer, reflect.TypeOf(int32(0))},
		{Logical, reflect.TypeOf(false)},
		{Memo, reflect.TypeOf([]byte{})},
		{Numeric, reflect.TypeOf(float64(0))},
		{Double, reflect.TypeOf(float64(0))},
		{Currency, reflect.TypeOf(float64(0))},
		{DateTime, reflect.TypeOf(time.Time{})},
		{Varchar, reflect.TypeOf([]byte{})},
		{Varbinary, reflect.TypeOf([]byte{})},
		{Blob, reflect.TypeOf([]byte{})},
		{General, reflect.TypeOf([]byte{})},
		{Picture, reflect.TypeOf([]byte{})},
	}

	for _, tc := range testCases {
		result, err := tc.dataType.Reflect()
		if err != nil {
			t.Errorf("Unexpected error for DataType %v: %v", tc.dataType, err)
			continue
		}

		if result != tc.expectedType {
			t.Errorf("For DataType %v, expected type %v, got %v", tc.dataType, tc.expectedType, result)
		}
	}
}

func TestDataType_Reflect_Unknown(t *testing.T) {
	// Test with unknown data type
	unknownType := DataType(255)
	_, err := unknownType.Reflect()
	if err == nil {
		t.Error("Expected error for unknown data type")
	}
}

func TestDataType_String(t *testing.T) {
	testCases := map[DataType]string{
		Character: "C",
		Date:      "D",
		Float:     "F",
		Integer:   "I",
		Logical:   "L",
		Memo:      "M",
		Numeric:   "N",
		Double:    "B",
		Currency:  "Y",
		DateTime:  "T",
		Varchar:   "V",
		Varbinary: "Q",
		Blob:      "W",
		General:   "G",
		Picture:   "P",
	}

	for dataType, expected := range testCases {
		result := dataType.String()
		if result != expected {
			t.Errorf("For DataType %v, expected string %s, got %s", dataType, expected, result)
		}
	}
}

func TestDataType_String_Unknown(t *testing.T) {
	// Test with unknown data type
	unknownType := DataType(255)
	result := unknownType.String()
	expected := string(byte(255)) // Should return the character representation
	if result != expected {
		t.Errorf("Expected %s for unknown data type, got %s", expected, result)
	}
}

func TestColumnFlag_Constants(t *testing.T) {
	// Test that column flag constants exist and have reasonable values
	flags := []ColumnFlag{
		HiddenFlag,
		NullableFlag,
		BinaryFlag,
		AutoincrementFlag,
	}

	expectedValues := map[ColumnFlag]byte{
		HiddenFlag:        0x01,
		NullableFlag:      0x02,
		BinaryFlag:        0x04,
		AutoincrementFlag: 0x0C,
	}

	for _, flag := range flags {
		expected := expectedValues[flag]
		if byte(flag) != expected {
			t.Errorf("Expected flag %v to have value %02x, got %02x", flag, expected, byte(flag))
		}
	}
}

func TestMarker_Constants(t *testing.T) {
	// Test that marker constants exist
	expectedValues := map[string]struct {
		marker Marker
		value  byte
	}{
		"Null":      {Null, 0x00},
		"Blank":     {Blank, 0x20},
		"ColumnEnd": {ColumnEnd, 0x0D},
		"Active":    {Active, 0x20}, // Same as Blank
		"Deleted":   {Deleted, 0x2A},
		"EOFMarker": {EOFMarker, 0x1A},
	}

	for name, expected := range expectedValues {
		if byte(expected.marker) != expected.value {
			t.Errorf("Expected marker %s to have value %02x, got %02x", name, expected.value, byte(expected.marker))
		}
	}

	// Test that Active is the same as Blank
	if Active != Blank {
		t.Error("Active marker should be the same as Blank marker")
	}
}

func TestTableFlag_Constants(t *testing.T) {
	flags := map[TableFlag]byte{
		StructuralFlag: 0x01,
		MemoFlag:       0x02,
		DatabaseFlag:   0x04,
	}

	for flag, expectedValue := range flags {
		if byte(flag) != expectedValue {
			t.Errorf("Expected table flag %v to have value %02x, got %02x", flag, expectedValue, byte(flag))
		}
	}
}

func TestTableFlag_Defined(t *testing.T) {
	// Test the Defined method
	// The Defined method checks if the current flag is defined in the provided byte
	// So we test with the combined flag value
	combinedValue := byte(MemoFlag | DatabaseFlag) // 0x06

	if !MemoFlag.Defined(combinedValue) {
		t.Error("Expected MemoFlag to be defined in combined flag")
	}

	if !DatabaseFlag.Defined(combinedValue) {
		t.Error("Expected DatabaseFlag to be defined in combined flag")
	}

	if StructuralFlag.Defined(combinedValue) {
		t.Error("Expected StructuralFlag not to be defined in combined flag")
	}
}
