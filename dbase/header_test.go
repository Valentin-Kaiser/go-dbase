package dbase

import (
	"testing"
	"time"
)

func TestHeaderModified(t *testing.T) {
	// Test with known values
	header := &Header{
		Year:  23, // 2023
		Month: 12,
		Day:   25,
	}

	// Test with base year 2000
	modifiedDate := header.Modified(2000)
	expected := time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC)

	if modifiedDate.Year() != expected.Year() {
		t.Errorf("Expected year %d, got %d", expected.Year(), modifiedDate.Year())
	}

	if modifiedDate.Month() != expected.Month() {
		t.Errorf("Expected month %d, got %d", expected.Month(), modifiedDate.Month())
	}

	if modifiedDate.Day() != expected.Day() {
		t.Errorf("Expected day %d, got %d", expected.Day(), modifiedDate.Day())
	}
}

func TestHeaderModifiedWithDifferentBase(t *testing.T) {
	// Test with different base year
	header := &Header{
		Year:  50, // Should be 1950 with base 1900
		Month: 6,
		Day:   15,
	}

	modifiedDate := header.Modified(1900)
	expected := time.Date(1950, 6, 15, 0, 0, 0, 0, time.UTC)

	if modifiedDate.Year() != expected.Year() {
		t.Errorf("Expected year %d, got %d", expected.Year(), modifiedDate.Year())
	}

	if modifiedDate.Month() != expected.Month() {
		t.Errorf("Expected month %d, got %d", expected.Month(), modifiedDate.Month())
	}

	if modifiedDate.Day() != expected.Day() {
		t.Errorf("Expected day %d, got %d", expected.Day(), modifiedDate.Day())
	}
}

func TestHeaderModifiedEdgeCases(t *testing.T) {
	// Test with invalid values
	header := &Header{
		Year:  99,
		Month: 0,  // Invalid month
		Day:   32, // Invalid day
	}

	// Should not panic, but may return invalid date
	modifiedDate := header.Modified(2000)
	if modifiedDate.IsZero() {
		t.Error("Modified date should not be zero even with invalid input")
	}
}

func TestHeaderStructure(t *testing.T) {
	// Test header struct has expected fields
	header := &Header{
		FileType:   0x30,
		Year:       23,
		Month:      12,
		Day:        25,
		RowsCount:  100,
		FirstRow:   264,
		RowLength:  50,
		TableFlags: 0x00,
		CodePage:   0x03,
	}

	if header.FileType != 0x30 {
		t.Errorf("Expected FileType 0x30, got 0x%x", header.FileType)
	}

	if header.RowsCount != 100 {
		t.Errorf("Expected RowsCount 100, got %d", header.RowsCount)
	}

	if header.FirstRow != 264 {
		t.Errorf("Expected FirstRow 264, got %d", header.FirstRow)
	}

	if header.RowLength != 50 {
		t.Errorf("Expected RowLength 50, got %d", header.RowLength)
	}

	if header.CodePage != 0x03 {
		t.Errorf("Expected CodePage 0x03, got 0x%x", header.CodePage)
	}
}

func TestMemoHeaderStructure(t *testing.T) {
	// Test memo header struct has expected fields
	memoHeader := &MemoHeader{
		NextFree:  1024,
		BlockSize: 512,
	}

	if memoHeader.NextFree != 1024 {
		t.Errorf("Expected NextFree 1024, got %d", memoHeader.NextFree)
	}

	if memoHeader.BlockSize != 512 {
		t.Errorf("Expected BlockSize 512, got %d", memoHeader.BlockSize)
	}
}