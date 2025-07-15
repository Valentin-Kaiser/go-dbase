package dbase

import (
	"strings"
	"testing"
)

func TestOpenTableIO(t *testing.T) {
	// Test with nil config - this should panic, so we need to handle it
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil config")
		}
	}()
	_, _ = OpenTable(nil)
}

func TestFile_defaults(t *testing.T) {
	file := &File{}

	// Test that defaults sets IO if nil
	if file.io != nil {
		t.Error("Expected io to be nil initially")
	}

	result := file.defaults()
	if result.io == nil {
		t.Error("Expected defaults() to set io")
	}

	if result != file {
		t.Error("Expected defaults() to return the same file instance")
	}
}

func TestFile_GetIO(t *testing.T) {
	mockIO := &GenericIO{}
	file := &File{
		io: mockIO,
	}

	result := file.GetIO()
	if result != mockIO {
		t.Error("Expected GetIO() to return the same IO instance")
	}
}

func TestFile_GetHandle(t *testing.T) {
	handle := "test_handle"
	relatedHandle := "test_related_handle"

	file := &File{
		handle:        handle,
		relatedHandle: relatedHandle,
	}

	h, rh := file.GetHandle()
	if h != handle {
		t.Error("Expected GetHandle() to return the correct handle")
	}
	if rh != relatedHandle {
		t.Error("Expected GetHandle() to return the correct related handle")
	}
}

func TestValidateFileVersion(t *testing.T) {
	// Test with untested = true (should always pass)
	err := ValidateFileVersion(0xFF, true)
	if err != nil {
		t.Errorf("Expected no error when untested=true, got: %v", err)
	}

	// Test with valid FoxPro versions
	validVersions := []byte{
		byte(FoxPro),
		byte(FoxProAutoincrement),
		byte(FoxProVar),
	}

	for _, version := range validVersions {
		err := ValidateFileVersion(version, false)
		if err != nil {
			t.Errorf("Expected no error for valid version %02x, got: %v", version, err)
		}
	}

	// Test with invalid version
	err = ValidateFileVersion(0xFF, false)
	if err == nil {
		t.Error("Expected error for invalid file version")
	}

	// Check error message contains version info
	if err != nil && !strings.Contains(err.Error(), "0xff") {
		t.Errorf("Expected error message to contain version info, got: %s", err.Error())
	}
}

func TestOpenTableIO_InvalidFile(t *testing.T) {
	// Test with invalid filename
	config := &Config{
		Filename: "nonexistent.dbf",
	}
	_, err := OpenTable(config)
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}
