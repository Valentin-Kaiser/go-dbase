package dbase

import "time"

// Containing DBF header information like dBase FileType, last change and rows count.
// https://docs.microsoft.com/en-us/previous-versions/visualstudio/foxpro/st4a0s68(v=vs.80)#table-header-record-structure
type Header struct {
	FileType   byte     // File type flag
	Year       uint8    // Last update year (0-99)
	Month      uint8    // Last update month
	Day        uint8    // Last update day
	RowsCount  uint32   // Number of rows in file
	FirstRow   uint16   // Position of first data row
	RowLength  uint16   // Length of one data row, including delete flag
	Reserved   [16]byte // Reserved
	TableFlags byte     // Table flags
	CodePage   byte     // Code page mark
}

// The raw header of the Memo file.
type MemoHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

// Parses the year, month and day to time.Time.
// The year is stored in decades (2 digits) and added to the base century (2000).
// Note: we assume the year is between 2000 and 2099 as default.
func (h *Header) Modified(base int) time.Time {
	if base == 0 {
		base = 2000
	}
	return time.Date(base+int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.Local)
}

// Returns the calculated number of columns from the header info alone (without the need to read the columninfo from the header).
// This is the fastest way to determine the number of rows in the file.
func (h *Header) ColumnsCount() uint16 {
	return (h.FirstRow - 296) / 32
}

// Returns the amount of records in the table
func (h *Header) RecordsCount() uint32 {
	return h.RowsCount
}

// Returns the calculated file size based on the header info
func (h *Header) FileSize() int64 {
	return 296 + int64(h.ColumnsCount()*32) + int64(h.RowsCount*uint32(h.RowLength))
}
