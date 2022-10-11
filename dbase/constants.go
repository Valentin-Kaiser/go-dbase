package dbase

type DataType byte

const (
	// Supported and testet file types - other file types may work but are not tested
	// The file type check has to be bypassed when opening, if the file type is not supported
	FoxPro              byte = 0x30
	FoxProAutoincrement byte = 0x31
	// Relevant byte marker
	Null      byte = 0x00
	Blank     byte = 0x20
	ColumnEnd byte = 0x0D
	Active         = Blank
	Deleted        = 0x2A
	EOFMarker byte = 0x1A
	// DBase Table flags
	StructuralFlag     byte = 0x01
	MemoFlag           byte = 0x02
	StructuralMemoFlag byte = 0x03
	DatabaseFlag       byte = 0x04
	// Data types
	Character DataType = 0x43
	Currency  DataType = 0x59
	Double    DataType = 0x42
	Date      DataType = 0x44
	DateTime  DataType = 0x54
	Float     DataType = 0x46
	Integer   DataType = 0x49
	Logical   DataType = 0x4C
	Memo      DataType = 0x4D
	Numeric   DataType = 0x4E
	Blob      DataType = 0x57
	General   DataType = 0x47
	Picture   DataType = 0x50
	Varbinary DataType = 0x51
	Varchar   DataType = 0x56
)

// Returns the type of the column as string (length 1)
func (t DataType) String() string {
	return string(t)
}
