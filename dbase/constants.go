package dbase

// Supported and testet file types - other file types may work but are not tested
// The file type check has to be bypassed when opening a file type that is not supported
const (
	FoxPro              = 0x30
	FoxProAutoincrement = 0x31
	FoxProVar           = 0x32
)

// Relevant byte marker
const (
	Null      = 0x00
	Blank     = 0x20
	ColumnEnd = 0x0D
	Active    = Blank
	Deleted   = 0x2A
	EOFMarker = 0x1A
)

// Table flags
const (
	StructuralFlag     = 0x01
	MemoFlag           = 0x02
	StructuralMemoFlag = 0x03
	DatabaseFlag       = 0x04
)

// DataType defines the possible types of a column
type DataType byte

const (
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

// Returns the type of the column as string
func (t DataType) String() string {
	return string(t)
}
