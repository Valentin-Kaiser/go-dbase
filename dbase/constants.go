package dbase

// Supported and testet file types - other file types may work but are not tested
// The file type check has to be bypassed when opening a file type that is not supported
type FileType byte

// Supported and testet file types - other file types may work but are not tested
const (
	FoxPro              FileType = 0x30
	FoxProAutoincrement FileType = 0x31
	FoxProVar           FileType = 0x32
)

// Not tested
const (
	FoxBase         FileType = 0x02
	FoxBase2        FileType = 0xFB
	FoxBasePlus     FileType = 0x03
	DBaseSQLTable   FileType = 0x43
	FoxBasePlusMemo FileType = 0x83
	DBaseMemo       FileType = 0x8B
	DBaseSQLMemo    FileType = 0xCB
	FoxPro2Memo     FileType = 0xF5
)

type Marker byte

const (
	Null      Marker = 0x00
	Blank     Marker = 0x20
	ColumnEnd Marker = 0x0D
	Active    Marker = Blank
	Deleted   Marker = 0x2A
	EOFMarker Marker = 0x1A
)

type TableFlag byte

const (
	StructuralFlag     TableFlag = 0x01
	MemoFlag           TableFlag = 0x02
	StructuralMemoFlag TableFlag = 0x03
	DatabaseFlag       TableFlag = 0x04
)

type ColumnFlag byte

const (
	HiddenFlag        ColumnFlag = 0x01
	NullableFlag      ColumnFlag = 0x02
	BinaryFlag        ColumnFlag = 0x04
	AutoincrementFlag ColumnFlag = 0x0C
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
