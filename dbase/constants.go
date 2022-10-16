package dbase

// Supported and testet file versions - other files may work but are not tested
// The file version check has to be bypassed when opening a file type that is not supported
// https://learn.microsoft.com/en-us/previous-versions/visualstudio/foxpro/st4a0s68(v=vs.71)
type FileVersion byte

// Supported and testet file types - other file types may work but are not tested
const (
	FoxPro              FileVersion = 0x30
	FoxProAutoincrement FileVersion = 0x31
	FoxProVar           FileVersion = 0x32
)

// Not tested
const (
	FoxBase         FileVersion = 0x02
	FoxBase2        FileVersion = 0xFB
	FoxBasePlus     FileVersion = 0x03
	DBaseSQLTable   FileVersion = 0x43
	FoxBasePlusMemo FileVersion = 0x83
	DBaseMemo       FileVersion = 0x8B
	DBaseSQLMemo    FileVersion = 0xCB
	FoxPro2Memo     FileVersion = 0xF5
)

// Table file extenstions
type FileExtension string

const (
	DBC FileExtension = ".DBC" // Database file extension
	DCT FileExtension = ".DCT" // Database container file extension
	DBT FileExtension = ".DBT" // Table file extension
	FPT FileExtension = ".FPT" // Memo file extension
	SCX FileExtension = ".SCX" // Form file extension
	LBX FileExtension = ".LBX" // Label file extension
	MNX FileExtension = ".MNX" // Menu file extension
	PJX FileExtension = ".PJX" // Project file extension
	RPX FileExtension = ".RPX" // Report file extension
	VCX FileExtension = ".VCX" // Visual class library file extension
)

// Important byte marker for the dbase file
type Marker byte

const (
	Null      Marker = 0x00
	Blank     Marker = 0x20
	ColumnEnd Marker = 0x0D
	Active    Marker = Blank
	Deleted   Marker = 0x2A
	EOFMarker Marker = 0x1A
)

// Table flags inidicate the type of the table
// https://learn.microsoft.com/en-us/previous-versions/visualstudio/foxpro/st4a0s68(v=vs.71)
type TableFlag byte

const (
	StructuralFlag TableFlag = 0x01
	MemoFlag       TableFlag = 0x02
	DatabaseFlag   TableFlag = 0x04
)

func (t TableFlag) Defined(flag byte) bool {
	return t&TableFlag(flag) == TableFlag(t)
}

// Column flags indicate wether a column is hidden, can be null, is binary or is autoincremented
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
	Character DataType = 0x43 // C - Character (string)
	Currency  DataType = 0x59 // Y - Currency (float64)
	Double    DataType = 0x42 // B - Double (float64)
	Date      DataType = 0x44 // D - Date (time.Time)
	DateTime  DataType = 0x54 // T - DateTime (time.Time)
	Float     DataType = 0x46 // F - Float (float64)
	Integer   DataType = 0x49 // I - Integer (int32)
	Logical   DataType = 0x4C // L - Logical (bool)
	Memo      DataType = 0x4D // M - Memo (string)
	Numeric   DataType = 0x4E // N - Numeric (int64)
	Blob      DataType = 0x57 // W - Blob ([]byte)
	General   DataType = 0x47 // G - General (string)
	Picture   DataType = 0x50 // P - Picture (string)
	Varbinary DataType = 0x51 // Q - Varbinary ([]byte)
	Varchar   DataType = 0x56 // V - Varchar (string)
)

// Returns the type of the column as string
func (t DataType) String() string {
	return string(t)
}
