package dbase

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
	Structural     byte = 0x01
	Memo           byte = 0x02
	StructuralMemo byte = 0x03
	Database       byte = 0x04
)
