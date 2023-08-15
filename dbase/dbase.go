// This go-dbase package offers tools for managing dBase-format database files.
// It supports tailored I/O operations for Unix and Windows platforms,
// provides flexible data representations like maps, JSON, and Go structs,
// and ensures safe concurrent operations with built-in mutex locks.
//
// The package facilitates defining, manipulating, and querying columns
// and rows in dBase tables, converting between dBase-specific data types
// and Go data types, and performing systematic error handling.
//
// Typical use cases include data retrieval from legacy dBase systems,
// conversion of dBase files to modern formats, and building applications
// that interface with dBase databases.
package dbase

// Config is a struct containing the configuration for opening a Foxpro/dbase databse or table.
// The filename is mandatory.
//
// The other fields are optional and are false by default.
// If Converter and InterpretCodePage are both not set the package will try to interpret the code page mark.
// To open untested files set Untested to true. Tested files are defined in the constants.go file.
type Config struct {
	Filename                          string            // The filename of the DBF file.
	Converter                         EncodingConverter // The encoding converter to use.
	Exclusive                         bool              // If true the file is opened in exclusive mode.
	Untested                          bool              // If true the file version is not checked.
	TrimSpaces                        bool              // Trimspaces default value
	DisableConvertFilenameUnderscores bool              // If false underscores in the table filename are converted to spaces.
	ReadOnly                          bool              // If true the file is opened in read-only mode.
	WriteLock                         bool              // Whether or not the write operations should lock the record
	ValidateCodePage                  bool              // Whether or not the code page mark should be validated.
	InterpretCodePage                 bool              // Whether or not the code page mark should be interpreted. Ignores the defined converter.
	IO                                IO                // The IO interface to use.
}

// Modification allows to change the column name or value type of a column when reading the table
// The TrimSpaces option is only used for a specific column, if the general TrimSpaces option in the config is false.
type Modification struct {
	TrimSpaces  bool                                   // Trim spaces from string values
	Convert     func(interface{}) (interface{}, error) // Conversion function to convert the value
	ExternalKey string                                 // External key to use for the column
}
