package dbase

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

type Database struct {
	file   *File
	tables map[string]*File
}

// OpenDatabase opens a dbase/foxpro database file and all related tables
// The database file must be a DBC file and the tables must be DBF files and in the same directory as the database
func OpenDatabase(config *Config) (*Database, error) {
	if config == nil {
		return nil, NewError("missing dbase configuration")
	}
	if len(strings.TrimSpace(config.Filename)) == 0 {
		return nil, NewError("missing dbase filename")
	}
	if strings.ToUpper(filepath.Ext(config.Filename)) != string(DBC) {
		return nil, NewError("invalid dbase filename").Details(fmt.Errorf("file extension must be %v", DBC))
	}
	debugf("Opening database: %v", config.Filename)
	databaseTable, err := OpenTable(config)
	if err != nil {
		return nil, WrapError(err)
	}
	// Search by all records where object type is table
	typeField, err := databaseTable.NewFieldByName("OBJECTTYPE", "Table")
	if err != nil {
		return nil, WrapError(err)
	}
	rows, err := databaseTable.Search(typeField, true)
	if err != nil {
		return nil, WrapError(err)
	}
	// Try to load the table files
	tables := make(map[string]*File, 0)
	for _, row := range rows {
		objectName, err := row.ValueByName("OBJECTNAME")
		if err != nil {
			return nil, WrapError(err)
		}
		tableName, ok := objectName.(string)
		if !ok {
			return nil, NewError("table name is not a string")
		}
		tableName = strings.Trim(tableName, " ")
		if tableName == "" {
			continue
		}
		debugf("Found table: %v in database", tableName)
		tablePath := path.Join(filepath.Dir(config.Filename), tableName+string(DBF))
		// Replace underscores with spaces
		if !config.DisableConvertFilenameUnderscores {
			tablePath = path.Join(filepath.Dir(config.Filename), strings.ReplaceAll(tableName, "_", " ")+string(DBF))
		}
		tableConfig := &Config{
			Filename:                          tablePath,
			Converter:                         config.Converter,
			Exclusive:                         config.Exclusive,
			Untested:                          config.Untested,
			TrimSpaces:                        config.TrimSpaces,
			DisableConvertFilenameUnderscores: config.DisableConvertFilenameUnderscores,
			ReadOnly:                          config.ReadOnly,
			WriteLock:                         config.WriteLock,
			ValidateCodePage:                  config.ValidateCodePage,
			InterpretCodePage:                 config.InterpretCodePage,
		}
		// Load the table
		table, err := OpenTable(tableConfig)
		if err != nil {
			return nil, WrapError(err)
		}
		if table != nil {
			tables[tableName] = table
		}
	}
	return &Database{file: databaseTable, tables: tables}, nil
}

// Close the database file and all related tables
func (db *Database) Close() error {
	for _, table := range db.tables {
		if err := table.Close(); err != nil {
			return WrapError(err)
		}
	}
	return db.file.Close()
}

// Returns all table of the database
func (db *Database) Tables() map[string]*File {
	return db.tables
}

// Returns the names of every table in the database
func (db *Database) Names() []string {
	names := make([]string, 0)
	for name := range db.tables {
		names = append(names, name)
	}
	return names
}

// Returns the complete database schema
func (db *Database) Schema() map[string][]*Column {
	schema := make(map[string][]*Column)
	for name, table := range db.tables {
		schema[name] = table.Columns()
	}
	return schema
}
