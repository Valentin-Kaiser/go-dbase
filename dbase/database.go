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

// Open a database and all related tables
// Only works with default IO implementation
func OpenDatabase(config *Config) (*Database, error) {
	if config == nil {
		return nil, newError("dbase-io-opendatabase-1", fmt.Errorf("missing config"))
	}
	if len(strings.TrimSpace(config.Filename)) == 0 {
		return nil, newError("dbase-io-opendatabase-2", fmt.Errorf("missing filename"))
	}
	if strings.ToUpper(filepath.Ext(config.Filename)) != string(DBC) {
		return nil, newError("dbase-io-opendatabase-3", fmt.Errorf("invalid file name: %v", config.Filename))
	}
	debugf("Opening database: %v", config.Filename)
	databaseTable, err := OpenTable(config)
	if err != nil {
		return nil, newError("dbase-io-opendatabase-4", fmt.Errorf("opening database table failed with error: %w", err))
	}
	// Search by all records where object type is table
	typeField, err := databaseTable.NewFieldByName("OBJECTTYPE", "Table")
	if err != nil {
		return nil, newError("dbase-io-opendatabase-5", fmt.Errorf("creating type field failed with error: %w", err))
	}
	rows, err := databaseTable.Search(typeField, true)
	if err != nil {
		return nil, newError("dbase-io-opendatabase-6", fmt.Errorf("searching for type field failed with error: %w", err))
	}
	// Try to load the table files
	tables := make(map[string]*File, 0)
	for _, row := range rows {
		objectName, err := row.ValueByName("OBJECTNAME")
		if err != nil {
			return nil, newError("dbase-io-opendatabase-7", fmt.Errorf("getting table name failed with error: %w", err))
		}
		tableName, ok := objectName.(string)
		if !ok {
			return nil, newError("dbase-io-opendatabase-8", fmt.Errorf("table name is not a string"))
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
			return nil, newError("dbase-io-opendatabase-9", fmt.Errorf("opening table failed with error: %w", err))
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
			return newError("dbase-io-close-1", err)
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
