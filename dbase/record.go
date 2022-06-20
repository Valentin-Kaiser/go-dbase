package dbase

import (
	"encoding/json"
	"fmt"
	"strings"
	"syscall"
)

// Contains the raw record data and a deleted flag
type Record struct {
	DBF     *DBF
	Deleted bool
	Data    []interface{}
}

// Reads raw record data of one record at recordPosition
func (dbf *DBF) readRecord(recordPosition uint32) ([]byte, error) {
	if recordPosition >= dbf.dbaseHeader.RecordsCount {
		return nil, fmt.Errorf("dbase-reader-read-record-1:FAILED:%v", ERROR_EOF.AsError())
	}
	buf := make([]byte, dbf.dbaseHeader.RecordLength)

	_, err := syscall.Seek(syscall.Handle(*dbf.dbaseFileHandle), int64(dbf.dbaseHeader.FirstRecord)+(int64(recordPosition)*int64(dbf.dbaseHeader.RecordLength)), 0)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-record-2:FAILED:%v", err)
	}

	read, err := syscall.Read(syscall.Handle(*dbf.dbaseFileHandle), buf)
	if err != nil {
		return buf, fmt.Errorf("dbase-reader-read-record-3:FAILED:%v", err)
	}

	if read != int(dbf.dbaseHeader.RecordLength) {
		return buf, fmt.Errorf("dbase-reader-read-record-1:FAILED:%v", ERROR_INCOMPLETE.AsError())
	}
	return buf, nil
}

// Returns all records
func (dbf *DBF) Records(skipInvalid bool) ([]*Record, error) {
	records := make([]*Record, 0)
	for !dbf.EOF() {
		// This reads the complete record
		record, err := dbf.GetRecord()
		if err != nil && !skipInvalid {
			return nil, fmt.Errorf("dbase-reader-records-1:FAILED:%v", err)
		}

		dbf.Skip(1)
		// skip deleted records
		if record.Deleted {
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

// Returns the requested record at dbf.recordPointer.
func (dbf *DBF) GetRecord() (*Record, error) {
	data, err := dbf.readRecord(dbf.recordPointer)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-get-record-1:FAILED:%v", err)
	}

	return dbf.bytesToRecord(data)
}

/**
 *	################################################################
 *	#						Record conversion
 *	################################################################
 */

// Returns all records as a slice of maps.
func (dbf *DBF) RecordsToMap(skipInvalid bool) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0)

	records, err := dbf.Records(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		rmap, err := record.ToMap()
		if err != nil {
			return nil, err
		}

		out = append(out, rmap)
	}

	return out, nil
}

// Returns all records as json
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (dbf *DBF) RecordsToJSON(skipInvalid bool, trimspaces bool) ([]byte, error) {
	records, err := dbf.RecordsToMap(skipInvalid)
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-to-json-1:FAILED:%v", err)
	}

	mapRecords := make([]map[string]interface{}, 0)
	for _, record := range records {
		if trimspaces {
			for k, v := range record {
				if str, ok := v.(string); ok {
					record[k] = strings.TrimSpace(str)
				}
			}
		}
		mapRecords = append(mapRecords, record)
	}

	return json.Marshal(mapRecords)
}

// Returns all records as a slice of struct.
// Parses the record from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the record into a struct, json.Unmarshal matches incoming object keys to either the struct field name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
// v keeps the last converted struct.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (dbf *DBF) RecordsToStruct(v interface{}, skipInvalid bool, trimspaces bool) ([]interface{}, error) {
	out := make([]interface{}, 0)

	records, err := dbf.Records(skipInvalid)
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		err := record.ToStruct(v, trimspaces)
		if err != nil {
			return nil, err
		}

		out = append(out, v)
	}

	return out, nil
}

// Returns a complete record as a map.
func (rec *Record) ToMap() (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for i, fn := range rec.DBF.FieldNames() {
		val, err := rec.Field(i)
		if err != nil {
			return out, fmt.Errorf("dbase-reader-to-map-1:FAILED:error on field %s (column %d): %s", fn, i, err)
		}
		out[fn] = val
	}
	return out, nil
}

// Returns a complete record as a JSON object.
// If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (rec *Record) ToJSON(trimspaces bool) ([]byte, error) {
	m, err := rec.ToMap()
	if err != nil {
		return nil, fmt.Errorf("dbase-reader-to-json-1:FAILED:%v", err)
	}
	if trimspaces {
		for k, v := range m {
			if str, ok := v.(string); ok {
				m[k] = strings.TrimSpace(str)
			}
		}
	}
	return json.Marshal(m)
}

// Parses the record from map to JSON-encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, an InvalidUnmarshalError will be returned.
// To convert the record into a struct, json.Unmarshal matches incoming object keys to either the struct field name or its tag,
// preferring an exact match but also accepting a case-insensitive match.
func (rec *Record) ToStruct(v interface{}, trimspaces bool) error {
	jsonRecord, err := rec.ToJSON(trimspaces)
	if err != nil {
		return fmt.Errorf("dbase-reader-to-struct-1:FAILED:%v", err)
	}

	err = json.Unmarshal(jsonRecord, v)
	if err != nil {
		return fmt.Errorf("dbase-reader-to-struct-2:FAILED:%v", err)
	}

	return nil
}

// Field gets a fields value by field pos (index)
func (r *Record) Field(pos int) (interface{}, error) {
	if pos < 0 || len(r.Data) < pos {
		return 0, fmt.Errorf("dbase-reader-field-1:FAILED:%v", ERROR_INVALID.AsError())
	}
	return r.Data[pos], nil
}

// FieldSlice gets all fields as a slice
func (r *Record) FieldSlice() []interface{} {
	return r.Data
}
