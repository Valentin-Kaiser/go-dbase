package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Convert year, month and day to a julian day number.
// (Julian day number -> days since 01-01-4712 BC)
// This method is based on Fliegel/van Flandern algorithm.
func julianDate(y, m, d int) int {
	return (1461*(y+4800+(m-14)/12))/4 +
		(367*(m-2-12*((m-14)/12)))/12 -
		(3*((y+4900+(m-14)/12)/100))/4 +
		d - 32075
}

// Convert julian day number to year, month and day.
// (Julian day number -> days since 01-01-4712 BC)
// nolint: gocritic // assignment can not be simplified
func julianToDate(jd int) (int, int, int) {
	l := jd + 68569
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	d := l - (2447*j)/80
	l = j / 11
	m := j + 2 - 12*l
	y := 100*(n-49) + i + l
	return y, m, d
}

// parseDate parses a date string from a byte slice and returns a time.Time
func parseDate(raw []byte) (time.Time, error) {
	raw = sanitizeEmptyBytes(raw)
	if len(raw) == 0 {
		return time.Time{}, nil
	}
	t, err := time.Parse("20060102", string(raw))
	if err != nil {
		return t, NewError("failed to parse date").Details(err)
	}
	return t, nil
}

// parseDateTIme parses a date and time string from a byte slice and returns a time.Time
func parseDateTime(raw []byte) time.Time {
	if len(raw) != 8 {
		return time.Time{}
	}
	julDat := int(binary.LittleEndian.Uint32(raw[:4]))
	mSec := int(binary.LittleEndian.Uint32(raw[4:]))
	// Determine year, month, day
	y, m, d := julianToDate(julDat)
	if y < 0 || y > 9999 {
		return time.Time{}
	}
	// Calculate whole seconds and use the remainder as nanosecond resolution
	nSec := mSec / 1000
	mSec -= (nSec * 1000)
	// Create time using ymd and nanosecond timestamp
	return time.Date(y, time.Month(m), d, 0, 0, nSec, mSec*int(time.Millisecond), time.UTC)
}

// parseNumericInt parses a string as byte array to int64
func parseNumericInt(raw []byte) (int64, error) {
	trimmed := string(sanitizeEmptyBytes(raw))
	if len(trimmed) == 0 {
		return int64(0), nil
	}
	i, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return i, NewError("failed to parse int").Details(err)
	}
	return i, nil
}

// parseFloat parses a string as byte array to float64
func parseFloat(raw []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(sanitizeEmptyBytes(raw)))
	if len(trimmed) == 0 {
		return float64(0), nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
	if err != nil {
		return f, NewError("failed to parse float").Details(err)
	}
	return f, nil
}

// toUTF8String converts a byte slice to a UTF8 string using the converter
func toUTF8String(raw []byte, converter EncodingConverter) (string, error) {
	utf8, err := converter.Decode(raw)
	if err != nil {
		return string(raw), WrapError(err)
	}
	return string(utf8), nil
}

// fromUTF8String converts a UTF8 string to a byte slice using the given converter
func fromUtf8String(raw []byte, converter EncodingConverter) ([]byte, error) {
	utf8, err := converter.Encode(raw)
	if err != nil {
		return raw, WrapError(err)
	}
	return utf8, nil
}

// Convert data to binary representation
func toBinary(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil, NewError("failed to convert to binary").Details(err)
	}
	return buf.Bytes(), nil
}

// appendSpaces appends spaces to a byte slice until it reaches the given length
func appendSpaces(raw []byte, length int) []byte {
	return appendBytes(raw, length, ' ')
}

// appendBytes appends bytes to a byte slice until it reaches the given length
func appendBytes(raw []byte, length int, value byte) []byte {
	if len(raw) < length {
		a := make([]byte, length-len(raw))
		for i := range a {
			a[i] = value
		}
		return append(raw, a...)
	}
	return raw
}

// prependSpaces prepends spaces to a byte slice until it reaches the given length
func prependSpaces(raw []byte, length int) []byte {
	return prependBytes(raw, length, ' ')
}

// prependBytes prepends bytes to a byte slice until it reaches the given length
func prependBytes(raw []byte, length int, value byte) []byte {
	if len(raw) < length {
		result := make([]byte, 0)
		for i := 0; i < length-len(raw); i++ {
			result = append(result, value)
		}
		return append(result, raw...)
	}
	return raw
}

func sanitizeEmptyBytes(raw []byte) []byte {
	raw = bytes.ReplaceAll(raw, []byte{0x00}, []byte{})
	raw = []byte(strings.TrimSpace(string(raw)))
	return raw
}

func sanitizeSpaces(str string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(str, " ")
}

// nthBit returns the nth bit of a byte slice
func getNthBit(bytes []byte, n int) bool {
	if n > len(bytes)*8 {
		return false
	}
	byteIndex := n / 8 // byte index
	bitIndex := n % 8  // bit index
	return bytes[byteIndex]&(1<<bitIndex) == (1 << bitIndex)
}

func setNthBit(b byte, n int) byte {
	b |= 1 << n
	return b
}

// setStructField sets the field with the key or dbase tag of name of the struct obj to the given value
func setStructField(tags map[string]string, obj interface{}, name string, value interface{}) error {
	if fieldName, ok := tags[strings.ToUpper(name)]; ok {
		name = fieldName
	}
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)
	if !structFieldValue.IsValid() {
		for i := 0; i < structValue.NumField(); i++ {
			field := structValue.Type().Field(i)
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				err := setStructField(tags, structValue.Field(i).Addr().Interface(), name, value)
				if err == nil {
					return nil
				}
			}
		}
		return nil
	}
	if !structFieldValue.CanSet() {
		return NewError("failed to set struct field value").Details(fmt.Errorf("cannot set %s field value", name))
	}
	structFieldType := structFieldValue.Type()
	value = cast(value, structFieldType)
	val := reflect.ValueOf(value)

	if structFieldType.Kind() == reflect.Ptr {
		// Convert the value to a pointer to match the field type
		ptr := reflect.New(structFieldType.Elem())
		ptr.Elem().Set(reflect.ValueOf(cast(value, structFieldType.Elem())))
		val = ptr
	}

	if structFieldType != val.Type() {
		return NewError("provided value type didn't match obj field type").Details(fmt.Errorf("provided value type %v didn't match obj field type %v", val.Type(), structFieldType))
	}
	structFieldValue.Set(val)
	return nil
}

// structTags extracts the dbase tag from the struct fields
func getStructTags(v interface{}) map[string]string {
	tags := make(map[string]string)
	extractTags(reflect.ValueOf(v).Elem(), tags)
	return tags
}

func extractTags(structValue reflect.Value, tags map[string]string) {
	for i := 0; i < structValue.NumField(); i++ {
		field := structValue.Type().Field(i)

		// Check if this field is an embedded struct
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			extractTags(structValue.Field(i), tags)
			continue
		}

		tag := strings.ToUpper(field.Tag.Get("dbase"))
		if len(tag) > 0 {
			tags[tag] = field.Name
		}
	}
}

// cast converts a value to the given type if possible
func cast(v interface{}, t reflect.Type) interface{} {
	if v == nil {
		return nil
	}
	if reflect.TypeOf(v) == t {
		return v
	}
	if reflect.TypeOf(v).ConvertibleTo(t) {
		return reflect.ValueOf(v).Convert(t).Interface()
	}
	return v
}
