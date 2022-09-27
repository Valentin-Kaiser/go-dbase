package dbase

import (
	"bytes"
	"encoding/binary"
	"fmt"

	syscall "golang.org/x/sys/windows"
)

// The raw header of the Memo file.
type MemoHeader struct {
	NextFree  uint32  // Location of next free block
	Unused    [2]byte // Unused
	BlockSize uint16  // Block size (bytes per block)
}

/**
 *	################################################################
 *	#					dBase memo helper
 *	################################################################
 */

// Parses a memo file from raw []byte, decodes and returns as []byte
func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readMemo(raw)
	if err != nil {
		return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-1:FAILED:%w", err)
	}
	if isText {
		memo, err = dbf.convert.Decode(memo)
		if err != nil {
			return []byte{}, false, fmt.Errorf("dbase-table-parse-memo-2:FAILED:%w", err)
		}
	}
	return memo, isText, nil
}

// prepareMemo prepares the memo file for reading.
func (dbf *DBF) prepareMemo(fd syscall.Handle) error {
	memoHeader, err := readMemoHeader(fd)
	if err != nil {
		return fmt.Errorf("dbase-table-prepare-memo-1:FAILED:%w", err)
	}
	dbf.memoFileHandle = &fd
	dbf.memoHeader = memoHeader
	return nil
}

// readMemoHeader reads the memo header from the given file handle.
func readMemoHeader(fd syscall.Handle) (*MemoHeader, error) {
	h := &MemoHeader{}
	if _, err := syscall.Seek(fd, 0, 0); err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-1:FAILED:%w", err)
	}
	b := make([]byte, 1024)
	n, err := syscall.Read(fd, b)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-2:FAILED:%w", err)
	}
	err = binary.Read(bytes.NewReader(b[:n]), binary.BigEndian, h)
	if err != nil {
		return nil, fmt.Errorf("dbase-table-read-memo-header-3:FAILED:%w", err)
	}
	return h, nil
}
