package tidb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/codec"
)

type TableRowStatus int32

const (
	MinInf TableRowStatus = 1
	MaxInf TableRowStatus = 2
)

type TableRow struct {
	TableID int64
	RowID   int64
	Status  TableRowStatus
}

type TiKVKey struct {
	key []byte
}

func NewTableRow(tableID, rowID int64) TableRow {
	return TableRow{TableID: tableID, RowID: rowID}
}

func NewTableRowAsKey(tableID, rowID int64) TiKVKey {
	r := TableRow{TableID: tableID, RowID: rowID}
	return r.GetKey()
}

func (r *TableRow) GetKey() TiKVKey {
	key := []byte{'t'}
	if r.Status == MaxInf {
		key = codec.EncodeInt(key, r.TableID+1)
	} else {
		key = codec.EncodeInt(key, r.TableID)
		key = append(key, []byte("_r")...)
		if r.Status != MinInf {
			key = codec.EncodeInt(key, r.RowID)
		}
	}
	key = codec.EncodeBytes([]byte{}, key)
	return TiKVKey{key}
}

func FromPDKey(k string) (TiKVKey, error) {
	b, err := hex.DecodeString(k)
	return TiKVKey{key: b}, err
}

func (k *TiKVKey) GetBytes() []byte {
	return k.key
}

func (k *TiKVKey) GetPDKey() string {
	return strings.ToUpper(hex.EncodeToString(k.key))
}

func (k *TiKVKey) GetTableRow() (TableRow, error) {
	_, b, err := codec.DecodeBytes(k.key, nil)
	if err != nil {
		return TableRow{}, err
	}
	var (
		tableID int64
		rowID   int64
	)
	if len(b) == 1+8+2 || len(b) == 1+8 {
		// The start key or end key of a table
		if _, tableID, err = codec.DecodeInt(b[1:]); err != nil {
			return TableRow{}, err
		}
		var status TableRowStatus = 0
		if len(b) == 1+8 {
			tableID = tableID - 1
			status = MaxInf
		} else {
			status = MinInf
		}
		return TableRow{TableID: tableID, RowID: rowID, Status: status}, nil
	} else if len(b) == 1+8+2+8 {
		if !bytes.Equal(b[9:11], []byte("_r")) {
			return TableRow{}, fmt.Errorf("invalid row prefix")
		}
		if _, tableID, err = codec.DecodeInt(b[1:]); err != nil {
			return TableRow{}, err
		}
		if _, rowID, err = codec.DecodeInt(b[11:]); err != nil {
			return TableRow{}, err
		}
		return TableRow{TableID: tableID, RowID: rowID}, nil
	}
	return TableRow{}, fmt.Errorf("size not fit, actual is %d, %s", len(b), k.GetPDKey())
}
