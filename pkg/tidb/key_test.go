package tidb_test

import (
	"testing"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/stretchr/testify/assert"
)

func TestKey(t *testing.T) {
	const pdTableNormalKey string = "7480000000000000FF375F72830000003DFF3FEC150000000000FA"
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableNormalKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableNormalKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, int64(55), tableRow.TableID)
	assert.Equal(t, int64(216172783141383189), tableRow.RowID)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableNormalKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)
	assert.Equal(t, tableRow.RowID, encTableRow.RowID)
}

func TestStartKey(t *testing.T) {
	const pdTableStartKey string = "7480000000000000FFEC5F720000000000FA"
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableStartKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableStartKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, int64(236), tableRow.TableID)
	assert.Equal(t, tidb.MinInf, tableRow.Status)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableStartKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)
}

func TestEndKey(t *testing.T) {
	const pdTableEndKey string = "7480000000000000FFED00000000000000F8"
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableEndKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableEndKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, int64(236), tableRow.TableID)
	assert.Equal(t, tidb.MaxInf, tableRow.Status)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableEndKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)
}
