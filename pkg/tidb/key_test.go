package tidb_test

import (
	"testing"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/stretchr/testify/assert"
)

func TestKey(t *testing.T) {
	const (
		pdTableNormalKey string = "7480000000000000FF375F72830000003DFF3FEC150000000000FA"
		expectTableID    int64  = 55
		expectTableRowID int64  = 216172783141383189
	)
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableNormalKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableNormalKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, expectTableID, tableRow.TableID)
	assert.Equal(t, expectTableRowID, tableRow.RowID)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableNormalKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)
	assert.Equal(t, tableRow.RowID, encTableRow.RowID)

	newKey := tidb.NewTableRowAsKey(expectTableID, expectTableRowID)
	assert.Equal(t, newKey.GetPDKey(), key.GetPDKey())
}

func TestStartKey(t *testing.T) {
	const pdTableStartKey string = "7480000000000000FFEC5F720000000000FA"
	const expectTableID int64 = 236
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableStartKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableStartKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, expectTableID, tableRow.TableID)
	assert.Equal(t, tidb.MinInf, tableRow.Status)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableStartKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)

	newKey := tidb.NewTableStartAsKey(expectTableID)
	assert.Equal(t, newKey.GetPDKey(), key.GetPDKey())
}

func TestEndKey(t *testing.T) {
	const pdTableEndKey string = "7480000000000000FFED00000000000000F8"
	const expectTableID int64 = 236
	// pd key -> kv key
	key, err := tidb.FromPDKey(pdTableEndKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), pdTableEndKey)
	// kv key -> tidb tableID, rowID
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, expectTableID, tableRow.TableID)
	assert.Equal(t, tidb.MaxInf, tableRow.Status)
	// tidb tableID, rowID -> kv key
	encKey := tableRow.GetKey()
	assert.Equal(t, pdTableEndKey, encKey.GetPDKey())
	// kv key -> tidb tableID, rowID again
	encTableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, tableRow.TableID, encTableRow.TableID)
	assert.Equal(t, tableRow.Status, encTableRow.Status)

	newKey := tidb.NewTableEndAsKey(expectTableID)
	assert.Equal(t, newKey.GetPDKey(), key.GetPDKey())
}
