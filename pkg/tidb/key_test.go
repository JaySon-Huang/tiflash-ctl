package tidb_test

import (
	"testing"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/stretchr/testify/assert"
)

func TestKey(t *testing.T) {
	key, err := tidb.FromPDKey("7480000000000000FF375F72830000003DFF3FEC150000000000FA")
	assert.Equal(t, err, nil)
	assert.Equal(t, key.GetPDKey(), "7480000000000000FF375F72830000003DFF3FEC150000000000FA")
	assert.True(t, false)
	tableRow, err := key.GetTableRow()
	assert.Equal(t, err, nil)
	assert.Equal(t, int64(55), tableRow.TableID)
	assert.Equal(t, int64(216172783141383189), tableRow.RowID)
}
