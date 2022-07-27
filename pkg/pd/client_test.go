package pd_test

import (
	"encoding/json"
	"testing"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/pd"
	"github.com/stretchr/testify/assert"
)

func TestParseRegionFromV4(t *testing.T) {
	// pd Region unmarsal
	jsonRsp := []byte(`
{"id": 58,
  "start_key": "7480000000000000FF345F720000000000FA",
  "end_key": "7480000000000000FF3500000000000000F8",
  "epoch": {
    "conf_ver": 2,
    "version": 28
  },
  "peers": [
    {
      "id": 59,
      "store_id": 1
    },
    {
      "id": 60,
      "store_id": 44,
      "is_learner": true
    }
  ],
  "leader": {
    "id": 59,
    "store_id": 1
  },
  "written_bytes": 828,
  "read_bytes": 0,
  "written_keys": 2,
  "read_keys": 0,
  "approximate_size": 1,
  "approximate_keys": 128
}`)

	region := pd.Region{}
	err := json.Unmarshal(jsonRsp, &region)
	assert.Equal(t, err, nil)

	assert.Equal(t, int64(58), region.Id)
	assert.Equal(t, "7480000000000000FF345F720000000000FA", region.StartKey)
	assert.Equal(t, "7480000000000000FF3500000000000000F8", region.EndKey)
	assert.Equal(t, 2, len(region.Peers))
	p59 := region.Peers[0]
	assert.Equal(t, int64(59), p59.Id)
	assert.Equal(t, int64(1), p59.StoreId)
	assert.Equal(t, pd.RoleNameVoter, p59.RoleName)
	p60 := region.Peers[1]
	assert.Equal(t, int64(60), p60.Id)
	assert.Equal(t, int64(44), p60.StoreId)
	assert.Equal(t, pd.RoleNameLearner, p60.RoleName)

	assert.Equal(t, []int64{44}, region.GetLearnerStoreIDs())
}

func TestParseRegionFromV5(t *testing.T) {
	// pd Region unmarsal
	jsonRsp := []byte(`
{"id": 4824,
  "start_key": "7480000000000000FF4C5F728000000094FFFFC3460000000000FA",
  "end_key": "7480000000000000FF4C5F728000000095FF06C0E00000000000FA",
  "epoch": {
    "conf_ver": 2,
    "version": 817
  },
  "peers": [
    {
      "id": 4825,
      "store_id": 1,
      "role_name": "Voter"
    },
    {
      "id": 4826,
      "store_id": 68,
      "role": 1,
      "role_name": "Learner",
      "is_learner": true
    }
  ],
  "leader": {
    "id": 4825,
    "store_id": 1,
    "role_name": "Voter"
  },
  "written_bytes": 0,
  "read_bytes": 0,
  "written_keys": 0,
  "read_keys": 0,
  "approximate_size": 105,
  "approximate_keys": 503161}`)

	region := pd.Region{}
	err := json.Unmarshal(jsonRsp, &region)
	assert.Equal(t, err, nil)

	assert.Equal(t, int64(4824), region.Id)
	assert.Equal(t, "7480000000000000FF4C5F728000000094FFFFC3460000000000FA", region.StartKey)
	assert.Equal(t, "7480000000000000FF4C5F728000000095FF06C0E00000000000FA", region.EndKey)
	assert.Equal(t, 2, len(region.Peers))
	p0 := region.Peers[0]
	assert.Equal(t, int64(4825), p0.Id)
	assert.Equal(t, int64(1), p0.StoreId)
	assert.Equal(t, pd.RoleNameVoter, p0.RoleName)
	p1 := region.Peers[1]
	assert.Equal(t, int64(4826), p1.Id)
	assert.Equal(t, int64(68), p1.StoreId)
	assert.Equal(t, pd.RoleNameLearner, p1.RoleName)

	assert.Equal(t, []int64{68}, region.GetLearnerStoreIDs())
}
