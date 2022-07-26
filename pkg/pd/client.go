package pd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
)

type Client struct {
	baseURL string
}

func NewPDClient(url string) Client {
	return Client{
		baseURL: url,
	}
}

type Peer struct {
	Id       int64  `json:"id"`
	StoreId  int64  `json:"store_id"`
	RoleName string `json:"role_name"`
}

const RoleNameLearner = "Learner"
const RoleNameVoter = "Voter"

func (p *Peer) UnmarshalJSON(data []byte) error {
	type APeer Peer
	peer := &APeer{}
	if err := json.Unmarshal(data, peer); err != nil {
		return err
	}

	if len(peer.RoleName) != 0 {
		p.RoleName = peer.RoleName
	} else {
		// Try to parse from v4.0.x version
		// { "id": 60,
		//   "store_id": 44,
		//   "is_learner": true }
		type V4Peer struct {
			IsLearner bool `json:"is_learner"`
		}
		v4peer := V4Peer{}
		if err := json.Unmarshal(data, &v4peer); err != nil {
			return err
		}

		if v4peer.IsLearner {
			p.RoleName = RoleNameLearner
		} else {
			p.RoleName = RoleNameVoter
		}
	}
	p.Id = peer.Id
	p.StoreId = peer.StoreId
	return nil
}

func (p *Peer) IsLearner() bool {
	return p.RoleName == RoleNameLearner
}

type Region struct {
	Id       int64  `json:"id"`
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
	Peers    []Peer `json:"peers"`
}

func (r *Region) GetLearnerIDs() []int64 {
	res := make([]int64, 0)
	for _, p := range r.Peers {
		if p.IsLearner() {
			res = append(res, p.StoreId)
		}
	}
	return res
}

func (c *Client) getAPIWithParam(route string, params url.Values) string {
	u, err := url.Parse(fmt.Sprintf("http://%s/pd/api/v1/%s", c.baseURL, route))
	if err != nil {
		return ""
	}
	u.RawQuery = params.Encode()
	return u.String()
}

func (c *Client) getAPI(route string) string {
	return fmt.Sprintf("http://%s/pd/api/v1/%s", c.baseURL, route)
}

func (c *Client) GetRegionByKey(key tidb.TiKVKey) (Region, error) {
	var region Region
	resp, err := http.Get(c.getAPI(fmt.Sprintf("region/key/%s", url.QueryEscape(string(key.GetBytes())))))
	if err != nil {
		return region, err
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return region, err
	}
	// fmt.Printf("%s\n", string(bytes))
	err = json.Unmarshal(bytes, &region)
	if err != nil {
		return region, err
	}
	return region, err
}

func (c *Client) GetNumRegionBetweenKey(startKey, endKey tidb.TiKVKey) (int64, error) {
	params := url.Values{}
	params.Set("start_key", string(startKey.GetBytes()))
	params.Set("end_key", string(endKey.GetBytes()))
	resp, err := http.Get(c.getAPIWithParam("stats/region", params))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	// fmt.Printf("%s\n", string(bytes))
	var result map[string]interface{}
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return 0, err
	}
	count, ok := result["count"]
	if !ok {
		return 0, fmt.Errorf("'count' is not exits in the response: %s", bytes)
	}
	cnt, ok := count.(float64)
	if !ok {
		return 0, fmt.Errorf("can not parse 'count' as float64, response: %s", bytes)
	}
	return int64(cnt), nil
}

type regionsByKeyResp struct {
	Count   int64    `json:"count"`
	Regions []Region `json:"regions"`
}

func (c *Client) GetRegions(startKey tidb.TiKVKey, limit int64) ([]Region, error) {
	params := url.Values{}
	params.Set("key", string(startKey.GetBytes()))
	params.Set("limit", strconv.FormatInt(limit, 10))
	resp, err := http.Get(c.getAPIWithParam("regions/key", params))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("%s\n", string(bytes))
	var result regionsByKeyResp
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, err
	}
	return result.Regions, nil
}
