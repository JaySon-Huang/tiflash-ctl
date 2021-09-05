package pd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

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

func (p *Peer) IsLearner() bool {
	return p.RoleName == "Learner"
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
