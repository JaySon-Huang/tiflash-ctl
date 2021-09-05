package tidb

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	Db *sql.DB
}

func NewClient(host string, port int32, user, password string) (Client, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return Client{}, fmt.Errorf("connect to database fail: %s", err)
	}
	return Client{Db: db}, nil
}

func (c *Client) Close() error {
	return c.Db.Close()
}

func (c *Client) GetTableID(db_name, tbl_name string) int64 {
	rows, err := c.Db.Query("select TABLE_ID from information_schema.tiflash_replica where TABLE_SCHEMA = ? and TABLE_NAME = ?", db_name, tbl_name)
	if err != nil {
		panic(err)
	}
	var table_id int64
	for rows.Next() {
		rows.Scan(&table_id)
	}
	return table_id
}

func (c *Client) GetInstances(selectType string) []string {
	rows, err := c.Db.Query("select INSTANCE from information_schema.cluster_info where type = ?", selectType)
	if err != nil {
		panic(err)
	}
	var pdInstances []string
	var inst string
	for rows.Next() {
		rows.Scan(&inst)
		pdInstances = append(pdInstances, inst)
	}
	return pdInstances
}
