package tidb

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type TiDBClientOpts struct {
	Host     string
	Port     int32
	User     string
	Password string
}

type Client struct {
	Db *sql.DB // TODO: Maybe find a way not exposing it to public?
}

func NewClientFromOpts(opts TiDBClientOpts) (Client, error) {
	return NewClient(opts.Host, int32(opts.Port), opts.User, opts.Password)
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

func (c *Client) ExecWithElapsed(sql string) error {
	defer func(start time.Time) {
		elapsed := time.Since(start)
		fmt.Printf("%s => %dms\n", sql, elapsed.Milliseconds())
	}(time.Now())

	_, err := c.Db.Exec(sql)
	return err
}

func (c *Client) GetTableID(dbName, tblName string) (int64, error) {
	rows, err := c.Db.Query("select TABLE_ID from information_schema.tiflash_replica where TABLE_SCHEMA = ? and TABLE_NAME = ?", dbName, tblName)
	if err != nil {
		return 0, err
	}
	var table_id int64
	for rows.Next() {
		rows.Scan(&table_id)
	}
	return table_id, nil
}

func (c *Client) GetInstances(selectType string) ([]string, error) {
	rows, err := c.Db.Query("select INSTANCE from information_schema.cluster_info where type = ?", selectType)
	if err != nil {
		return nil, err
	}
	var pdInstances []string
	var inst string
	for rows.Next() {
		rows.Scan(&inst)
		pdInstances = append(pdInstances, inst)
	}
	return pdInstances, nil
}
