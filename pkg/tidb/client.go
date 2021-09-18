package tidb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
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
	rows, err := c.Db.Query("select TIDB_TABLE_ID from information_schema.tables where TABLE_SCHEMA = ? and TABLE_NAME = ?", dbName, tblName)
	if err != nil {
		return 0, err
	}
	var tableID int64
	for rows.Next() {
		rows.Scan(&tableID)
	}
	return tableID, nil
}

type ClusteredIndexType int32

const (
	ClusteredIndexInt64  ClusteredIndexType = 0
	ClusteredIndexCommon ClusteredIndexType = 1
)

func (c *Client) GetTableIDAndClusteredIndex(dbName, tblName string) (int64, ClusteredIndexType, error) {
	rows, err := c.Db.Query("select `TIDB_TABLE_ID`,`TIDB_PK_TYPE` from information_schema.tables where TABLE_SCHEMA = ? and TABLE_NAME = ?", dbName, tblName)
	var (
		tableID   int64
		indexType ClusteredIndexType
	)
	if err != nil {
		if driverErr, ok := err.(*mysql.MySQLError); !ok || driverErr.Number != 1054 {
			return 0, ClusteredIndexInt64, err
		}
		// Backward compatibility for TiDB v4.x that all primary key are not clustered
		indexType = ClusteredIndexInt64
		rows, err := c.Db.Query("select `TIDB_TABLE_ID` from information_schema.tables where TABLE_SCHEMA = ? and TABLE_NAME = ?", dbName, tblName)
		if err != nil {
			return 0, ClusteredIndexInt64, err
		}
		for rows.Next() {
			rows.Scan(&tableID)
		}
	} else {
		// For TiDB 5.x and later
		var tidbPKType string
		for rows.Next() {
			rows.Scan(&tableID, &tidbPKType)
		}

		switch tidbPKType {
		case "CLUSTERED":
			indexType = ClusteredIndexCommon
		case "NONCLUSTERED":
			indexType = ClusteredIndexInt64
		default:
			return 0, ClusteredIndexInt64, fmt.Errorf("invalid TIDB_PK_TYPE from information_schema.tables, got: %s for `%s`.`%s`", tidbPKType, dbName, tblName)
		}
	}

	return tableID, indexType, nil
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
