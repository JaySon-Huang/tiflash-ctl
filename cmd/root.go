package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd *cobra.Command
)

func init() {
	cobra.EnableCommandSorting = false

	rootCmd = &cobra.Command{
		Use:   "tiflash-ctl",
		Short: "TiFlash Controller",
		Long:  "TiFlash Controller (tiflash-ctl) is a command line tool for TiFlash Server",
	}
	rootCmd.AddCommand(newDispatchCmd(), newCheckCmd())
}

func getTableID(db *sql.DB, db_name, tbl_name string) int64 {
	rows, err := db.Query("select TABLE_ID from information_schema.tiflash_replica where TABLE_SCHEMA = ? and TABLE_NAME = ?", db_name, tbl_name)
	if err != nil {
		panic(err)
	}
	var table_id int64
	for rows.Next() {
		rows.Scan(&table_id)
	}
	return table_id
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	/*
		var tidb_host string = "127.0.0.1"
		var tidb_port int = 4000
		// var tidb_status_port int = 10080
		var tiflash_http_port int = 8123
		var user string = "root"
		var password string = ""
		var db_name string = "tpch_100"
		var table_name string = "lineitem"

		flag.StringVar(&tidb_host, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
		flag.IntVar(&tidb_port, "tidb_port", 4000, "The port of TiDB instance")
		// flag.IntVar(&tidb_status_port, "tidb_status_port", 10080, "The status port of TiDB instance")
		flag.IntVar(&tiflash_http_port, "tiflash_http_port", 8123, "The port of TiFlash instance")
		flag.StringVar(&user, "user", "root", "TiDB user")
		flag.StringVar(&password, "password", "", "TiDB user password")

		flag.StringVar(&db_name, "database", "", "The database name of query table")
		flag.StringVar(&table_name, "table", "", "The table name of query table")

		flag.Parse()

		conn_cmd := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", user, password, tidb_host, tidb_port)
		db, err := sql.Open("mysql", conn_cmd)
		if err != nil {
			fmt.Println("Connect database fail: ", err)
			return
		}
		defer db.Close()
		fmt.Println("Connect to database succ: ", conn_cmd)

		DumpTiflashRegionInfo(db, tiflash_http_port, db_name, table_name)
	*/
}
