package cmd

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

type checkRowsOpts struct {
	tidb_host   string
	tidb_port   int
	user        string
	password    string
	db_name     string
	table_name  string
	num_replica int
}

func newCheckCmd() *cobra.Command {
	var opt checkRowsOpts
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check the consistent betweeen TiKV && TiFlash",
		RunE: func(cmd *cobra.Command, args []string) error {
			return CheckRows(opt)
		},
	}
	// Flags for "check"
	cmd.Flags().StringVar(&opt.tidb_host, "tidb_ip", "127.0.0.1", "A TiDB Instance IP")
	cmd.Flags().IntVar(&opt.tidb_port, "tidb_port", 4000, "The port of TiDB instance")
	cmd.Flags().StringVar(&opt.user, "user", "root", "TiDB user")
	cmd.Flags().StringVar(&opt.password, "password", "", "TiDB user password")

	cmd.Flags().StringVar(&opt.db_name, "database", "", "The database name of query table")
	cmd.Flags().StringVar(&opt.table_name, "table", "", "The table name of query table")
	cmd.Flags().IntVar(&opt.num_replica, "num_replica", 2, "The number of TiFlash replica for the query table")

	return cmd
}

func execSQL(db *sql.DB, sql string) {
	start := time.Now()
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	end := time.Now()
	fmt.Printf("%s => %dms\n", sql, end.Sub(start).Milliseconds())
}

func setEngine(db *sql.DB, engine string) {
	start := time.Now()
	sql := "set tidb_isolation_read_engines=" + engine
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	end := time.Now()
	fmt.Printf("%s => %dms\n", sql, end.Sub(start).Milliseconds())
}

func getMinMaxTiDBRowID(db *sql.DB, database_name, table_name string, engine string) (int64, int64) {
	setEngine(db, engine)
	start := time.Now()
	sql := fmt.Sprintf("select min(_tidb_rowid), max(_tidb_rowid) from `%s`.`%s`", database_name, table_name)
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	var (
		min_row_id int64
		max_row_id int64
	)
	for rows.Next() {
		rows.Scan(&min_row_id, &max_row_id)
	}
	end := time.Now()
	fmt.Printf("%s => %dms\n", sql, end.Sub(start).Milliseconds())
	return min_row_id, max_row_id
}

func getNumOfRows(db *sql.DB, database_name, table_name string, engine string, min int64, max int64) uint64 {
	setEngine(db, engine)
	start := time.Now()
	sql := fmt.Sprintf("select count(*) from `%s`.`%s` where %d <= _tidb_rowid and _tidb_rowid < %d", database_name, table_name, min, max)
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	var count uint64
	for rows.Next() {
		rows.Scan(&count)
	}
	end := time.Now()
	fmt.Printf("%s => %dms\n", sql, end.Sub(start).Milliseconds())
	return count
}

type MinMax struct {
	min int64
	max int64
}

func CheckRows(opts checkRowsOpts) error {
	conn_cmd := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", opts.user, opts.password, opts.tidb_host, opts.tidb_port)
	db, err := sql.Open("mysql", conn_cmd)
	if err != nil {
		return fmt.Errorf("connect to database fail: %s", err)
	}
	defer db.Close()
	fmt.Println("Connect to database succ: ", conn_cmd)

	execSQL(db, "set tidb_allow_batch_cop = 0")
	execSQL(db, "set tidb_allow_mpp = 0")

	tikv_min_id, tikv_max_id := getMinMaxTiDBRowID(db, opts.db_name, opts.table_name, "tikv")
	tiflash_min_id, tiflash_max_id := getMinMaxTiDBRowID(db, opts.db_name, opts.table_name, "tiflash")

	fmt.Printf("tikv min %d max %d\n", tikv_min_id, tikv_max_id)
	fmt.Printf("tiflash min %d max %d\n", tiflash_min_id, tiflash_max_id)

	if tikv_min_id != tiflash_min_id {
		panic(fmt.Sprintf("tikv_min_id %d != tiflash_min_id %d", tikv_min_id, tiflash_min_id))
	}
	if tikv_max_id != tiflash_max_id {
		panic(fmt.Sprintf("tikv_max_id %d != tiflash_max_id %d", tikv_max_id, tiflash_max_id))
	}

	var min_max_list []MinMax
	min_max_list = append(min_max_list, MinMax{tikv_min_id, tikv_max_id + 1})
	fmt.Println(min_max_list)

	for len(min_max_list) > 0 {
		min := min_max_list[0].min
		max := min_max_list[0].max
		min_max_list = min_max_list[1:]

		var tikv_count uint64 = 0
		var tiflash_count uint64 = 0
		for i := 0; i < opts.num_replica && tikv_count == tiflash_count; i++ {
			tikv_count = getNumOfRows(db, opts.db_name, opts.table_name, "tikv", min, max)
			tiflash_count = getNumOfRows(db, opts.db_name, opts.table_name, "tiflash", min, max)
		}

		if tikv_count != tiflash_count {
			min_max_list = nil
			mid := (min + max) / 2
			if mid > min && mid < max {
				min_max_list = append(min_max_list, MinMax{min, mid}, MinMax{mid, max})
			}
			fmt.Printf("min %d max %d tikv_count %d tiflash_count %d => new range %v\n", min, max, tikv_count, tiflash_count, min_max_list)
		} else {
			fmt.Printf("min %d max %d tikv_count %d tiflash_count %d OK\n", min, max, tikv_count, tiflash_count)
		}
	}

	return nil
}
