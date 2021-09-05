package cmd

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

type fetchRegionsOpts struct {
	tidb_host         string
	tidb_port         int
	tiflash_http_port int
	user              string
	password          string
	db_name           string
	table_name        string
}

func newDispatchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dispatch",
		Short: "Dispatch some actions for each TiFlash server",
		RunE: func(cmd *cobra.Command, args []string) error {
			switch len(args) {
			default:
				return cmd.Help()
			}
		},
	}

	/// Fetch Regions info for a table from all TiFlash instances
	var opt fetchRegionsOpts
	getRegionCmd := &cobra.Command{
		Use:   "fetch_region",
		Short: "Fetch Regions info for each TiFlash server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return dumpTiFlashRegionInfo(opt)
		},
	}
	// Flags for "fetch region"
	getRegionCmd.Flags().StringVar(&opt.tidb_host, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
	getRegionCmd.Flags().IntVar(&opt.tidb_port, "tidb_port", 4000, "The port of TiDB instance")
	// getRegionCmd.Flags().IntVar(&tidb_status_port, "tidb_status_port", 10080, "The status port of TiDB instance")
	getRegionCmd.Flags().IntVar(&opt.tiflash_http_port, "tiflash_http_port", 8123, "The port of TiFlash instance")
	getRegionCmd.Flags().StringVar(&opt.user, "user", "root", "TiDB user")
	getRegionCmd.Flags().StringVar(&opt.password, "password", "", "TiDB user password")

	getRegionCmd.Flags().StringVar(&opt.db_name, "database", "", "The database name of query table")
	getRegionCmd.Flags().StringVar(&opt.table_name, "table", "", "The table name of query table")

	/// TODO: Apply delta merge for a table for all TiFlash instances

	cmd.AddCommand(getRegionCmd)

	return cmd
}

func getInstances(db *sql.DB, select_types string) []string {
	rows, err := db.Query("select TYPE,INSTANCE,STATUS_ADDRESS from information_schema.cluster_info where TYPE=?", select_types)
	if err != nil {
		panic(err)
	}
	var (
		instance_type string
		instance      string
		status_addr   string

		ret_instances []string
	)
	for rows.Next() {
		rows.Scan(&instance_type, &instance, &status_addr)
		ret_instances = append(ret_instances, instance)
	}
	return ret_instances
}

func getIPs(instances []string) []string {
	var IPs []string
	for _, s := range instances {
		sp := strings.Split(s, ":")
		IPs = append(IPs, sp[0])
	}
	return IPs
}

func curlTiFlash(ip string, http_port int, query string) error {
	req_body_reader := strings.NewReader(query)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/post", ip, http_port), "text/html", req_body_reader)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func dumpTiFlashRegionInfo(opts fetchRegionsOpts) error {
	if opts.db_name == "" || opts.table_name == "" {
		return fmt.Errorf("should set the database name and table name for running")
	}

	conn_cmd := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", opts.user, opts.password, opts.tidb_host, opts.tidb_port)
	db, err := sql.Open("mysql", conn_cmd)
	if err != nil {
		return fmt.Errorf("connect to database fail: %s", err)
	}
	defer db.Close()
	fmt.Println("Connect to database succ: ", conn_cmd)

	instances := getInstances(db, "tiflash")
	ips := getIPs(instances)

	table_id := getTableID(db, opts.db_name, opts.table_name)
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s table: `%s`.`%s` table_id: %d; Dumping Regions of table\n", ip, opts.db_name, opts.table_name, table_id)
		err = curlTiFlash(ip, opts.tiflash_http_port, fmt.Sprintf("DBGInvoke dump_all_region(%d)", table_id))
		fmt.Printf("err: %v", err)
	}
	return nil
}
