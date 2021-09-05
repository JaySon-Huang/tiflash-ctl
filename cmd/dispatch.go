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
	tidbHost        string
	tidbPort        int
	tiflashHttpPort int
	user            string
	password        string
	dbName          string
	tableName       string
}

type AnyCmdOpts struct {
	tidbHost        string
	tidbPort        int
	tiflashHttpPort int
	user            string
	password        string
	flashCmd        string
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
	newGetRegionCmd := func() *cobra.Command {
		var opt fetchRegionsOpts
		getRegionCmd := &cobra.Command{
			Use:   "fetch_region",
			Short: "Fetch Regions info for each TiFlash server",
			RunE: func(cmd *cobra.Command, args []string) error {
				return dumpTiFlashRegionInfo(opt)
			},
		}
		// Flags for "fetch region"
		getRegionCmd.Flags().StringVar(&opt.tidbHost, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
		getRegionCmd.Flags().IntVar(&opt.tidbPort, "tidb_port", 4000, "The port of TiDB instance")
		// getRegionCmd.Flags().IntVar(&tidb_status_port, "tidb_status_port", 10080, "The status port of TiDB instance")
		getRegionCmd.Flags().IntVar(&opt.tiflashHttpPort, "tiflash_http_port", 8123, "The port of TiFlash instance")
		getRegionCmd.Flags().StringVar(&opt.user, "user", "root", "TiDB user")
		getRegionCmd.Flags().StringVar(&opt.password, "password", "", "TiDB user password")

		getRegionCmd.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
		getRegionCmd.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")
		return getRegionCmd
	}

	/// TODO: Apply delta merge for a table for all TiFlash instances

	newAnyCmd := func() *cobra.Command {
		var opt AnyCmdOpts
		anyCmd := &cobra.Command{
			Use:   "any",
			Short: "Any command",
			RunE: func(cmd *cobra.Command, args []string) error {
				return execAnyTiFlashCmd(opt)
			},
		}
		// Flags for "fetch region"
		anyCmd.Flags().StringVar(&opt.tidbHost, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
		anyCmd.Flags().IntVar(&opt.tidbPort, "tidb_port", 4000, "The port of TiDB instance")
		anyCmd.Flags().IntVar(&opt.tiflashHttpPort, "tiflash_http_port", 8123, "The port of TiFlash instance")
		anyCmd.Flags().StringVar(&opt.user, "user", "root", "TiDB user")
		anyCmd.Flags().StringVar(&opt.password, "password", "", "TiDB user password")

		anyCmd.Flags().StringVar(&opt.flashCmd, "cmd", "", "The command executed in all TiFlash")
		return anyCmd
	}

	cmd.AddCommand(newGetRegionCmd(), newAnyCmd())

	return cmd
}

func getInstances(db *sql.DB, selectTypes string) []string {
	rows, err := db.Query("select TYPE,INSTANCE,STATUS_ADDRESS from information_schema.cluster_info where TYPE=?", selectTypes)
	if err != nil {
		panic(err)
	}
	var (
		instanceType string
		instance     string
		statusAddr   string

		retInstances []string
	)
	for rows.Next() {
		rows.Scan(&instanceType, &instance, &statusAddr)
		retInstances = append(retInstances, instance)
	}
	return retInstances
}

func getIPs(instances []string) []string {
	var IPs []string
	for _, s := range instances {
		sp := strings.Split(s, ":")
		IPs = append(IPs, sp[0])
	}
	return IPs
}

func curlTiFlash(ip string, httpPort int, query string) error {
	reqBodyReader := strings.NewReader(query)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/post", ip, httpPort), "text/html", reqBodyReader)
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
	if opts.dbName == "" || opts.tableName == "" {
		return fmt.Errorf("should set the database name and table name for running")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", opts.user, opts.password, opts.tidbHost, opts.tidbPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect to database fail: %s", err)
	}
	defer db.Close()
	fmt.Println("Connect to database succ: ", dsn)

	instances := getInstances(db, "tiflash")
	ips := getIPs(instances)

	tableID := getTableID(db, opts.dbName, opts.tableName)
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d table: `%s`.`%s` table_id: %d; Dumping Regions of table\n", ip, opts.tiflashHttpPort, opts.dbName, opts.tableName, tableID)
		err = curlTiFlash(ip, opts.tiflashHttpPort, fmt.Sprintf("DBGInvoke dump_all_region(%d)", tableID))
		fmt.Printf("err: %v", err)
	}
	return nil
}

func execAnyTiFlashCmd(opts AnyCmdOpts) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", opts.user, opts.password, opts.tidbHost, opts.tidbPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect to database fail: %s", err)
	}
	defer db.Close()
	fmt.Println("Connect to database succ: ", dsn)

	instances := getInstances(db, "tiflash")
	ips := getIPs(instances)

	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d\n", ip, opts.tiflashHttpPort)
		if err = curlTiFlash(ip, opts.tiflashHttpPort, opts.flashCmd); err != nil {
			fmt.Printf("err: %v\n", err)
		}
	}

	return nil
}
