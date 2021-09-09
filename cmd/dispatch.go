package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/options"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/spf13/cobra"
)

type FetchRegionsOpts struct {
	tidb            tidb.TiDBClientOpts
	tiflashHttpPort int
	dbName          string
	tableName       string
}

type CompactTableOpts struct {
	tidb            tidb.TiDBClientOpts
	tiflashHttpPort int
	dbName          string
	tableName       string
}

type ExecCmdOpts struct {
	tidb            tidb.TiDBClientOpts
	tiflashHttpPort int
	flashCmd        string
}

func newDispatchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dispatch",
		Short: "Dispatch some actions for each TiFlash server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	/// Fetch Regions info for a table from all TiFlash instances
	newGetRegionCmd := func() *cobra.Command {
		var opt FetchRegionsOpts
		c := &cobra.Command{
			Use:   "fetch_region",
			Short: "Fetch Regions info for each TiFlash server",
			RunE: func(cmd *cobra.Command, args []string) error {
				return dumpTiFlashRegionInfo(opt)
			},
		}
		// Flags for "fetch region"
		options.AddTiDBConnFlags(c, &opt.tidb)
		c.Flags().IntVar(&opt.tiflashHttpPort, "tiflash_http_port", 8123, "The port of TiFlash instance")

		c.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
		c.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")
		return c
	}

	// Apply data compaction (delta merge) for a table for all TiFlash instances
	newCompactCmd := func() *cobra.Command {
		var opt CompactTableOpts
		c := &cobra.Command{
			Use:   "compact",
			Short: "Compact data (delta-merge) on TiFlash servers",
			RunE: func(cmd *cobra.Command, args []string) error {
				return compactDataCmd(opt)
			},
		}
		options.AddTiDBConnFlags(c, &opt.tidb)
		c.Flags().IntVar(&opt.tiflashHttpPort, "tiflash_http_port", 8123, "The port of TiFlash instance")

		c.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
		c.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")
		return c
	}

	newExecCmd := func() *cobra.Command {
		var opt ExecCmdOpts
		c := &cobra.Command{
			Use:   "exec",
			Short: "Exec command",
			RunE: func(cmd *cobra.Command, args []string) error {
				return execTiFlashCmd(opt)
			},
		}
		// Flags for "fetch region"
		options.AddTiDBConnFlags(c, &opt.tidb)
		c.Flags().IntVar(&opt.tiflashHttpPort, "tiflash_http_port", 8123, "The port of TiFlash instance")

		c.Flags().StringVar(&opt.flashCmd, "cmd", "", "The command executed in all TiFlash")
		return c
	}

	cmd.AddCommand(newGetRegionCmd(), newCompactCmd(), newExecCmd())

	return cmd
}

func getTiFlashIPs(client *tidb.Client) []string {
	instances := client.GetInstances("tiflash")
	var IPs []string
	for _, s := range instances {
		sp := strings.Split(s, ":")
		IPs = append(IPs, sp[0])
	}
	return IPs
}

func curlTiFlash(ip string, httpPort int, query string) error {
	// TODO: well-defined http interface that response data in JSON format is better
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

func dumpTiFlashRegionInfo(opts FetchRegionsOpts) error {
	if opts.dbName == "" || opts.tableName == "" {
		return fmt.Errorf("should set the database name and table name for running")
	}

	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	ips := getTiFlashIPs(&client)
	tableID := client.GetTableID(opts.dbName, opts.tableName)
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d table: `%s`.`%s` table_id: %d; Dumping Regions of table\n", ip, opts.tiflashHttpPort, opts.dbName, opts.tableName, tableID)
		// TODO: Find a way to get http port
		err = curlTiFlash(ip, opts.tiflashHttpPort, fmt.Sprintf("DBGInvoke dump_all_region(%d)", tableID))
		fmt.Printf("err: %v", err)
	}
	return nil
}

func compactDataCmd(opts CompactTableOpts) error {
	if opts.dbName == "" || opts.tableName == "" {
		return fmt.Errorf("should set the database name and table name for running")
	}

	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	ips := getTiFlashIPs(&client)
	databaseID := 0 // FIXME: get the id of database
	tableID := client.GetTableID(opts.dbName, opts.tableName)
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d table: `%s`.`%s` table_id: %d; Dumping Regions of table\n", ip, opts.tiflashHttpPort, opts.dbName, opts.tableName, tableID)
		// TODO: Find a way to get http port
		err = curlTiFlash(ip, opts.tiflashHttpPort, fmt.Sprintf("manage table `%d`.`%d` merge delta", databaseID, tableID))
		fmt.Printf("err: %v", err)
	}

	return nil
}

func execTiFlashCmd(opts ExecCmdOpts) error {
	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	ips := getTiFlashIPs(&client)
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d\n", ip, opts.tiflashHttpPort)
		// TODO: Find a way to get http port
		if err = curlTiFlash(ip, opts.tiflashHttpPort, opts.flashCmd); err != nil {
			fmt.Printf("err: %v\n", err)
		}
	}

	return nil
}
