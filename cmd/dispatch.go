package cmd

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/logutil"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/options"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	kvConfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

type FetchRegionsOpts struct {
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

type ExecSQLCmdOpts struct {
	pdAddr    string
	flashAddr string
	flashSQL  string
	decimal   uint32
	sslCA     string
	sslCert   string
	sslKey    string
}

type CompactCmdOpts struct {
	pdAddr          string
	flashAddr       string
	physicalTableId int64
	startKey        string
	sslCA           string
	sslCert         string
	sslKey          string
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

	/// TODO: Apply delta merge for a table for all TiFlash instances

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

	newExecSQLCmd := func() *cobra.Command {
		var opt ExecSQLCmdOpts
		c := &cobra.Command{
			Use:   "exec_sql",
			Short: "Exec SQL command",
			RunE: func(cmd *cobra.Command, args []string) error {
				if opt.flashSQL == "" {
					return fmt.Errorf("should set the command to execute")
				}
				return execTiFlashSQLCmd(opt)
			},
		}
		c.Flags().StringVar(&opt.pdAddr, "pd", "127.0.0.1:2379", "pd address")
		c.Flags().StringVar(&opt.flashAddr, "flash", "127.0.0.1:3930", "TiFlash address for SQL execution")
		c.Flags().StringVar(&opt.flashSQL, "sql", "", "The SQL command to execute in TiFlash")
		c.Flags().Uint32Var(&opt.decimal, "decimal", 3, "The decimal precision for floating point values in the output")
		c.Flags().StringVar(&opt.sslCA, "ca", "", "Path to the CA certificate file for TLS")
		c.Flags().StringVar(&opt.sslCert, "cert", "", "Path to the client certificate file for TLS")
		c.Flags().StringVar(&opt.sslKey, "key", "", "Path to the client key file for TLS")
		return c
	}

	newCompactCmd := func() *cobra.Command {
		var opt CompactCmdOpts
		c := &cobra.Command{
			Use:   "compact",
			Short: "Compact a table in TiFlash",
			RunE: func(cmd *cobra.Command, args []string) error {
				if opt.physicalTableId == 0 {
					return fmt.Errorf("should set the physical table id to compact")
				}
				return compactTiFlashTable(opt)
			},
		}
		c.Flags().StringVar(&opt.pdAddr, "pd", "127.0.0.1:2379", "pd address")
		c.Flags().StringVar(&opt.flashAddr, "flash", "127.0.0.1:3930", "TiFlash address for SQL execution")
		c.Flags().Int64Var(&opt.physicalTableId, "table_id", 0, "The physical table ID to compact in TiFlash")
		c.Flags().StringVar(&opt.startKey, "start_key", "", "The start key for compacting the table, in hex format")
		c.Flags().StringVar(&opt.sslCA, "ca", "", "Path to the CA certificate file for TLS")
		c.Flags().StringVar(&opt.sslCert, "cert", "", "Path to the client certificate file for TLS")
		c.Flags().StringVar(&opt.sslKey, "key", "", "Path to the client key file for TLS")
		return c
	}

	cmd.AddCommand(newGetRegionCmd(), newExecCmd(), newExecSQLCmd(), newCompactCmd())

	return cmd
}

func getTiFlashIPs(client *tidb.Client) ([]string, error) {
	instances, err := client.GetInstances("tiflash")
	if err != nil {
		return nil, err
	}
	var IPs []string
	for _, s := range instances {
		sp := strings.Split(s, ":")
		IPs = append(IPs, sp[0])
	}
	return IPs, nil
}

func curlTiFlash(ip string, httpPort int, query string) error {
	// TODO: well-defined http interface that response data in JSON format is better
	reqBodyReader := strings.NewReader(query)
	resp, err := http.Post(fmt.Sprintf("http://%s:%d/post", ip, httpPort), "text/html", reqBodyReader)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
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

	ips, err := getTiFlashIPs(&client)
	if err != nil {
		return err
	}
	tableID, err := client.GetTableID(opts.dbName, opts.tableName)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d table: `%s`.`%s` table_id: %d; Dumping Regions of table\n", ip, opts.tiflashHttpPort, opts.dbName, opts.tableName, tableID)
		// TODO: Find a way to get http port
		if err = curlTiFlash(ip, opts.tiflashHttpPort, fmt.Sprintf("DBGInvoke dump_all_region(%d)", tableID)); err != nil {
			fmt.Printf("err: %v", err)
		}
	}
	return nil
}

func execTiFlashCmd(opts ExecCmdOpts) error {
	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	ips, err := getTiFlashIPs(&client)
	if err != nil {
		return err
	}
	for _, ip := range ips {
		fmt.Printf("TiFlash ip: %s:%d\n", ip, opts.tiflashHttpPort)
		// TODO: Find a way to get http port
		if err = curlTiFlash(ip, opts.tiflashHttpPort, opts.flashCmd); err != nil {
			fmt.Printf("err: %v\n", err)
		}
	}

	return nil
}

type tiFlashSQLExecuteResponseMetaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type tiFlashSQLExecuteResponse struct {
	Meta []tiFlashSQLExecuteResponseMetaColumn `json:"meta"`
	Data [][]any                               `json:"data"`
}

func execTiFlashSQLCmd(opts ExecSQLCmdOpts) error {
	cfg := kvConfig.GetGlobalConfig()
	cfg.Security = kvConfig.NewSecurity(opts.sslCA, opts.sslCert, opts.sslKey, []string{})
	kvConfig.StoreGlobalConfig(cfg)

	client, err := txnkv.NewClient([]string{opts.pdAddr})
	if err != nil {
		return fmt.Errorf("failed to create TiFlash client: %w", err)
	}

	ctx := context.Background()
	timeout := time.Duration(5*60) * time.Second
	req := tikvrpc.Request{
		Type:    tikvrpc.CmdGetTiFlashSystemTable,
		StoreTp: tikvrpc.TiFlash,
		Req: &kvrpcpb.TiFlashSystemTableRequest{
			Sql: opts.flashSQL,
		},
	}
	resp, err := client.KVStore.GetTiKVClient().SendRequest(ctx, opts.flashAddr, &req, timeout)
	if err != nil {
		return fmt.Errorf("failed to send request to TiFlash: %w", err)
	}
	tiflashResp, ok := resp.Resp.(*kvrpcpb.TiFlashSystemTableResponse)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", resp.Resp)
	}
	logutil.BgLogger().Debug("response", zap.String("response", tiflashResp.String()))
	// Parse the response data to be more user-friendly
	var result tiFlashSQLExecuteResponse
	err = json.Unmarshal(tiflashResp.Data, &result)
	if err != nil {
		return fmt.Errorf("failed to unmarshal TiFlash response data: %w", err)
	}

	/// Output as csv format, output to console
	w := csv.NewWriter(os.Stdout)

	// header
	header := make([]string, len(result.Meta))
	for i, col := range result.Meta {
		header[i] = col.Name
	}
	w.Write(header)

	// rows
	floatFormat := fmt.Sprintf("%%.%df", opts.decimal)
	for _, rowFields := range result.Data {
		if len(rowFields) == 0 {
			continue
		}
		outputRow := make([]string, len(rowFields))
		for colIdx, fieldVal := range rowFields {
			if fieldVal == nil {
				outputRow[colIdx] = "NULL"
				continue
			}
			switch result.Meta[colIdx].Type {
			case "Float64", "Float32":
				valStr := fmt.Sprintf(floatFormat, fieldVal)
				outputRow[colIdx] = valStr
			case "String", "Int64", "UInt64":
				valStr := fmt.Sprintf("%s", fieldVal)
				outputRow[colIdx] = valStr
			default:
				// for other types, just convert to string
				valStr := fmt.Sprintf("%s", fieldVal)
				outputRow[colIdx] = valStr
			}
		}

		if err = w.Write(outputRow); err != nil {
			return fmt.Errorf("failed to write row to CSV: %w", err)
		}
	}
	w.Flush()
	return w.Error()
}

func compactTiFlashTable(opts CompactCmdOpts) error {
	cfg := kvConfig.GetGlobalConfig()
	cfg.Security = kvConfig.NewSecurity(opts.sslCA, opts.sslCert, opts.sslKey, []string{})
	kvConfig.StoreGlobalConfig(cfg)

	client, err := txnkv.NewClient([]string{opts.pdAddr})
	if err != nil {
		return fmt.Errorf("failed to create TiFlash client: %w", err)
	}
	ctx := context.Background()
	timeout := time.Duration(5*60) * time.Second

	// Empty start key to compact the whole table
	var startKey []byte
	if len(opts.startKey) > 0 {
		var err error
		startKey, err = hex.DecodeString(opts.startKey)
		if err != nil {
			return fmt.Errorf("failed to decode start key: %w", err)
		}
	}

	tableCompactSuccess := false
	for {
		req := tikvrpc.Request{
			Type:    tikvrpc.CmdCompact,
			StoreTp: tikvrpc.TiFlash,
			Req:     &kvrpcpb.CompactRequest{StartKey: startKey, PhysicalTableId: opts.physicalTableId},
		}
		logutil.BgLogger().Info("Compact TiFlash table",
			zap.Int64("physical_table_id", opts.physicalTableId),
			zap.String("store", opts.flashAddr),
			zap.String("start_key", hex.EncodeToString(startKey)),
		)
		response, err := client.KVStore.GetTiKVClient().SendRequest(ctx, opts.flashAddr, &req, timeout)
		if err != nil {
			logutil.BgLogger().Error("Failed to send request to TiFlash",
				zap.Error(err),
			)
			break
		}
		resp, ok := response.Resp.(*kvrpcpb.CompactResponse)
		if !ok {
			logutil.BgLogger().Error("Unexpected response type from TiFlash",
				zap.String("store", opts.flashAddr),
			)
			break
		}
		if resp.GetError() != nil {
			logutil.BgLogger().Error("Compact failed",
				zap.String("store", opts.flashAddr),
				zap.String("resp", proto.MarshalTextString(resp)),
			)
			break
		}
		if !resp.HasRemaining {
			tableCompactSuccess = true
			logutil.BgLogger().Info("Compact finished",
				zap.Int64("physical_table_id", opts.physicalTableId),
			)
			break
		}
		lastEndKey := resp.GetCompactedEndKey()
		if len(lastEndKey) == 0 {
			logutil.BgLogger().Error("Compact failed, internal error, no end key returned",
				zap.String("store", opts.flashAddr),
			)
			break
		}
		// then continue to compact the next range
		startKey = lastEndKey
		logutil.BgLogger().Info("Next compact range",
			zap.Int64("physical_table_id", opts.physicalTableId),
			zap.String("store", opts.flashAddr),
			zap.String("start_key", hex.EncodeToString(startKey)),
		)
	}

	logutil.BgLogger().Info("Compact command finished", //
		zap.Int64("physical_table_id", opts.physicalTableId), zap.Bool("success", tableCompactSuccess), zap.String("key", hex.EncodeToString(startKey)))
	return nil
}
