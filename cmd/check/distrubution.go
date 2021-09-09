package check

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/options"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func NewDistributionCmd() *cobra.Command {
	var opt checkDistributionOpts
	c := &cobra.Command{
		Use:   "dist",
		Short: "Check the Region distribution of a table",
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkDistribution(cmd, opt)
		},
	}
	// Flags for "dist"
	options.AddTiDBConnFlags(c, &opt.tidb)

	c.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
	c.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")

	c.Flags().BoolVar(&opt.dryRun, "dry", false, "Only print the distribution query text")
	return c
}

type checkDistributionOpts struct {
	tidb      tidb.TiDBClientOpts
	dbName    string
	tableName string
	dryRun    bool
}

func checkDistribution(cmd *cobra.Command, opts checkDistributionOpts) error {
	if len(opts.dbName) == 0 || len(opts.tableName) == 0 {
		fmt.Println("You must set the database and table name")
		return cmd.Help()
	}

	if opts.dryRun {
		sql := getDistQuery(opts.dbName, opts.tableName)
		fmt.Println(strings.ReplaceAll(strings.ReplaceAll(sql, "\t", ""), "\n", " "))
		return nil
	}

	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	dists, err := execGetDist(client.Db, opts.dbName, opts.tableName)
	if err != nil {
		return err
	}

	avgTiKVLeaderRegions, avgTiKVFollowerRegions, avgTiFlashRegions := getDistAvg(dists)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"store type", "store id", "address", "is leader", "num regions", "diff per"})

	for _, v := range dists {
		if v.storeType == "tikv" {
			if v.isLeader {
				table.Append(v.toRow(avgTiKVLeaderRegions))
			} else {
				table.Append(v.toRow(avgTiKVFollowerRegions))
			}
		} else if v.storeType == "tiflash" {
			table.Append(v.toRow(avgTiFlashRegions))
		}
	}
	table.Render() // Send output

	return nil
}

func getDistQuery(database, table string) string {
	return fmt.Sprintf(`select c.type, a.store_id, a.address, a.db_name, a.table_name, a.is_leader, a.cnt 
from (
	select r.db_name, r.table_name, r.store_id, s.address, r.is_leader, count(*) as cnt
	from (
		select s.region_id, s.db_name, s.table_name, p.store_id, p.is_leader, p.status 
		from
			information_schema.tikv_region_status s,
			information_schema.tikv_region_peers p
		where 1=1
			and db_name ='%s' and table_name='%s'
			and s.region_id = p.region_id
		order by p.store_id
		) as r,
		information_schema.tikv_store_status s
	where r.store_id=s.store_id
	group by
		r.db_name, r.table_name, r.store_id, r.is_leader, s.address
) a, information_schema.cluster_info c
where c.instance = a.address
order by c.type desc, a.store_id;`, database, table)
}

type distribution struct {
	storeType  string
	storeId    int64
	address    string
	dbName     string
	tableName  string
	isLeader   bool
	numRegions int64
}

func (d *distribution) toRow(avg float32) []string {
	s := make([]string, 0)
	s = append(s, d.storeType)
	s = append(s, strconv.FormatInt(d.storeId, 10))
	s = append(s, d.address)
	s = append(s, strconv.FormatBool(d.isLeader))
	s = append(s, strconv.FormatInt(d.numRegions, 10))
	diff := (float32(d.numRegions) - avg) / avg * 100
	s = append(s, fmt.Sprintf("%6.2f%%", diff))

	return s
}

func execGetDist(db *sql.DB, database, table string) ([]distribution, error) {
	sql := getDistQuery(database, table)
	rows, err := db.Query(sql)
	if err != nil {
		return nil, err
	}
	var dists []distribution
	var dist distribution
	for rows.Next() {
		rows.Scan(&dist.storeType, &dist.storeId, &dist.address, &dist.dbName, &dist.tableName, &dist.isLeader, &dist.numRegions)
		dists = append(dists, dist)
	}
	return dists, nil
}

func getDistAvg(dists []distribution) (float32, float32, float32) {
	var (
		sumTiKVFollower float32 = 0.0
		sumTiKVLeader   float32 = 0.0
		sumTiFlashALL   float32 = 0.0
		numTiKVFollower int32   = 0
		numTiKVLeader   int32   = 0
		numTiFlashALL   int32   = 0
	)
	for _, dist := range dists {
		switch dist.storeType {
		case "tikv":
			if dist.isLeader {
				numTiKVLeader += 1
				sumTiKVLeader += float32(dist.numRegions)
			} else {
				numTiKVFollower += 1
				sumTiKVFollower += float32(dist.numRegions)
			}
		case "tiflash":
			numTiFlashALL += 1
			sumTiFlashALL += float32(dist.numRegions)
		}
	}
	return sumTiKVLeader / float32(numTiKVLeader),
		sumTiKVFollower / float32(numTiKVFollower),
		sumTiFlashALL / float32(numTiFlashALL)
}
