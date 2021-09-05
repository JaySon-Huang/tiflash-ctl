package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/pd"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"

	"github.com/spf13/cobra"
)

type checkRowsOpts struct {
	tidbHost        string
	tidbPort        int
	user            string
	password        string
	dbName          string
	tableName       string
	numReplica      int
	queryLowerBound int64
	queryUpperBound int64
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
	cmd.Flags().StringVar(&opt.tidbHost, "tidb_ip", "127.0.0.1", "A TiDB Instance IP")
	cmd.Flags().IntVar(&opt.tidbPort, "tidb_port", 4000, "The port of TiDB instance")
	cmd.Flags().StringVar(&opt.user, "user", "root", "TiDB user")
	cmd.Flags().StringVar(&opt.password, "password", "", "TiDB user password")

	cmd.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
	cmd.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")
	cmd.Flags().IntVar(&opt.numReplica, "num_replica", 2, "The number of TiFlash replica for the query table")

	cmd.Flags().Int64Var(&opt.queryLowerBound, "lower_bound", 0, "The lower bound of query")
	cmd.Flags().Int64Var(&opt.queryUpperBound, "upper_bound", 0, "The upper bound of query")

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

func setEngine(db *sql.DB, engine string) error {
	sql := "set tidb_isolation_read_engines=" + engine
	_, err := db.Exec(sql)
	return err
}

func getMinMaxTiDBRowID(db *sql.DB, database, table string, engine string) (int64, int64) {
	if err := setEngine(db, engine); err != nil {
		panic(err)
	}
	start := time.Now()
	sql := fmt.Sprintf("select min(_tidb_rowid), max(_tidb_rowid) from `%s`.`%s`", database, table)
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	var (
		minRowID int64
		maxRowID int64
	)
	for rows.Next() {
		rows.Scan(&minRowID, &maxRowID)
	}
	end := time.Now()
	fmt.Printf("%s => %dms (%s)\n", sql, end.Sub(start).Milliseconds(), engine)
	return minRowID, maxRowID
}

func getNumOfRows(db *sql.DB, database, table string, engine string, checkRange QueryRange) uint64 {
	if err := setEngine(db, engine); err != nil {
		panic(err)
	}
	start := time.Now()
	sql := fmt.Sprintf("select count(*) from `%s`.`%s` %s", database, table, checkRange.toWhereFilter())
	rows, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	var count uint64
	for rows.Next() {
		rows.Scan(&count)
	}
	end := time.Now()
	fmt.Printf("%s => %dms (%s)\n", sql, end.Sub(start).Milliseconds(), engine)
	return count
}

type QueryRange struct {
	min    int64
	max    int64
	minInf bool
	maxInf bool
}

func NewMinMax(min, max int64) QueryRange {
	return QueryRange{min: min, max: max, minInf: false, maxInf: false}
}

func NewAll() QueryRange {
	return QueryRange{minInf: true, maxInf: true}
}

func NewMinMaxFrom(min int64) QueryRange {
	return QueryRange{min: min, maxInf: true}
}

func NewMinMaxTo(max int64) QueryRange {
	return QueryRange{max: max, minInf: true}
}

func (m QueryRange) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	if m.minInf {
		buffer.WriteString("-Inf")
	} else {
		buffer.WriteString(strconv.FormatInt(m.min, 10))
	}
	buffer.WriteString(", ")
	if m.maxInf {
		buffer.WriteString("+Inf")
	} else {
		buffer.WriteString(strconv.FormatInt(m.max, 10))
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (m *QueryRange) toWhereFilter() string {
	var buffer bytes.Buffer
	if m.minInf && m.maxInf {
		return buffer.String()
	}
	buffer.WriteString("where ")
	if !m.minInf {
		buffer.WriteString(strconv.FormatInt(m.min, 10))
		buffer.WriteString(" <= _tidb_rowid")
		if !m.maxInf {
			buffer.WriteString(" and ")
		}
	}
	if !m.maxInf {
		buffer.WriteString("_tidb_rowid < ")
		buffer.WriteString(strconv.FormatInt(m.max, 10))
	}
	return buffer.String()
}

func getCheckRangeFromRegion(region *pd.Region) (QueryRange, error) {
	l, _ := tidb.FromPDKey(region.StartKey)
	r, _ := tidb.FromPDKey(region.EndKey)
	// fmt.Printf("low:%s, high:%s\n", l.GetPDKey(), r.GetPDKey())
	lRow, err := l.GetTableRow()
	if err != nil {
		return QueryRange{}, err
	}
	rRow, err := r.GetTableRow()
	if err != nil {
		return QueryRange{}, err
	}
	// fmt.Printf("low:%v, high:%v\n", lRow, rRow)
	var queryRange QueryRange
	if lRow.Status == tidb.MinInf && rRow.Status == tidb.MaxInf {
		queryRange = NewAll()
	} else if lRow.Status == tidb.MinInf {
		queryRange = NewMinMaxTo(rRow.RowID)
	} else if rRow.Status == tidb.MaxInf {
		queryRange = NewMinMaxFrom(lRow.RowID)
	} else {
		queryRange = NewMinMax(lRow.RowID, rRow.RowID)
	}
	// fmt.Printf("%v\n", queryRange)
	return queryRange, nil
}

func haveConsistNumOfRows(db *sql.DB, database, table string, queryRange QueryRange, numCheckTimes int) bool {
	var numRowsTiKV uint64 = 0
	var numRowsTiFlash uint64 = 0
	for i := 0; i < numCheckTimes && numRowsTiKV == numRowsTiFlash; i++ {
		numRowsTiKV = getNumOfRows(db, database, table, "tikv", queryRange)
		numRowsTiFlash = getNumOfRows(db, database, table, "tiflash", queryRange)
	}

	if numRowsTiKV != numRowsTiFlash {
		fmt.Printf("Range %s, num of rows: tikv %d, tiflash %d. FAIL\n", queryRange.String(), numRowsTiKV, numRowsTiFlash)
	} else {
		fmt.Printf("Range %s, num of rows: tikv %d, tiflash %d. OK\n", queryRange.String(), numRowsTiKV, numRowsTiFlash)
	}
	return numRowsTiKV == numRowsTiFlash
}

func getInitQueryRange(db *sql.DB, opts checkRowsOpts) []QueryRange {
	var queryRanges []QueryRange
	if opts.queryLowerBound == 0 && opts.queryUpperBound == 0 {
		tikvMinID, tikvMaxID := getMinMaxTiDBRowID(db, opts.dbName, opts.tableName, "tikv")
		tiflashMinID, tiflashMaxID := getMinMaxTiDBRowID(db, opts.dbName, opts.tableName, "tiflash")

		fmt.Printf("RowID range: [%d, %d] (tikv)\n", tikvMinID, tikvMaxID)
		fmt.Printf("RowID range: [%d, %d] (tiflash)\n", tiflashMinID, tiflashMaxID)
		if tikvMinID != tiflashMinID {
			panic(fmt.Sprintf("tikv min id %d != tiflash min id %d", tikvMinID, tiflashMinID))
		}
		if tikvMaxID != tiflashMaxID {
			panic(fmt.Sprintf("tikv max id %d != tiflash max id %d", tikvMaxID, tiflashMaxID))
		}

		queryRanges = append(queryRanges, NewMinMax(tikvMinID, tikvMaxID+1))
	} else if opts.queryLowerBound != 0 && opts.queryUpperBound != 0 {
		queryRanges = append(queryRanges, NewMinMax(opts.queryLowerBound, opts.queryUpperBound))
	} else if opts.queryLowerBound != 0 {
		queryRanges = append(queryRanges, NewMinMaxFrom(opts.queryLowerBound))
	} else if opts.queryUpperBound != 0 {
		queryRanges = append(queryRanges, NewMinMaxTo(opts.queryUpperBound))
	}
	return queryRanges
}

func CheckRows(opts checkRowsOpts) error {
	client, err := tidb.NewClient(opts.tidbHost, int32(opts.tidbPort), opts.user, opts.password)
	if err != nil {
		return err
	}
	defer client.Close()

	execSQL(client.Db, "set tidb_allow_batch_cop = 0")
	execSQL(client.Db, "set tidb_allow_mpp = 0")

	queryRanges := getInitQueryRange(client.Db, opts)
	fmt.Printf("Init query ranges: %s\n", queryRanges)

	var (
		curRange          QueryRange
		curRangeIsConsist bool
	)
	for len(queryRanges) > 0 {
		curRange = queryRanges[0]
		min, max := queryRanges[0].min, queryRanges[0].max
		queryRanges = queryRanges[1:]

		if !haveConsistNumOfRows(client.Db, opts.dbName, opts.tableName, curRange, opts.numReplica) {
			queryRanges = nil
			mid := min + (max-min)/2
			if mid > min && mid < max {
				queryRanges = append(queryRanges, NewMinMax(min, mid), NewMinMax(mid, max))
			}
			fmt.Printf("New query ranges: %v\n", queryRanges)
			curRangeIsConsist = false
		} else {
			curRangeIsConsist = true
		}
	}

	if curRangeIsConsist {
		return nil
	}

	fmt.Printf("\n========\nChecking the rows of Region\n")
	pdInstances := client.GetInstances("pd")
	pdClient := pd.NewPDClient(pdInstances[0]) // FIXME: can not get instances

	tableID := client.GetTableID(opts.dbName, opts.tableName)
	checkKey := tidb.NewTableRowAsKey(tableID, curRange.min)
	fmt.Printf("table id: %d, min: %s\n", tableID, checkKey.GetPDKey())

	err = checkRowsByKey(client.Db, opts, &pdClient, checkKey)

	return err
}

func checkRowsByKey(db *sql.DB, opts checkRowsOpts, pdClient *pd.Client, key tidb.TiKVKey) error {
	numSuccess := 0
	for {
		region, err := pdClient.GetRegionByKey(key)
		if err != nil {
			return err
		}
		queryRange, err := getCheckRangeFromRegion(&region)
		if err != nil {
			return err
		}
		fmt.Printf("The query range of Region %d is %s\n", region.Id, queryRange.String())
		isConsist := haveConsistNumOfRows(db, opts.dbName, opts.tableName, queryRange, opts.numReplica)
		if isConsist {
			numSuccess += 1
			fmt.Printf("Region %v have consist num of rows\n", region)
			if numSuccess > 20 {
				break
			}
		} else {
			numSuccess = 0
			fmt.Printf("Region %v have not consist num of rows\n", region)
			for _, storeID := range region.GetLearnerIDs() {
				fmt.Printf("operator add remove-peer %d %d\n", region.Id, storeID)
			}
		}
		if key, err = tidb.FromPDKey(region.EndKey); err != nil {
			return err
		}
	}
	return nil
}
