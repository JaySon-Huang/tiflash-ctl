package check

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/options"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/pd"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/spf13/cobra"
)

func NewRowConsistencyCmd() *cobra.Command {
	var opt checkRowsOpts
	c := &cobra.Command{
		Use:   "consistency",
		Short: "Check the consistency betweeen TiKV && TiFlash",
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkRows(opt)
		},
	}

	// Flags for "consistency"
	options.AddTiDBConnFlags(c, &opt.tidb)

	c.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
	c.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")
	c.Flags().IntVar(&opt.numReplica, "num_replica", 2, "The number of TiFlash replica for the query table")

	c.Flags().Int64Var(&opt.queryLowerBound, "lower_bound", 0, "The lower bound of query")
	c.Flags().Int64Var(&opt.queryUpperBound, "upper_bound", 0, "The upper bound of query")
	c.Flags().StringVar(&opt.rowIdColName, "row_id_col_name", "_tidb_rowid", "The TiDB row id column name")
	return c
}

type checkRowsOpts struct {
	tidb            tidb.TiDBClientOpts
	dbName          string
	tableName       string
	numReplica      int
	queryLowerBound int64
	queryUpperBound int64
	rowIdColName    string
}

func checkRows(opts checkRowsOpts) error {
	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.ExecWithElapsed("set tidb_allow_batch_cop = 0"); err != nil {
		return err
	}
	if err = client.ExecWithElapsed("set tidb_allow_mpp = 0"); err != nil {
		return err
	}

	queryRanges, err := getInitQueryRange(client.Db, opts)
	if err != nil {
		return err
	}
	fmt.Printf("Init query ranges: %s\n", queryRanges)

	var (
		curRange          QueryRange
		curRangeIsConsist bool
	)
	for len(queryRanges) > 0 {
		curRange = queryRanges[0]
		min, max := queryRanges[0].min, queryRanges[0].max
		queryRanges = queryRanges[1:]

		if ok, err := haveConsistNumOfRows(client.Db, opts.dbName, opts.tableName, opts.rowIdColName, curRange, opts.numReplica); err != nil {
			return err
		} else if ok {
			curRangeIsConsist = true
		} else {
			queryRanges = nil
			mid := min + (max-min)/2
			if mid > min && mid < max {
				queryRanges = append(queryRanges, NewMinMax(min, mid), NewMinMax(mid, max))
			}
			fmt.Printf("New query ranges: %v\n", queryRanges)
			curRangeIsConsist = false
		}
	}

	if curRangeIsConsist {
		return nil
	}

	fmt.Printf("\n========\nChecking the rows of Region\n")
	pdInstances, err := client.GetInstances("pd")
	if err != nil {
		return err
	}
	pdClient := pd.NewPDClient(pdInstances[0]) // FIXME: can not get instances

	tableID, err := client.GetTableID(opts.dbName, opts.tableName)
	if err != nil {
		return err
	}
	checkKey := tidb.NewTableRowAsKey(tableID, curRange.min)
	fmt.Printf("table id: %d, min: %s\n", tableID, checkKey.GetPDKey())

	err = checkRowsByKey(client.Db, opts, &pdClient, checkKey)

	return err
}

func setEngine(db *sql.DB, engine string) error {
	sql := "set tidb_isolation_read_engines=" + engine
	_, err := db.Exec(sql)
	return err
}

func getMinMaxTiDBRowID(db *sql.DB, database, table string, rowIdColName string, engine string) (int64, int64, error) {
	if err := setEngine(db, engine); err != nil {
		return 0, 0, err
	}
	sql := fmt.Sprintf("select min(%s), max(%s) from `%s`.`%s`", rowIdColName, rowIdColName, database, table)
	defer func(start time.Time) {
		elapsed := time.Since(start)
		fmt.Printf("%s => %dms (%s)\n", sql, elapsed.Milliseconds(), engine)
	}(time.Now())

	rows, err := db.Query(sql)
	if err != nil {
		return 0, 0, err
	}
	var (
		minRowID int64
		maxRowID int64
	)
	for rows.Next() {
		rows.Scan(&minRowID, &maxRowID)
	}
	return minRowID, maxRowID, err
}

func getNumOfRows(db *sql.DB, database, table, rowIdColName string, engine string, checkRange QueryRange) (uint64, error) {
	if err := setEngine(db, engine); err != nil {
		return 0, err
	}
	sql := fmt.Sprintf("select count(*) from `%s`.`%s` %s", database, table, checkRange.toWhereFilter(rowIdColName))
	defer func(start time.Time) {
		elapsed := time.Since(start)
		fmt.Printf("%s => %dms (%s)\n", sql, elapsed.Milliseconds(), engine)
	}(time.Now())

	rows, err := db.Query(sql)
	if err != nil {
		return 0, err
	}
	var count uint64
	for rows.Next() {
		rows.Scan(&count)
	}
	return count, nil
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

func (m *QueryRange) toWhereFilter(rowIdColName string) string {
	var buffer bytes.Buffer
	if m.minInf && m.maxInf {
		return buffer.String()
	}
	buffer.WriteString("where ")
	if !m.minInf {
		buffer.WriteString(strconv.FormatInt(m.min, 10))
		buffer.WriteString(" <= " + rowIdColName)
		if !m.maxInf {
			buffer.WriteString(" and ")
		}
	}
	if !m.maxInf {
		buffer.WriteString(rowIdColName + " < ")
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

func haveConsistNumOfRows(db *sql.DB, database, table, rowIdColName string, queryRange QueryRange, numCheckTimes int) (bool, error) {
	var (
		numRowsTiKV    uint64 = 0
		numRowsTiFlash uint64 = 0
		err            error
	)
	for i := 0; i < numCheckTimes && numRowsTiKV == numRowsTiFlash; i++ {
		if numRowsTiKV, err = getNumOfRows(db, database, table, rowIdColName, "tikv", queryRange); err != nil {
			return false, err
		}
		if numRowsTiFlash, err = getNumOfRows(db, database, table, rowIdColName, "tiflash", queryRange); err != nil {
			return false, err
		}
	}

	if numRowsTiKV != numRowsTiFlash {
		fmt.Printf("Range %s, num of rows: tikv %d, tiflash %d. FAIL\n", queryRange.String(), numRowsTiKV, numRowsTiFlash)
	} else {
		fmt.Printf("Range %s, num of rows: tikv %d, tiflash %d. OK\n", queryRange.String(), numRowsTiKV, numRowsTiFlash)
	}
	return numRowsTiKV == numRowsTiFlash, err
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func getInitQueryRange(db *sql.DB, opts checkRowsOpts) ([]QueryRange, error) {
	var queryRanges []QueryRange
	if opts.queryLowerBound == 0 && opts.queryUpperBound == 0 {
		tikvMinID, tikvMaxID, err := getMinMaxTiDBRowID(db, opts.dbName, opts.tableName, opts.rowIdColName, "tikv")
		if err != nil {
			return nil, err
		}
		tiflashMinID, tiflashMaxID, err := getMinMaxTiDBRowID(db, opts.dbName, opts.tableName, opts.rowIdColName, "tiflash")
		if err != nil {
			return nil, err
		}

		fmt.Printf("RowID range: [%d, %d] (tikv)\n", tikvMinID, tikvMaxID)
		fmt.Printf("RowID range: [%d, %d] (tiflash)\n", tiflashMinID, tiflashMaxID)
		allMinID := min(tikvMinID, tiflashMinID)
		allMaxID := max(tikvMaxID, tiflashMaxID)
		if tikvMinID != tiflashMinID {
			fmt.Printf("tikv min id %d != tiflash min id %d, use %d as begin", tikvMinID, tiflashMinID, allMinID)
		}
		if tikvMaxID != tiflashMaxID {
			fmt.Printf("tikv max id %d != tiflash max id %d, use %d as end", tikvMaxID, tiflashMaxID, allMaxID)
		}

		queryRanges = append(queryRanges, NewMinMax(allMinID, allMaxID+1))
	} else if opts.queryLowerBound != 0 && opts.queryUpperBound != 0 {
		queryRanges = append(queryRanges, NewMinMax(opts.queryLowerBound, opts.queryUpperBound))
	} else if opts.queryLowerBound != 0 {
		queryRanges = append(queryRanges, NewMinMaxFrom(opts.queryLowerBound))
	} else if opts.queryUpperBound != 0 {
		queryRanges = append(queryRanges, NewMinMaxTo(opts.queryUpperBound))
	}
	return queryRanges, nil
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
		isConsist, err := haveConsistNumOfRows(db, opts.dbName, opts.tableName, opts.rowIdColName, queryRange, opts.numReplica)
		if err != nil {
			return err
		}
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
