package check

import (
	"fmt"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/options"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/pd"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/spf13/cobra"
)

func NewCheckRegionBoundaryCmd() *cobra.Command {
	var opt checkRegionBoundaryOpts
	c := &cobra.Command{
		Use:   "boundary",
		Short: "Check the boundary of Regions",
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkBoundary(opt)
		},
	}

	// Flags for "boundary"
	options.AddTiDBConnFlags(c, &opt.tidb)

	c.Flags().StringVar(&opt.dbName, "database", "", "The database name of query table")
	c.Flags().StringVar(&opt.tableName, "table", "", "The table name of query table")

	c.Flags().Int64Var(&opt.numPerBatch, "batch", 16, "The batch size for fetching Region info")
	c.Flags().StringVar(&opt.mode, "cmd", "split", "'split' dump the split command, 'merge' dump the merge command")

	return c
}

type checkRegionBoundaryOpts struct {
	tidb      tidb.TiDBClientOpts
	dbName    string
	tableName string

	numPerBatch int64
	mode        string
}

func checkBoundary(opts checkRegionBoundaryOpts) error {
	client, err := tidb.NewClientFromOpts(opts.tidb)
	if err != nil {
		return err
	}
	defer client.Close()

	pdInstances, err := client.GetInstances("pd")
	if err != nil {
		return err
	}
	pdClient := pd.NewPDClient(pdInstances[0]) // FIXME: can not get instances

	tableID, tidbPkType, err := client.GetTableIDAndClusteredIndex(opts.dbName, opts.tableName)
	if err != nil {
		return err
	}
	if tidbPkType == tidb.ClusteredIndexCommon {
		fmt.Printf("Checking boundary on clustered index table is not supported\n")
		return nil
	}

	startKey, endKey := tidb.NewTableStartAsKey(tableID), tidb.NewTableEndAsKey(tableID)

	numRegions, err := pdClient.GetNumRegionBetweenKey(startKey, endKey)
	if err != nil {
		return err
	}

	fmt.Printf("The expected total num of Regions is %d, table: `%s`.`%s`, table id: %d\n",
		numRegions, opts.dbName, opts.tableName, tableID)
	fmt.Printf("Scanning all Regions with batch size: %d\n", opts.numPerBatch)

	// The numRegions may be not accurate cause there could be region merge/split
	// cause by other reason
	var allRegions []pd.Region = make([]pd.Region, 0)
	queryStartKey := startKey
	for {
		regions, err := pdClient.GetRegions(queryStartKey, opts.numPerBatch)
		if err != nil {
			return err
		}
		if len(regions) == 0 {
			break
		}
		var (
			needMore     bool
			nextQueryKey tidb.TiKVKey
		)
		allRegions, needMore, nextQueryKey, err = concatRegionsWithSameTableID(allRegions, regions, tableID)
		if err != nil {
			return err
		}
		if !needMore {
			break
		}
		queryStartKey = nextQueryKey
	}

	fmt.Printf("The actual total num of Regions is %d, table: `%s`.`%s`, table id: %d\n",
		len(allRegions), opts.dbName, opts.tableName, tableID)

	// RegionID -> Region
	regionsWithInvalidBoundary := make(map[int64]pd.Region)
	invalidBoundaryRegions := make(map[string][]int64)
	for _, region := range allRegions {
		startKey, err := tidb.FromPDKey(region.StartKey)
		if err != nil {
			return err
		}
		_, err = startKey.GetTableRow()
		if err != nil {
			fmt.Printf("Region %d, start key: %s, err: %s\n", region.Id, region.StartKey, err)
			regionsWithInvalidBoundary[region.Id] = region
			if _, ok := invalidBoundaryRegions[region.StartKey]; !ok {
				invalidBoundaryRegions[region.StartKey] = []int64{region.Id}
			} else {
				invalidBoundaryRegions[region.StartKey] = append(invalidBoundaryRegions[region.StartKey], region.Id)
			}
		}
		endKey, err := tidb.FromPDKey(region.EndKey)
		if err != nil {
			return err
		}
		_, err = endKey.GetTableRow()
		if err != nil {
			fmt.Printf("Region %d, end   key: %s, err: %s\n", region.Id, region.EndKey, err)
			regionsWithInvalidBoundary[region.Id] = region
			if _, ok := invalidBoundaryRegions[region.EndKey]; !ok {
				invalidBoundaryRegions[region.EndKey] = []int64{region.Id}
			} else {
				invalidBoundaryRegions[region.EndKey] = append(invalidBoundaryRegions[region.EndKey], region.Id)
			}
		}
	}

	fmt.Printf("The num of Regions have invalid boundary is: %d, total Region num is: %d\n", len(regionsWithInvalidBoundary), len(allRegions))

	if opts.mode == "split" || opts.mode == "" {
		fmt.Printf("\nRun these command through pd-ctl to split Regions with an exist key:\n")
		for _, region := range regionsWithInvalidBoundary {
			fmt.Printf("operator add split-region %d --policy=scan\n", region.Id)
		}
	} else if opts.mode == "merge" {
		mergeRegionSet := make(map[int64]int64)
		for k, regions := range invalidBoundaryRegions {
			fmt.Printf("Need to merge the Regions with invalid boundary: %s, Regions: %d %d\n", k, regions[0], regions[1])
		}

		fmt.Printf("\nRun these command through pd-ctl to merge Regions that share invalid boundary:\n")
		for _, regions := range invalidBoundaryRegions {
			// Already apply merge with region id
			if _, ok := mergeRegionSet[regions[0]]; ok {
				continue
			}
			if _, ok := mergeRegionSet[regions[1]]; ok {
				continue
			}

			fmt.Printf("operator add merge-region %d %d\n", regions[0], regions[1])
			mergeRegionSet[regions[0]] = 1
			mergeRegionSet[regions[1]] = 1
		}
	}

	return nil
}

func concatRegionsWithSameTableID(allRegions, newRegions []pd.Region, tableID int64) ([]pd.Region, bool, tidb.TiKVKey, error) {
	var (
		allWithInOneTable bool = true
		lastRegionID      int64
		lastStartKey      tidb.TiKVKey
		lastEndKey        tidb.TiKVKey
		lastTblID         int64
		err               error
	)
	for _, region := range newRegions {
		lastRegionID = region.Id
		lastStartKey, err = tidb.FromPDKey(region.StartKey)
		if err != nil {
			return allRegions, allWithInOneTable, lastStartKey, err
		}
		lastEndKey, err = tidb.FromPDKey(region.EndKey)
		if err != nil {
			return allRegions, allWithInOneTable, lastStartKey, err
		}

		lastTblID, err = lastStartKey.GetTableID()
		if err != nil {
			return allRegions, allWithInOneTable, lastStartKey, err
		}
		if lastTblID != tableID {
			allWithInOneTable = false
			break
		}
		allRegions = append(allRegions, region)
	}

	if allWithInOneTable {
		fmt.Printf("The start key of Region %d is %s, table id: %d. continue with the end key: %s\n",
			lastRegionID, lastStartKey.GetPDKey(), tableID, lastEndKey.GetPDKey())
	} else {
		fmt.Printf("The start key of Region %d is %s, table id: %d. All finished, break.\n",
			lastRegionID, lastStartKey.GetPDKey(), lastTblID)
	}
	return allRegions, allWithInOneTable, lastEndKey, nil
}
