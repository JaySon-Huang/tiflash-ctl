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

func getPDInstances(db *sql.DB) []string {
	rows, err := db.Query("select INSTANCE from information_schema.cluster_info where type = 'pd'")
	if err != nil {
		panic(err)
	}
	var pdInstances []string
	var inst string
	for rows.Next() {
		rows.Scan(&inst)
		pdInstances = append(pdInstances, inst)
	}
	return pdInstances
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
