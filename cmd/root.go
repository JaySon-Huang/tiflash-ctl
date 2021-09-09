package cmd

import (
	"fmt"
	"os"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/spf13/cobra"
)

func addTiDBConnFlags(c *cobra.Command, tidbFlags *tidb.TiDBClientOpts) {
	c.Flags().StringVar(&tidbFlags.Host, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
	c.Flags().Int32Var(&tidbFlags.Port, "tidb_port", 4000, "The port of TiDB instance")
	c.Flags().StringVar(&tidbFlags.User, "user", "root", "TiDB user")
	c.Flags().StringVar(&tidbFlags.Password, "password", "", "TiDB user password")
}

func Execute() {
	cobra.EnableCommandSorting = false

	rootCmd := &cobra.Command{
		Use:   "tiflash-ctl",
		Short: "TiFlash Controller",
		Long:  "TiFlash Controller (tiflash-ctl) is a command line tool for TiFlash Server",
	}
	rootCmd.AddCommand(newDispatchCmd(), newCheckCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
