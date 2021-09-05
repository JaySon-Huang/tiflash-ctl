package cmd

import (
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

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
