package cmd

import (
	"fmt"
	"os"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
)

var (
	logLevel  string
	logConfig log.Config
)

func initLogger() {
	logConfig = log.Config{
		Level: logLevel,
	}

	logger, props, err := log.InitLogger(&logConfig)
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	log.ReplaceGlobals(logger, props)
}

func Execute() {
	cobra.EnableCommandSorting = false

	rootCmd := &cobra.Command{
		Use:   "tiflash-ctl",
		Short: "TiFlash Controller",
		Long:  "TiFlash Controller (tiflash-ctl) is a command line tool for TiFlash Server",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			initLogger()
		},
	}
	// shared by all subcommands
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "L", "info", "Set the log level")
	rootCmd.AddCommand(newDispatchCmd(), newCheckCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
