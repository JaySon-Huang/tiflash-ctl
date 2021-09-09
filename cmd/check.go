package cmd

import (
	"github.com/JaySon-Huang/tiflash-ctl/cmd/check"

	"github.com/spf13/cobra"
)

func newCheckCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Some troubleshooting tools for TiFlash",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(check.NewRowConsistencyCmd(), check.NewDistributionCmd())

	return cmd
}
