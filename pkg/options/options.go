package options

import (
	"github.com/JaySon-Huang/tiflash-ctl/pkg/tidb"
	"github.com/spf13/cobra"
)

func AddTiDBConnFlags(c *cobra.Command, tidbFlags *tidb.TiDBClientOpts) {
	c.Flags().StringVar(&tidbFlags.Host, "tidb_ip", "127.0.0.1", "A TiDB instance IP")
	c.Flags().Int32Var(&tidbFlags.Port, "tidb_port", 4000, "The port of TiDB instance")
	c.Flags().StringVar(&tidbFlags.User, "user", "root", "TiDB user")
	c.Flags().StringVar(&tidbFlags.Password, "password", "", "TiDB user password")
}
