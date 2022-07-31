package cmd

import (
	"context"
	"fmt"

	"github.com/JaySon-Huang/tiflash-ctl/pkg/pg"
	"github.com/spf13/cobra"
)

type ExecOpts struct {
	pgHost   string
	pgPort   int
	dbName   string
	user     string
	password string
}

func newPgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pg",
		Short: "pg connect",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	newExecCmd := func() *cobra.Command {
		var opt ExecOpts
		c := &cobra.Command{
			Use:   "e",
			Short: "e",
			RunE: func(cmd *cobra.Command, args []string) error {
				return exec(opt)
			},
		}
		return c
	}

	cmd.AddCommand(newExecCmd())

	return cmd
}

func exec(opts ExecOpts) error {
	//opts.pgHost = "172.16.5.81"
	// opts.dbName = "test"
	opts.pgHost = "127.0.0.1"
	opts.pgPort = 5432
	opts.dbName = "hat"
	opts.user = "test"
	opts.password = "test"
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", opts.user, opts.password, opts.pgHost, opts.pgPort, opts.dbName)
	// conn, err := pgx.Connect(context.Background(), dsn)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
	// 	os.Exit(1)
	// }
	// defer conn.Close(context.Background())

	// var greeting string
	// err = conn.QueryRow(context.Background(), "select 'Hello, world!'").Scan(&greeting)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
	// 	os.Exit(1)
	// }

	// fmt.Println(greeting)
	sf := 1.0
	dataSrc := pg.NewDataSource(sf)
	fmt.Printf("sf=%f created\n", sf)
	txnClient, err := pg.NewTxnClient(context.Background(), dsn, &dataSrc)
	if err != nil {
		return err
	}
	defer txnClient.Close(context.Background())
	fmt.Printf("client created\n")

	txnClient.NewOrderTransactionPS(context.Background())
	fmt.Printf("done\n")
	return nil
}
