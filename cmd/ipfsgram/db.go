package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/db"
)

// newDBCmd returns the `ipfsgram db` command group.
func newDBCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Manage the database schema",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "migrate",
		Short: "Apply the embedded migrations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// `db migrate` is the only command that runs against an out-of-date
			// schema, so it skips the schema-version check and only needs the DSN.
			dsn := resolveDSN(cmd)
			if dsn == "" {
				return errNoDSN
			}
			if err := db.Migrate(dsn); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Migrations applied, schema version: %s\n", db.BinarySchemaVersion())
			return nil
		},
	})
	return cmd
}
