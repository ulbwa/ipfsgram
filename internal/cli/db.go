package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/db"
)

// newDBCmd returns the `ipfsgram db` command group.
func newDBCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Управление схемой базы данных",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "migrate",
		Short: "Применить встроенные миграции",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dsn := ResolveDSN(cmd)
			if dsn == "" {
				return errNoDSN
			}
			if err := db.Migrate(dsn); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Миграции применены, версия схемы: %s\n", db.BinarySchemaVersion())
			return nil
		},
	})
	return cmd
}
