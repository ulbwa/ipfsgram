// Package cli wires the top-level ipfsgram commands onto the cobra root.
package cli

import (
	"os"

	"github.com/spf13/cobra"
)

// DSNEnv is the environment variable consulted when the --dsn flag is unset.
const DSNEnv = "IPFSGRAM_DSN"

// ResolveDSN returns the PostgreSQL DSN: the --dsn persistent flag if set,
// otherwise the IPFSGRAM_DSN environment variable.
func ResolveDSN(cmd *cobra.Command) string {
	if dsn, _ := cmd.Flags().GetString("dsn"); dsn != "" {
		return dsn
	}
	return os.Getenv(DSNEnv)
}

// Register attaches the top-level commands to the root command.
func Register(root *cobra.Command) {
	root.AddCommand(
		newAddCmd(),
		newRmCmd(),
		newGCCmd(),
		newStatusCmd(),
		newDoctorCmd(),
		newBotCmd(),
		newChannelCmd(),
		newMTProtoCmd(),
		newConfigCmd(),
		newDBCmd(),
	)
}
