// Package daemon runs the full IPFS node (libp2p + bitswap + DHT).
package daemon

import (
	"errors"

	"github.com/spf13/cobra"
)

// Command returns the `ipfsgram daemon` cobra command. Task 7 replaces the
// RunE body with the real daemon (daemon.go / node.go / blockstore.go).
func Command() *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Run the IPFS node (libp2p, bitswap, DHT)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.New("not implemented yet")
		},
	}
}
