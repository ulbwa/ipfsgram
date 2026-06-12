// add.go — the `ipfsgram add` command: loads a DAG from a local file or the
// IPFS network and publishes it through internal/publish.

package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/block"
	"github.com/ulbwa/ipfsgram/internal/publish"
)

// newAddCmd returns the top-level `ipfsgram add` command.
func newAddCmd() *cobra.Command {
	var (
		cidArg string
		name   string
	)
	cmd := &cobra.Command{
		Use:   "add [path]",
		Short: "Publish content: a local file or a DAG fetched from the IPFS network by CID",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if (len(args) == 1) == (cidArg != "") {
				return errors.New("give either a file path or --cid")
			}
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			var (
				root    cid.Cid
				blocks  []block.Block
				pinName = name
			)
			if cidArg != "" {
				root, err = cid.Decode(cidArg)
				if err != nil {
					return fmt.Errorf("invalid CID %q: %w", cidArg, err)
				}
				log.Info().Stringer("cid", root).Msg("fetching DAG from the IPFS network")
				root, blocks, err = block.FromNetwork(ctx, root)
				if err != nil {
					return err
				}
				if pinName == "" {
					pinName = root.String()
				}
			} else {
				root, blocks, err = block.FromFile(ctx, args[0])
				if err != nil {
					return err
				}
				if pinName == "" {
					pinName = filepath.Base(args[0])
				}
			}

			p := publish.New(a.Store, a.Transport, log.Logger)
			root, err = p.Publish(ctx, root, pinName, blocks)
			if err != nil {
				return err
			}
			fmt.Fprintln(stdout, root.String())
			return nil
		},
	}
	cmd.Flags().StringVar(&cidArg, "cid", "", "fetch the DAG from the IPFS network by CID instead of a local file")
	cmd.Flags().StringVar(&name, "name", "", "pin name (defaults to the file name or CID)")
	return cmd
}
