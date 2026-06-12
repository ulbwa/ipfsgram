package main

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/domain"
	"github.com/ulbwa/ipfsgram/internal/service/maintenance"
)

// maintenanceService builds a maintenance.Service from the app's ports.
func (a *app) maintenanceService() *maintenance.Service {
	return &maintenance.Service{
		Cars:      a.Cars,
		Channels:  a.Channels,
		Bots:      a.Bots,
		Pins:      a.Pins,
		Transport: a.Transport,
		Selector:  a.Selector,
		Locker:    a.Locker,
		Logger:    log.Logger,
	}
}

// newRmCmd returns the top-level `ipfsgram rm` command.
func newRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <root-cid>",
		Short: "Unpin content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := cid.Decode(args[0])
			if err != nil {
				return fmt.Errorf("invalid CID %q: %w", args[0], err)
			}
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			if err := a.maintenanceService().Unpin(ctx, c.Bytes()); err != nil {
				if errors.Is(err, domain.ErrNotFound) {
					return errors.New("pin not found")
				}
				return err
			}
			fmt.Fprintln(stdout, "Pin removed. Blocks will be freed on the next `ipfsgram gc`.")
			return nil
		},
	}
}

// newGCCmd returns the top-level `ipfsgram gc` command.
func newGCCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Delete CARs not owned by any pin",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			svc := a.maintenanceService()

			// Preview and prompt BEFORE GC takes the exclusive lock: an
			// interactive prompt must not stall concurrent publishers.
			candidates, total, err := svc.GCCandidates(ctx)
			if err != nil {
				return err
			}
			if len(candidates) == 0 {
				fmt.Fprintln(stdout, "nothing to collect")
				return nil
			}
			fmt.Fprintf(stdout, "Will delete %d CARs totalling %s\n",
				len(candidates), humanBytes(total))
			if !yes && !Confirm("Continue?") {
				fmt.Fprintln(stdout, "Cancelled")
				return nil
			}

			deleted, err := svc.GC(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "Deleted %d CARs\n", deleted)
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}
