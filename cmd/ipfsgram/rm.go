// rm.go — the `ipfsgram rm` and `ipfsgram gc` commands plus the per-workflow
// service constructors (gc, remove, doctor) used by the housekeeping commands.

package main

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/doctor"
	"github.com/ulbwa/ipfsgram/internal/gc"
	"github.com/ulbwa/ipfsgram/internal/remove"
	"github.com/ulbwa/ipfsgram/internal/selector"
	"github.com/ulbwa/ipfsgram/internal/store"
)

// gcService builds a gc.Service from the app's store and transport.
func (a *app) gcService() *gc.Service {
	return gc.New(a.Store, a.Transport, log.Logger)
}

// removeService builds a remove.Service from the app's store.
func (a *app) removeService() *remove.Service {
	return remove.New(a.Store)
}

// doctorService builds a doctor.Service from the app's store and transport.
func (a *app) doctorService() *doctor.Service {
	return doctor.New(a.Store, a.Transport, selector.NewLoadCounter(), log.Logger)
}

// newRmCmd returns the top-level `ipfsgram rm` command.
func newRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <root-cid>",
		Short: "Unpin content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRm(cmd, args[0])
		},
	}
}

// runRm unpins the given root CID.
func runRm(cmd *cobra.Command, rootArg string) error {
	c, err := cid.Decode(rootArg)
	if err != nil {
		return fmt.Errorf("invalid CID %q: %w", rootArg, err)
	}
	ctx := cmd.Context()
	a, err := openApp(ctx, cmd, "")
	if err != nil {
		return err
	}
	defer a.Close()

	if err := a.removeService().Unpin(ctx, c.Bytes()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errors.New("pin not found")
		}
		return err
	}
	fmt.Fprintln(stdout, "Pin removed. Blocks will be freed on the next `ipfsgram gc`.")
	return nil
}

// newGCCmd returns the top-level `ipfsgram gc` command.
func newGCCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Delete CARs not owned by any pin",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runGC(cmd, yes)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}

// runGC previews unpinned CARs, confirms, then garbage-collects them.
func runGC(cmd *cobra.Command, yes bool) error {
	ctx := cmd.Context()
	a, err := openApp(ctx, cmd, "")
	if err != nil {
		return err
	}
	defer a.Close()

	svc := a.gcService()

	// Preview and prompt BEFORE GC takes the exclusive lock: an
	// interactive prompt must not stall concurrent publishers.
	candidates, total, err := svc.Candidates(ctx)
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
}
