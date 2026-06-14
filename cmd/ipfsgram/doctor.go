// doctor.go — the `ipfsgram doctor` command: orphaned pending CARs, bot
// membership revalidation and CAR recovery via internal/doctor.

package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/doctor"
)

// newDoctorCmd returns the top-level `ipfsgram doctor` command.
func newDoctorCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnostics: orphaned pending CARs, membership revalidation, CAR recovery",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, yes)
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}

// runDoctor executes the three diagnostic phases in order.
func runDoctor(cmd *cobra.Command, yes bool) error {
	ctx := cmd.Context()
	a, err := openApp(ctx, cmd, "")
	if err != nil {
		return err
	}
	defer a.Close()

	svc := a.doctorService()
	if err := doctorOrphans(ctx, svc, yes); err != nil {
		return err
	}
	if err := doctorMembership(ctx, svc); err != nil {
		return err
	}
	return doctorRecover(ctx, svc)
}

// doctorOrphans is phase 1: list orphaned pending CARs and optionally delete them.
func doctorOrphans(ctx context.Context, svc *doctor.Service, yes bool) error {
	fmt.Fprintln(stdout, "Phase 1: orphaned pending CARs")
	orphans, err := svc.Orphans(ctx, doctor.OrphanPendingAge)
	if err != nil {
		return err
	}
	if len(orphans) == 0 {
		fmt.Fprintln(stdout, "  none found")
		return nil
	}
	for _, c := range orphans {
		fmt.Fprintf(stdout, "  car %d: channel %d, %s, %d blocks\n",
			c.ID, c.ChannelID, humanBytes(c.Size), c.BlockCount)
	}
	if yes || Confirm(fmt.Sprintf("Delete %d orphaned records?", len(orphans))) {
		if err := svc.CleanOrphans(ctx, orphans); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "  deleted %d records\n", len(orphans))
	} else {
		fmt.Fprintln(stdout, "  skipped")
	}
	return nil
}

// doctorMembership is phase 2: revalidate the bot×channel membership matrix.
func doctorMembership(ctx context.Context, svc *doctor.Service) error {
	fmt.Fprintln(stdout, "Phase 2: bot membership revalidation")
	report, err := svc.RevalidateMembership(ctx)
	if err != nil {
		return err
	}
	if len(report.Changes) == 0 {
		fmt.Fprintln(stdout, "  no changes")
		return nil
	}
	for _, ch := range report.Changes {
		if ch.Gained {
			fmt.Fprintf(stdout, "  @%s gained access to channel %q\n", ch.BotUsername, ch.ChannelTitle)
		} else {
			fmt.Fprintf(stdout, "  @%s lost access to channel %q\n", ch.BotUsername, ch.ChannelTitle)
		}
	}
	return nil
}

// doctorRecover is phase 3: recover CARs marked no_bot_access/too_large.
func doctorRecover(ctx context.Context, svc *doctor.Service) error {
	fmt.Fprintln(stdout, "Phase 3: recovery of unavailable CARs")
	restored, deleted, err := svc.Recover(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "  restored %d, deleted %d\n", restored, deleted)
	return nil
}
