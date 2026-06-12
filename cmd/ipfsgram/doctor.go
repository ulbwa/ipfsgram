// doctor.go — the `ipfsgram doctor` command: orphaned pending CARs, bot
// membership revalidation and CAR recovery via internal/maintain.

package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/maintain"
)

// newDoctorCmd returns the top-level `ipfsgram doctor` command.
func newDoctorCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnostics: orphaned pending CARs, membership revalidation, CAR recovery",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			a, err := openApp(ctx, cmd, "")
			if err != nil {
				return err
			}
			defer a.Close()

			svc := a.maintainService()

			// Phase 1: orphaned pending CARs.
			fmt.Fprintln(stdout, "Phase 1: orphaned pending CARs")
			orphans, err := svc.DoctorOrphans(ctx, maintain.OrphanPendingAge)
			if err != nil {
				return err
			}
			if len(orphans) == 0 {
				fmt.Fprintln(stdout, "  none found")
			} else {
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
			}

			// Phase 2: bot membership revalidation.
			fmt.Fprintln(stdout, "Phase 2: bot membership revalidation")
			report, err := svc.RevalidateMembership(ctx)
			if err != nil {
				return err
			}
			if len(report.Changes) == 0 {
				fmt.Fprintln(stdout, "  no changes")
			} else {
				for _, ch := range report.Changes {
					if ch.Gained {
						fmt.Fprintf(stdout, "  @%s gained access to channel %q\n", ch.BotUsername, ch.ChannelTitle)
					} else {
						fmt.Fprintf(stdout, "  @%s lost access to channel %q\n", ch.BotUsername, ch.ChannelTitle)
					}
				}
			}

			// Phase 3: recovery of unavailable CARs.
			fmt.Fprintln(stdout, "Phase 3: recovery of unavailable CARs")
			restored, deleted, err := svc.Recover(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "  restored %d, deleted %d\n", restored, deleted)
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "assume yes to all prompts")
	return cmd
}
