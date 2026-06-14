// Command ipfsgram is the CLI and daemon for an IPFS blockstore backed by
// Telegram channels. It wires the workflow packages (store, telegram, publish,
// gc, doctor, remove, daemon, node) into cobra commands.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// dsnEnv is the environment variable consulted when --dsn is unset.
const dsnEnv = "IPFSGRAM_DSN"

func main() {
	setupLogger()

	root := newRootCmd()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		stop()
		log.Error().Err(err).Msg("command failed")
		os.Exit(1)
	}
}

// newRootCmd builds the root command with all subcommands attached.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ipfsgram",
		Short:         "IPFS blockstore on top of Telegram channels",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("dsn", "", "PostgreSQL DSN (falls back to $"+dsnEnv+")")

	root.AddCommand(
		newDaemonCmd(),
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
	return root
}

// setupLogger configures zerolog: a human console writer on a TTY, structured
// JSON otherwise.
func setupLogger() {
	if isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()) {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}
