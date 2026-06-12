package main

import (
	"os"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/ulbwa/ipfsgram/internal/cli"
	"github.com/ulbwa/ipfsgram/internal/daemon"
)

func main() {
	setupLogger()

	root := &cobra.Command{
		Use:           "ipfsgram",
		Short:         "IPFS blockstore on top of Telegram channels",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().String("dsn", "", "PostgreSQL DSN (falls back to $"+cli.DSNEnv+")")

	root.AddCommand(daemon.Command())
	cli.Register(root)

	if err := root.Execute(); err != nil {
		log.Error().Err(err).Msg("command failed")
		os.Exit(1)
	}
}

func setupLogger() {
	if isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd()) {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}
