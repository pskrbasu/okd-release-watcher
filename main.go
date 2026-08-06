package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

const (
	releaseControllerURL = "https://amd64.origin.releases.ci.openshift.org"
)

var defaultStreams = []string{
	"4.22.0-0.okd-scos-nightly",
	"5.0.0-0.okd-scos-nightly",
}

type options struct {
	streams        []string
	lookback       time.Duration
	includeHealthy bool
	jsonOutput     bool
	slackAlias     string
}

func main() {
	o := &options{}

	rootCmd := &cobra.Command{
		Use:   "okd-release-watcher",
		Short: "Monitor OKD release streams for failed/rejected builds",
	}

	reportCmd := &cobra.Command{
		Use:   "report",
		Short: "Generate a release stream health report",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse inline arguments from positional args (for Slack bot compatibility)
			parseInlineArgs(args, o)
			report, err := generateReport(o)
			if err != nil {
				return err
			}
			if o.jsonOutput {
				fmt.Println(report.JSON())
			} else {
				fmt.Println(report.String())
			}
			return nil
		},
	}

	reportCmd.Flags().StringSliceVar(&o.streams, "streams", defaultStreams, "Release streams to monitor")
	reportCmd.Flags().DurationVar(&o.lookback, "lookback", 24*time.Hour, "How far back to check for builds")
	reportCmd.Flags().BoolVar(&o.includeHealthy, "include-healthy", false, "Include healthy (accepted) builds in output")
	reportCmd.Flags().BoolVar(&o.jsonOutput, "json", false, "Output as JSON")

	botCmd := &cobra.Command{
		Use:   "bot",
		Short: "Run the Slack bot server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.serve()
		},
	}

	botCmd.Flags().StringSliceVar(&o.streams, "streams", defaultStreams, "Release streams to monitor")
	botCmd.Flags().DurationVar(&o.lookback, "lookback", 24*time.Hour, "How far back to check for builds")
	botCmd.Flags().BoolVar(&o.includeHealthy, "include-healthy", false, "Include healthy (accepted) builds in output")
	botCmd.Flags().StringVar(&o.slackAlias, "slack-alias", "", "Slack subteam ID to tag in reports")

	rootCmd.AddCommand(reportCmd, botCmd)

	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func parseInlineArgs(args []string, o *options) {
	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "lookback="):
			if d, err := time.ParseDuration(strings.TrimPrefix(arg, "lookback=")); err == nil {
				o.lookback = d
			}
		case strings.HasPrefix(arg, "streams="):
			o.streams = strings.Split(strings.TrimPrefix(arg, "streams="), ",")
		case arg == "healthy":
			o.includeHealthy = true
		case arg == "json":
			o.jsonOutput = true
		}
	}
}
