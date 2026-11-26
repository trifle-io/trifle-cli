package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"

	"github.com/trifle-io/trifle-cli/internal/output"
)

func runTraces(args []string) {
	if len(args) == 0 {
		tracesUsage()
		os.Exit(1)
	}

	command := args[0]
	switch command {
	case "list":
		command = "search"
	case "search", "get", "entries", "attachments", "download":
	case "help", "-h", "--help":
		tracesUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown traces command: %s\n", command)
		tracesUsage()
		os.Exit(1)
	}

	args = args[1:]
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}
	fs := flag.NewFlagSet("traces "+command, flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source, rc.Config)
	fs.Lookup("token").DefValue = ""

	query := url.Values{}
	queryFlag := func(name, help string) {
		param := strings.ReplaceAll(name, "-", "_")
		fs.Func(name, help, func(value string) error {
			query.Set(param, value)
			return nil
		})
	}
	var reference, attachmentID, destination string
	if command != "search" {
		fs.StringVar(&reference, "reference", "", "Exact trace reference (required)")
	}
	switch command {
	case "search":
		queryFlag("path", "Trace path or slash-delimited prefix")
		fs.Func("tag", "Exact tag to match (repeatable, up to 20)", func(value string) error {
			query.Add("tags[]", value)
			return nil
		})
		queryFlag("tag-mode", "Match all or any tags (default all)")
		queryFlag("state", "Trace state (running, success, warning, error)")
		queryFlag("from", "Inclusive start time (ISO 8601 with UTC offset)")
		queryFlag("to", "Exclusive start time (ISO 8601 with UTC offset)")
		queryFlag("duration-min", "Minimum duration in milliseconds")
		queryFlag("limit", "Results per page, 1 to 100 (default 20)")
		queryFlag("cursor", "Next cursor from the previous search page")
	case "entries":
		queryFlag("part", "Stored part number (default 1)")
		queryFlag("row", "Row offset in the part (default 0)")
		queryFlag("limit", "Entries per page, 1 to 200 (default 100)")
	case "attachments":
		queryFlag("after-part", "Start scanning after this part (default 0)")
	case "download":
		fs.StringVar(&attachmentID, "attachment-id", "", "Attachment ID from an entry or attachment list (required)")
		fs.StringVar(&destination, "output", "", "New local file path or - for stdout (required)")
	}
	fs.Parse(args)
	if fs.NArg() > 0 {
		exitError(fmt.Errorf("unexpected argument %q", fs.Arg(0)))
	}
	if command != "search" && strings.TrimSpace(reference) == "" {
		exitError(errors.New("--reference is required"))
	}
	if command == "download" {
		if strings.TrimSpace(attachmentID) == "" {
			exitError(errors.New("--attachment-id is required"))
		}
		if destination == "" {
			exitError(errors.New("--output is required"))
		}
	}
	if driverNameFromSource(rc.Source) != "api" {
		exitError(errors.New("traces require a Trifle App API source (use --source <api-source>)"))
	}
	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}
	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	ctx := context.Background()
	var response map[string]any
	switch command {
	case "search":
		err = client.SearchTraces(ctx, query, &response)
	case "get":
		err = client.GetTrace(ctx, reference, &response)
	case "entries":
		err = client.GetTraceEntries(ctx, reference, query, &response)
	case "attachments":
		err = client.GetTraceAttachments(ctx, reference, query, &response)
	case "download":
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		body, err := client.DownloadTraceAttachment(ctx, reference, attachmentID)
		if err != nil {
			stop()
			exitError(err)
		}
		err = writeTraceAttachment(destination, body)
		body.Close()
		stop()
		if err != nil {
			exitError(fmt.Errorf("save trace attachment: %w", err))
		}
		return
	}
	if err != nil {
		exitError(err)
	}
	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func writeTraceAttachment(destination string, body io.Reader) error {
	if destination == "-" {
		_, err := io.Copy(os.Stdout, body)
		return err
	}

	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, body)
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return errors.Join(err, os.Remove(destination))
	}
	return nil
}

func tracesUsage() {
	fmt.Println("trifle traces <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  search       Search trace runs (alias: list)")
	fmt.Println("  get          Read trace metadata")
	fmt.Println("  entries      Read a page of trace entries")
	fmt.Println("  attachments  List a page of trace attachments")
	fmt.Println("  download     Save an attachment or stream it to stdout")
	fmt.Println()
	fmt.Println("Requires a Traces-enabled Trifle App database source and an organization API token with read access.")
	fmt.Println("Select the database with --source-id, TRIFLE_SOURCE_ID, or config source_id.")
	fmt.Println("Run 'trifle traces <command> --help' for options.")
}
