package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/trifle-io/trifle-cli/internal/api"
	"github.com/trifle-io/trifle-cli/internal/output"
	triflestats "github.com/trifle-io/trifle_stats_go"
)

var version = "0.1.0-dev"

func resolveVersion() string {
	if version != "0.1.0-dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "(devel)" && info.Main.Version != "" {
		return info.Main.Version
	}
	return version
}

var granularityPattern = regexp.MustCompile(`^\d+(s|m|h|d|w|mo|q|y)$`)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "metrics":
		runMetrics(os.Args[2:])
	case "transponders":
		runTransponders(os.Args[2:])
	case "mcp":
		runMCP(os.Args[2:])
	case "version":
		fmt.Println(resolveVersion())
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

type commonOptions struct {
	BaseURL string
	Token   string
	Timeout time.Duration
}

func addCommonFlags(fs *flag.FlagSet, cfg *sourceConfig) *commonOptions {
	var cfgURL string
	var cfgToken string
	timeout := 30 * time.Second
	if cfg != nil {
		cfgURL = cfg.URL
		cfgToken = cfg.Token
		if cfg.TimeoutSet {
			timeout = cfg.TimeoutDuration
		}
	}

	opts := &commonOptions{
		BaseURL: pickString(os.Getenv("TRIFLE_URL"), cfgURL, ""),
		Token:   pickString(os.Getenv("TRIFLE_TOKEN"), cfgToken, ""),
		Timeout: timeout,
	}

	fs.StringVar(&opts.BaseURL, "url", opts.BaseURL, "Trifle base URL (or TRIFLE_URL / config)")
	fs.StringVar(&opts.Token, "token", opts.Token, "API token (or TRIFLE_TOKEN / config)")
	fs.DurationVar(&opts.Timeout, "timeout", opts.Timeout, "HTTP timeout")
	return opts
}

func ensureToken(opts *commonOptions, allowPrompt bool) error {
	if opts.Token != "" {
		return nil
	}
	if !allowPrompt {
		return fmt.Errorf("missing token: set --token, TRIFLE_TOKEN, or api.token in config")
	}

	fmt.Fprint(os.Stderr, "Trifle token: ")
	reader := bufio.NewReader(os.Stdin)
	token, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	opts.Token = strings.TrimSpace(token)
	if opts.Token == "" {
		return fmt.Errorf("token is required")
	}
	return nil
}

func newClient(opts *commonOptions) (*api.Client, error) {
	return api.New(opts.BaseURL, opts.Token, opts.Timeout)
}

func runMetrics(args []string) {
	if len(args) == 0 {
		metricsUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "get":
		metricsGet(args[1:])
	case "keys":
		metricsKeys(args[1:])
	case "aggregate":
		metricsAggregate(args[1:])
	case "timeline":
		metricsTimeline(args[1:])
	case "category":
		metricsCategory(args[1:])
	case "push":
		metricsPush(args[1:])
	case "setup":
		metricsSetup(args[1:])
	case "help", "-h", "--help":
		metricsUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown metrics command: %s\n", args[0])
		metricsUsage()
		os.Exit(1)
	}
}

func metricsGet(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics get", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key (optional)")
	from := fs.String("from", "", "RFC3339 start timestamp")
	to := fs.String("to", "", "RFC3339 end timestamp")
	granularity := fs.String("granularity", "", "Granularity (e.g. 1h, 1d)")
	skipBlanks := fs.Bool("skip-blanks", false, "Skip empty data points (local drivers)")
	fs.Parse(args)

	driverName := strings.ToLower(strings.TrimSpace(driverOpts.Driver))
	if driverName == "" {
		driverName = "api"
	}

	fromValue, toValue, err := resolveTimeRange(*from, *to)
	if err != nil {
		exitError(err)
	}

	if isLocalDriver(driverName) {
		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		granularityValue, err := resolveGranularityLocal(*granularity, cfg)
		if err != nil {
			exitError(err)
		}

		fromTime, err := time.Parse(time.RFC3339Nano, fromValue)
		if err != nil {
			exitError(err)
		}
		toTime, err := time.Parse(time.RFC3339Nano, toValue)
		if err != nil {
			exitError(err)
		}

		if *key == "" {
			exitError(errors.New("--key is required for local drivers"))
		}

		result, err := triflestats.Values(cfg, *key, fromTime, toTime, granularityValue, *skipBlanks)
		if err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		response := map[string]any{
			"data": map[string]any{
				"at":     result.At,
				"values": result.Values,
			},
		}
		if err := output.PrintJSON(os.Stdout, response); err != nil {
			exitError(err)
		}
		return
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	granularityValue, err := resolveGranularityValue(context.Background(), client, *granularity)
	if err != nil {
		exitError(err)
	}

	params := map[string]string{
		"from":        fromValue,
		"to":          toValue,
		"granularity": granularityValue,
	}
	if *skipBlanks {
		params["skip_blanks"] = "true"
	}
	if *key != "" {
		params["key"] = *key
	}

	var response map[string]any
	if err := client.GetMetrics(context.Background(), params, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func metricsKeys(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics keys", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key (local drivers default to system keys)")
	from := fs.String("from", "", "RFC3339 start timestamp")
	to := fs.String("to", "", "RFC3339 end timestamp")
	granularity := fs.String("granularity", "", "Granularity (e.g. 1h, 1d)")
	format := fs.String("format", "json", "Output format: json|table|csv")
	fs.Parse(args)

	if isLocalDriver(driverOpts.Driver) {
		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		fromValue, toValue, err := resolveTimeRange(*from, *to)
		if err != nil {
			exitError(err)
		}

		granularityValue, err := resolveGranularityLocal(*granularity, cfg)
		if err != nil {
			exitError(err)
		}

		fromTime, err := time.Parse(time.RFC3339Nano, fromValue)
		if err != nil {
			exitError(err)
		}
		toTime, err := time.Parse(time.RFC3339Nano, toValue)
		if err != nil {
			exitError(err)
		}

		metricKey := strings.TrimSpace(*key)
		if metricKey == "" {
			metricKey = systemMetricsKey
		}

		result, err := triflestats.Values(cfg, metricKey, fromTime, toTime, granularityValue, true)
		if err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		var entries []keysEntry
		if metricKey == systemMetricsKey {
			entries = summarizeSystemKeys(result.Values)
		} else {
			entries = summarizeValuePaths(result.Values)
		}
		payload := map[string]any{
			"status": "ok",
			"timeframe": map[string]string{
				"from":        fromValue,
				"to":          toValue,
				"granularity": granularityValue,
			},
			"paths":       entries,
			"total_paths": len(entries),
		}

		switch strings.ToLower(*format) {
		case "table", "csv":
			table := output.Table{Columns: []string{"metric_key", "observations"}}
			for _, entry := range entries {
				table.Rows = append(table.Rows, []string{entry.MetricKey, fmt.Sprint(entry.Observations)})
			}
			if *format == "table" {
				output.PrintTable(os.Stdout, table)
			} else if err := output.PrintCSV(os.Stdout, table); err != nil {
				exitError(err)
			}
		default:
			if err := output.PrintJSON(os.Stdout, payload); err != nil {
				exitError(err)
			}
		}
		return
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	fromValue, toValue, err := resolveTimeRange(*from, *to)
	if err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	granularityValue, err := resolveGranularityValue(context.Background(), client, *granularity)
	if err != nil {
		exitError(err)
	}

	params := map[string]string{
		"from":        fromValue,
		"to":          toValue,
		"granularity": granularityValue,
	}

	var response metricsResponse
	if err := client.GetMetrics(context.Background(), params, &response); err != nil {
		exitError(err)
	}

	entries := summarizeKeys(response.Data.Values)
	payload := map[string]any{
		"status": "ok",
		"timeframe": map[string]string{
			"from":        fromValue,
			"to":          toValue,
			"granularity": granularityValue,
		},
		"paths":       entries,
		"total_paths": len(entries),
	}

	switch strings.ToLower(*format) {
	case "table", "csv":
		table := output.Table{Columns: []string{"metric_key", "observations"}}
		for _, entry := range entries {
			table.Rows = append(table.Rows, []string{entry.MetricKey, fmt.Sprint(entry.Observations)})
		}
		if *format == "table" {
			output.PrintTable(os.Stdout, table)
		} else if err := output.PrintCSV(os.Stdout, table); err != nil {
			exitError(err)
		}
	default:
		if err := output.PrintJSON(os.Stdout, payload); err != nil {
			exitError(err)
		}
	}
}

func metricsAggregate(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics aggregate", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key")
	valuePath := fs.String("value-path", "", "Value path")
	aggregator := fs.String("aggregator", "", "Aggregator (sum|mean|min|max)")
	from := fs.String("from", "", "RFC3339 start timestamp")
	to := fs.String("to", "", "RFC3339 end timestamp")
	granularity := fs.String("granularity", "", "Granularity (e.g. 1h, 1d)")
	slices := fs.Int("slices", 1, "Optional number of slices")
	format := fs.String("format", "json", "Output format: json|table|csv")
	fs.Parse(args)

	if isLocalDriver(driverOpts.Driver) {
		if *key == "" || *valuePath == "" || *aggregator == "" {
			exitError(errors.New("--key, --value-path, and --aggregator are required"))
		}
		if err := ensureNoWildcards(*valuePath); err != nil {
			exitError(err)
		}

		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		fromValue, toValue, err := resolveTimeRange(*from, *to)
		if err != nil {
			exitError(err)
		}

		granularityValue, err := resolveGranularityLocal(*granularity, cfg)
		if err != nil {
			exitError(err)
		}

		fromTime, err := time.Parse(time.RFC3339Nano, fromValue)
		if err != nil {
			exitError(err)
		}
		toTime, err := time.Parse(time.RFC3339Nano, toValue)
		if err != nil {
			exitError(err)
		}

		seriesResult, err := triflestats.Values(cfg, *key, fromTime, toTime, granularityValue, false)
		if err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		series := triflestats.SeriesFromResult(seriesResult)
		available := series.AvailablePaths()
		if len(available) == 0 {
			exitError(fmt.Errorf("no data available for path %s in the selected timeframe", *valuePath))
		}
		if !containsString(available, *valuePath) {
			exitError(fmt.Errorf("unknown path: %s", *valuePath))
		}

		aggName := strings.ToLower(strings.TrimSpace(*aggregator))
		var values []any
		switch aggName {
		case "sum":
			values = series.AggregateSum(*valuePath, *slices)
		case "mean":
			values = series.AggregateMean(*valuePath, *slices)
		case "min":
			values = series.AggregateMin(*valuePath, *slices)
		case "max":
			values = series.AggregateMax(*valuePath, *slices)
		default:
			exitError(fmt.Errorf("unsupported aggregator %q", *aggregator))
		}

		values = normalizeNumericSlice(values)
		if len(values) == 0 {
			exitError(fmt.Errorf("no data available for path %s in the selected timeframe", *valuePath))
		}

		payload := map[string]any{
			"status":          "ok",
			"aggregator":      aggName,
			"metric_key":      *key,
			"value_path":      *valuePath,
			"slices":          *slices,
			"values":          values,
			"count":           len(values),
			"timeframe":       buildTimeframePayload(fromValue, toValue, granularityValue),
			"available_paths": available,
			"matched_paths":   []string{*valuePath},
		}

		if *slices == 1 && len(values) > 0 && values[0] != nil {
			payload["value"] = values[0]
		}

		if table := buildSeriesTable(series, []string{*valuePath}); table != nil {
			payload["table"] = table
		}

		if err := output.PrintTableOrJSON(payload, strings.ToLower(*format)); err != nil {
			exitError(err)
		}
		return
	}

	if *key == "" || *valuePath == "" || *aggregator == "" {
		exitError(errors.New("--key, --value-path, and --aggregator are required"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	fromValue, toValue, err := resolveTimeRange(*from, *to)
	if err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	granularityValue, err := resolveGranularityValue(context.Background(), client, *granularity)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"mode":        "aggregate",
		"key":         *key,
		"value_path":  *valuePath,
		"aggregator":  *aggregator,
		"from":        fromValue,
		"to":          toValue,
		"granularity": granularityValue,
		"slices":      *slices,
	}

	data, err := queryMetrics(context.Background(), client, payload)
	if err != nil {
		exitError(err)
	}

	if err := output.PrintTableOrJSON(data, strings.ToLower(*format)); err != nil {
		exitError(err)
	}
}

func metricsTimeline(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics timeline", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key")
	valuePath := fs.String("value-path", "", "Value path")
	from := fs.String("from", "", "RFC3339 start timestamp")
	to := fs.String("to", "", "RFC3339 end timestamp")
	granularity := fs.String("granularity", "", "Granularity (e.g. 1h, 1d)")
	slices := fs.Int("slices", 1, "Optional number of slices")
	format := fs.String("format", "json", "Output format: json|table|csv")
	fs.Parse(args)

	if isLocalDriver(driverOpts.Driver) {
		if *key == "" || *valuePath == "" {
			exitError(errors.New("--key and --value-path are required"))
		}
		if err := ensureNoWildcards(*valuePath); err != nil {
			exitError(err)
		}

		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		fromValue, toValue, err := resolveTimeRange(*from, *to)
		if err != nil {
			exitError(err)
		}

		granularityValue, err := resolveGranularityLocal(*granularity, cfg)
		if err != nil {
			exitError(err)
		}

		fromTime, err := time.Parse(time.RFC3339Nano, fromValue)
		if err != nil {
			exitError(err)
		}
		toTime, err := time.Parse(time.RFC3339Nano, toValue)
		if err != nil {
			exitError(err)
		}

		seriesResult, err := triflestats.Values(cfg, *key, fromTime, toTime, granularityValue, false)
		if err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		series := triflestats.SeriesFromResult(seriesResult)
		available := series.AvailablePaths()
		formatted := series.FormatTimeline(*valuePath, *slices, nil)
		matched := filterAvailable(mapKeys(formatted), available)
		if len(matched) == 0 {
			exitError(fmt.Errorf("no matching data found for path %s in the selected timeframe", *valuePath))
		}

		payload := map[string]any{
			"status":          "ok",
			"formatter":       "timeline",
			"metric_key":      *key,
			"value_path":      *valuePath,
			"slices":          *slices,
			"timeframe":       buildTimeframePayload(fromValue, toValue, granularityValue),
			"result":          formatted,
			"available_paths": available,
			"matched_paths":   matched,
		}

		if table := buildSeriesTable(series, matched); table != nil {
			payload["table"] = table
		}

		if err := output.PrintTableOrJSON(payload, strings.ToLower(*format)); err != nil {
			exitError(err)
		}
		return
	}

	if *key == "" || *valuePath == "" {
		exitError(errors.New("--key and --value-path are required"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	fromValue, toValue, err := resolveTimeRange(*from, *to)
	if err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	granularityValue, err := resolveGranularityValue(context.Background(), client, *granularity)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"mode":        "timeline",
		"key":         *key,
		"value_path":  *valuePath,
		"from":        fromValue,
		"to":          toValue,
		"granularity": granularityValue,
		"slices":      *slices,
	}

	data, err := queryMetrics(context.Background(), client, payload)
	if err != nil {
		exitError(err)
	}

	if err := output.PrintTableOrJSON(data, strings.ToLower(*format)); err != nil {
		exitError(err)
	}
}

func metricsCategory(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics category", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key")
	valuePath := fs.String("value-path", "", "Value path")
	from := fs.String("from", "", "RFC3339 start timestamp")
	to := fs.String("to", "", "RFC3339 end timestamp")
	granularity := fs.String("granularity", "", "Granularity (e.g. 1h, 1d)")
	slices := fs.Int("slices", 1, "Optional number of slices")
	format := fs.String("format", "json", "Output format: json|table|csv")
	fs.Parse(args)

	if isLocalDriver(driverOpts.Driver) {
		if *key == "" || *valuePath == "" {
			exitError(errors.New("--key and --value-path are required"))
		}
		if err := ensureNoWildcards(*valuePath); err != nil {
			exitError(err)
		}

		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		fromValue, toValue, err := resolveTimeRange(*from, *to)
		if err != nil {
			exitError(err)
		}

		granularityValue, err := resolveGranularityLocal(*granularity, cfg)
		if err != nil {
			exitError(err)
		}

		fromTime, err := time.Parse(time.RFC3339Nano, fromValue)
		if err != nil {
			exitError(err)
		}
		toTime, err := time.Parse(time.RFC3339Nano, toValue)
		if err != nil {
			exitError(err)
		}

		seriesResult, err := triflestats.Values(cfg, *key, fromTime, toTime, granularityValue, false)
		if err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		series := triflestats.SeriesFromResult(seriesResult)
		available := series.AvailablePaths()
		formatted := series.FormatCategory(*valuePath, *slices, nil)
		matched := filterAvailable(extractCategoryPaths(formatted), available)
		if len(matched) == 0 {
			exitError(fmt.Errorf("no matching data found for path %s in the selected timeframe", *valuePath))
		}

		payload := map[string]any{
			"status":          "ok",
			"formatter":       "category",
			"metric_key":      *key,
			"value_path":      *valuePath,
			"slices":          *slices,
			"timeframe":       buildTimeframePayload(fromValue, toValue, granularityValue),
			"result":          formatted,
			"available_paths": available,
			"matched_paths":   matched,
		}

		if table := buildSeriesTable(series, matched); table != nil {
			payload["table"] = table
		}

		if err := output.PrintTableOrJSON(payload, strings.ToLower(*format)); err != nil {
			exitError(err)
		}
		return
	}

	if *key == "" || *valuePath == "" {
		exitError(errors.New("--key and --value-path are required"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	fromValue, toValue, err := resolveTimeRange(*from, *to)
	if err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	granularityValue, err := resolveGranularityValue(context.Background(), client, *granularity)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"mode":        "category",
		"key":         *key,
		"value_path":  *valuePath,
		"from":        fromValue,
		"to":          toValue,
		"granularity": granularityValue,
		"slices":      *slices,
	}

	data, err := queryMetrics(context.Background(), client, payload)
	if err != nil {
		exitError(err)
	}

	if err := output.PrintTableOrJSON(data, strings.ToLower(*format)); err != nil {
		exitError(err)
	}
}

func metricsPush(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics push", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	key := fs.String("key", "", "Metrics key")
	at := fs.String("at", "", "RFC3339 timestamp (default: now)")
	valuesJSON := fs.String("values", "", "Values payload as JSON")
	valuesFile := fs.String("values-file", "", "Path to JSON file with values payload")
	mode := fs.String("mode", "track", "Mode: track|assert (local drivers)")
	fs.Parse(args)

	if *key == "" {
		exitError(errors.New("--key is required"))
	}

	values, err := loadJSONPayload(*valuesJSON, *valuesFile)
	if err != nil {
		exitError(err)
	}

	if values == nil {
		exitError(errors.New("--values or --values-file is required"))
	}

	atValue := strings.TrimSpace(*at)
	if atValue == "" {
		atValue = time.Now().UTC().Format(time.RFC3339)
	} else if err := validateTimestamp("at", atValue); err != nil {
		exitError(err)
	}

	driverName := strings.ToLower(strings.TrimSpace(driverOpts.Driver))
	if driverName == "" {
		driverName = "api"
	}

	if isLocalDriver(driverName) {
		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}
		cfg := local.Config

		atTime, err := time.Parse(time.RFC3339Nano, atValue)
		if err != nil {
			exitError(err)
		}

		valuesMap, err := ensureValuesMap(values)
		if err != nil {
			exitError(err)
		}

		if err := performLocalWrite(cfg, *mode, *key, atTime, valuesMap); err != nil {
			exitError(maybeSuggestSetup(err, local.DriverName, local.TableName))
		}

		response := map[string]any{
			"data": map[string]any{
				"key":    *key,
				"at":     atTime,
				"values": valuesMap,
			},
		}
		if err := output.PrintJSON(os.Stdout, response); err != nil {
			exitError(err)
		}
		return
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	payload := map[string]any{
		"key":    *key,
		"at":     atValue,
		"values": values,
	}

	var response map[string]any
	if err := client.PostMetrics(context.Background(), payload, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func metricsSetup(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("metrics setup", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	driverOpts := addDriverFlags(fs, &rc.Source)
	fs.Parse(args)

	driverName := strings.ToLower(strings.TrimSpace(driverOpts.Driver))
	if driverName == "" {
		driverName = "api"
	}

	if !isLocalDriver(driverName) {
		exitError(fmt.Errorf("setup is only supported for local drivers (sqlite, postgres, mysql, redis, mongo)"))
	}

	local, err := loadLocalConfig(driverOpts)
	if err != nil {
		exitError(err)
	}

	if err := local.Setup(); err != nil {
		exitError(err)
	}

	target := strings.TrimSpace(local.TableName)
	if target == "" {
		target = "(default)"
	}
	driverLabel := local.DriverName
	if driverLabel != "" {
		driverLabel = strings.ToUpper(driverLabel[:1]) + driverLabel[1:]
	}
	fmt.Fprintf(os.Stdout, "%s setup complete for %s\n", driverLabel, target)
}

func runTransponders(args []string) {
	if len(args) == 0 {
		transponderUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		transpondersList(args[1:])
	case "create":
		transpondersCreate(args[1:])
	case "update":
		transpondersUpdate(args[1:])
	case "delete":
		transpondersDelete(args[1:])
	case "help", "-h", "--help":
		transponderUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown transponders command: %s\n", args[0])
		transponderUsage()
		os.Exit(1)
	}
}

func transpondersList(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("transponders list", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	fs.Parse(args)

	if driverNameFromSource(rc.Source) != "api" {
		exitError(errors.New("transponders are only available for api drivers (use --source <api-source>)"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.GetTransponders(context.Background(), &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func transpondersCreate(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("transponders create", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	payloadJSON := fs.String("payload", "", "JSON payload for transponder")
	payloadFile := fs.String("payload-file", "", "Path to JSON file for payload")
	fs.Parse(args)

	if driverNameFromSource(rc.Source) != "api" {
		exitError(errors.New("transponders are only available for api drivers (use --source <api-source>)"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	payload, err := loadJSONPayload(*payloadJSON, *payloadFile)
	if err != nil {
		exitError(err)
	}
	if payload == nil {
		exitError(errors.New("--payload or --payload-file is required"))
	}
	payloadMap, ok := payload.(map[string]any)
	if !ok {
		exitError(errors.New("payload must be a JSON object"))
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.CreateTransponder(context.Background(), payloadMap, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func transpondersUpdate(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("transponders update", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	id := fs.String("id", "", "Transponder ID")
	payloadJSON := fs.String("payload", "", "JSON payload for transponder")
	payloadFile := fs.String("payload-file", "", "Path to JSON file for payload")
	fs.Parse(args)

	if driverNameFromSource(rc.Source) != "api" {
		exitError(errors.New("transponders are only available for api drivers (use --source <api-source>)"))
	}

	if *id == "" {
		exitError(errors.New("--id is required"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	payload, err := loadJSONPayload(*payloadJSON, *payloadFile)
	if err != nil {
		exitError(err)
	}
	if payload == nil {
		exitError(errors.New("--payload or --payload-file is required"))
	}
	payloadMap, ok := payload.(map[string]any)
	if !ok {
		exitError(errors.New("payload must be a JSON object"))
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.UpdateTransponder(context.Background(), *id, payloadMap, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

func transpondersDelete(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("transponders delete", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	id := fs.String("id", "", "Transponder ID")
	fs.Parse(args)

	if driverNameFromSource(rc.Source) != "api" {
		exitError(errors.New("transponders are only available for api drivers (use --source <api-source>)"))
	}

	if *id == "" {
		exitError(errors.New("--id is required"))
	}

	if err := ensureToken(opts, true); err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	var response map[string]any
	if err := client.DeleteTransponder(context.Background(), *id, &response); err != nil {
		exitError(err)
	}

	if err := output.PrintJSON(os.Stdout, response); err != nil {
		exitError(err)
	}
}

type metricsResponse struct {
	Data seriesData `json:"data"`
}

type seriesData struct {
	At     []string                 `json:"at"`
	Values []map[string]interface{} `json:"values"`
}

type keysEntry struct {
	MetricKey    string `json:"metric_key"`
	Observations int64  `json:"observations"`
}

type sourceResponse struct {
	Data sourceResponseData `json:"data"`
}

type sourceResponseData struct {
	DefaultGranularity     string   `json:"default_granularity"`
	AvailableGranularities []string `json:"available_granularities"`
}

func summarizeKeys(values []map[string]interface{}) []keysEntry {
	counts := map[string]int64{}

	for _, row := range values {
		rawKeys, ok := row["keys"]
		if !ok {
			continue
		}

		keysMap, ok := rawKeys.(map[string]interface{})
		if !ok {
			continue
		}

		for key, value := range keysMap {
			counts[key] += toInt64(value)
		}
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]keysEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, keysEntry{MetricKey: key, Observations: counts[key]})
	}

	return entries
}

func summarizeSystemKeys(values []map[string]any) []keysEntry {
	counts := map[string]int64{}

	for _, row := range values {
		rawKeys, ok := row["keys"]
		if !ok || rawKeys == nil {
			continue
		}

		if keysMap, ok := rawKeys.(map[string]any); ok {
			for key, value := range keysMap {
				counts[key] += toInt64(value)
			}
		}
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]keysEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, keysEntry{MetricKey: key, Observations: counts[key]})
	}

	return entries
}

func summarizeValuePaths(values []map[string]any) []keysEntry {
	counts := map[string]int64{}

	for _, row := range values {
		if len(row) == 0 {
			continue
		}
		packed := triflestats.Pack(row)
		for key := range packed {
			counts[key]++
		}
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]keysEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, keysEntry{MetricKey: key, Observations: counts[key]})
	}

	return entries
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			return parsed
		}
	}

	return 0
}

func resolveTimeRange(from, to string) (string, string, error) {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)

	if from == "" && to == "" {
		now := time.Now().UTC()
		from = now.Add(-24 * time.Hour).Format(time.RFC3339)
		to = now.Format(time.RFC3339)
		return from, to, nil
	}

	if from == "" || to == "" {
		return "", "", fmt.Errorf("from and to are required together (RFC3339, e.g. 2024-01-02T15:04:05Z)")
	}

	if err := validateTimestamp("from", from); err != nil {
		return "", "", err
	}
	if err := validateTimestamp("to", to); err != nil {
		return "", "", err
	}

	return from, to, nil
}

func validateTimestamp(label, value string) error {
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("%s must be RFC3339 (e.g. 2024-01-02T15:04:05Z or 2024-01-02T15:04:05+00:00)", label)
	}
	return nil
}

func resolveGranularityValue(ctx context.Context, client *api.Client, granularity string) (string, error) {
	granularity = strings.TrimSpace(granularity)
	if granularity == "" {
		return resolveGranularity(ctx, client)
	}
	return validateGranularity(granularity)
}

func validateGranularity(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "", fmt.Errorf("granularity is required")
	}
	if !granularityPattern.MatchString(normalized) {
		return "", fmt.Errorf("granularity must be <number><unit> using s, m, h, d, w, mo, q, y (e.g. 1h, 15m, 1d)")
	}
	return normalized, nil
}

func resolveGranularity(ctx context.Context, client *api.Client) (string, error) {
	var response sourceResponse
	if err := client.GetSource(ctx, &response); err != nil {
		return "", err
	}

	if response.Data.DefaultGranularity != "" {
		return response.Data.DefaultGranularity, nil
	}

	available := response.Data.AvailableGranularities
	for _, candidate := range []string{"1h", "1d"} {
		for _, value := range available {
			if value == candidate {
				return candidate, nil
			}
		}
	}

	if len(available) > 0 {
		return available[0], nil
	}

	return "1h", nil
}

func queryMetrics(ctx context.Context, client *api.Client, payload map[string]any) (map[string]any, error) {
	var response map[string]any
	if err := client.QueryMetrics(ctx, payload, &response); err != nil {
		return nil, err
	}

	rawData, ok := response["data"]
	if !ok {
		return nil, fmt.Errorf("missing data in response")
	}

	data, ok := rawData.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected data shape")
	}

	return data, nil
}

type driverOptions struct {
	Driver          string
	DBPath          string
	DSN             string
	Host            string
	Port            string
	User            string
	Password        string
	Database        string
	Table           string
	Collection      string
	Prefix          string
	Joined          string
	Separator       string
	TimeZone        string
	BeginningOfWeek string
	Granularities   string
	BufferMode      string
	BufferDrivers   string
	BufferDuration  time.Duration
	BufferSize      int
	BufferAggregate bool
	BufferAsync     bool
}

func addDriverFlags(fs *flag.FlagSet, cfg *sourceConfig) *driverOptions {
	var cfgDriver string
	var cfgDB string
	var cfgDSN string
	var cfgHost string
	var cfgPort string
	var cfgUser string
	var cfgPassword string
	var cfgDatabase string
	var cfgTable string
	var cfgCollection string
	var cfgPrefix string
	var cfgJoined string
	var cfgSeparator string
	var cfgTimeZone string
	var cfgWeekStart string
	var cfgGranularities string
	var cfgBufferMode string
	var cfgBufferDrivers string
	var cfgBufferDuration string
	var cfgBufferSize int
	var cfgBufferSizeText string
	var cfgBufferAggregate bool
	var cfgBufferAsync bool
	var cfgBufferAggregateSet bool
	var cfgBufferAsyncSet bool
	if cfg != nil {
		cfgDriver = cfg.Driver
		cfgDB = cfg.DB
		cfgDSN = cfg.DSN
		cfgHost = cfg.Host
		cfgPort = cfg.Port
		cfgUser = cfg.User
		cfgPassword = cfg.Password
		cfgDatabase = firstNonEmpty(cfg.Database, cfg.DB)
		cfgTable = cfg.Table
		cfgCollection = cfg.Collection
		cfgPrefix = cfg.Prefix
		cfgJoined = cfg.Joined
		cfgSeparator = cfg.Separator
		cfgTimeZone = cfg.TimeZone
		cfgWeekStart = cfg.WeekStart
		cfgGranularities = cfg.Granularities.Joined()
		cfgBufferMode = cfg.BufferMode
		cfgBufferDrivers = cfg.BufferDrivers.Joined()
		cfgBufferDuration = cfg.BufferDuration
		cfgBufferSize = cfg.BufferSize
		if cfgBufferSize > 0 {
			cfgBufferSizeText = fmt.Sprintf("%d", cfgBufferSize)
		}
		if cfg.BufferAggregate != nil {
			cfgBufferAggregate = *cfg.BufferAggregate
			cfgBufferAggregateSet = true
		}
		if cfg.BufferAsync != nil {
			cfgBufferAsync = *cfg.BufferAsync
			cfgBufferAsyncSet = true
		}
	}

	defaultStats := triflestats.DefaultConfig()
	defaultBufferAggregate := defaultStats.BufferAggregate
	defaultBufferAsync := defaultStats.BufferAsync
	if cfgBufferAggregateSet {
		defaultBufferAggregate = cfgBufferAggregate
	}
	if cfgBufferAsyncSet {
		defaultBufferAsync = cfgBufferAsync
	}

	opts := &driverOptions{
		Driver:          pickString(os.Getenv("TRIFLE_DRIVER"), cfgDriver, "api"),
		DBPath:          pickString(os.Getenv("TRIFLE_DB"), cfgDB, ""),
		DSN:             pickString(os.Getenv("TRIFLE_DSN"), cfgDSN, ""),
		Host:            pickString(os.Getenv("TRIFLE_HOST"), cfgHost, ""),
		Port:            pickString(os.Getenv("TRIFLE_PORT"), cfgPort, ""),
		User:            pickString(os.Getenv("TRIFLE_USER"), cfgUser, ""),
		Password:        pickString(os.Getenv("TRIFLE_PASSWORD"), cfgPassword, ""),
		Database:        pickString(os.Getenv("TRIFLE_DATABASE"), cfgDatabase, ""),
		Table:           pickString(os.Getenv("TRIFLE_TABLE"), cfgTable, "trifle_stats"),
		Collection:      pickString(os.Getenv("TRIFLE_COLLECTION"), cfgCollection, ""),
		Prefix:          pickString(os.Getenv("TRIFLE_PREFIX"), cfgPrefix, ""),
		Joined:          pickString(os.Getenv("TRIFLE_JOINED"), cfgJoined, "full"),
		Separator:       pickString(os.Getenv("TRIFLE_SEPARATOR"), cfgSeparator, "::"),
		TimeZone:        pickString(os.Getenv("TRIFLE_TIMEZONE"), cfgTimeZone, "GMT"),
		BeginningOfWeek: pickString(os.Getenv("TRIFLE_WEEK_START"), cfgWeekStart, "monday"),
		Granularities:   pickString(os.Getenv("TRIFLE_GRANULARITIES"), cfgGranularities, ""),
		BufferMode:      pickString(os.Getenv("TRIFLE_BUFFER_MODE"), cfgBufferMode, "auto"),
		BufferDrivers:   pickString(os.Getenv("TRIFLE_BUFFER_DRIVERS"), cfgBufferDrivers, ""),
		BufferDuration:  parseDurationOrDefault(pickString(os.Getenv("TRIFLE_BUFFER_DURATION"), cfgBufferDuration, ""), defaultStats.BufferDuration),
		BufferSize:      parseIntOrDefault(pickString(os.Getenv("TRIFLE_BUFFER_SIZE"), cfgBufferSizeText, ""), defaultStats.BufferSize),
		BufferAggregate: parseBoolOrDefault(pickString(os.Getenv("TRIFLE_BUFFER_AGGREGATE"), "", ""), defaultBufferAggregate),
		BufferAsync:     parseBoolOrDefault(pickString(os.Getenv("TRIFLE_BUFFER_ASYNC"), "", ""), defaultBufferAsync),
	}

	fs.StringVar(&opts.Driver, "driver", opts.Driver, "Driver: api|sqlite|postgres|mysql|redis|mongo (or TRIFLE_DRIVER / config)")
	fs.StringVar(&opts.DBPath, "db", opts.DBPath, "SQLite DB path (sqlite) or database name fallback (or TRIFLE_DB / config)")
	fs.StringVar(&opts.DSN, "dsn", opts.DSN, "Driver DSN/URI (postgres/mysql/redis/mongo)")
	fs.StringVar(&opts.Host, "host", opts.Host, "Driver host (postgres/mysql/redis/mongo)")
	fs.StringVar(&opts.Port, "port", opts.Port, "Driver port (postgres/mysql/redis)")
	fs.StringVar(&opts.User, "user", opts.User, "Driver user (postgres/mysql/redis)")
	fs.StringVar(&opts.Password, "password", opts.Password, "Driver password (postgres/mysql/redis)")
	fs.StringVar(&opts.Database, "database", opts.Database, "Database name (postgres/mysql/mongo)")
	fs.StringVar(&opts.Table, "table", opts.Table, "Table name (sqlite/postgres/mysql)")
	fs.StringVar(&opts.Collection, "collection", opts.Collection, "Collection name (mongo)")
	fs.StringVar(&opts.Prefix, "prefix", opts.Prefix, "Key prefix (redis)")
	fs.StringVar(&opts.Joined, "joined", opts.Joined, "Identifier mode: full|partial|separated (or TRIFLE_JOINED / config)")
	fs.StringVar(&opts.Separator, "separator", opts.Separator, "Key separator (or TRIFLE_SEPARATOR / config)")
	fs.StringVar(&opts.TimeZone, "timezone", opts.TimeZone, "Time zone (or TRIFLE_TIMEZONE / config)")
	fs.StringVar(&opts.BeginningOfWeek, "week-start", opts.BeginningOfWeek, "Week start: monday..sunday (or TRIFLE_WEEK_START / config)")
	fs.StringVar(&opts.Granularities, "granularities", opts.Granularities, "Comma-separated granularities (or TRIFLE_GRANULARITIES / config)")
	fs.StringVar(&opts.BufferMode, "buffer-mode", opts.BufferMode, "Buffer mode: auto|on|off")
	fs.StringVar(&opts.BufferDrivers, "buffer-drivers", opts.BufferDrivers, "Comma-separated drivers allowed to buffer when mode is auto/on")
	fs.DurationVar(&opts.BufferDuration, "buffer-duration", opts.BufferDuration, "Buffer flush interval")
	fs.IntVar(&opts.BufferSize, "buffer-size", opts.BufferSize, "Buffer queue size")
	fs.BoolVar(&opts.BufferAggregate, "buffer-aggregate", opts.BufferAggregate, "Aggregate buffered writes")
	fs.BoolVar(&opts.BufferAsync, "buffer-async", opts.BufferAsync, "Flush buffered writes asynchronously")
	return opts
}

func parseGranularities(input string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseWeekday(input string) (time.Weekday, error) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "monday", "mon":
		return time.Monday, nil
	case "tuesday", "tue":
		return time.Tuesday, nil
	case "wednesday", "wed":
		return time.Wednesday, nil
	case "thursday", "thu":
		return time.Thursday, nil
	case "friday", "fri":
		return time.Friday, nil
	case "saturday", "sat":
		return time.Saturday, nil
	case "sunday", "sun":
		return time.Sunday, nil
	default:
		return time.Monday, fmt.Errorf("invalid week-start: %s", input)
	}
}

func parseJoinedIdentifier(input string) (triflestats.JoinedIdentifier, error) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "full", "":
		return triflestats.JoinedFull, nil
	case "partial":
		return triflestats.JoinedPartial, nil
	case "separated", "none", "null":
		return triflestats.JoinedSeparated, nil
	default:
		return triflestats.JoinedFull, fmt.Errorf("invalid joined mode: %s", input)
	}
}

func resolveGranularityLocal(granularity string, cfg *triflestats.Config) (string, error) {
	granularity = strings.TrimSpace(granularity)
	if granularity != "" {
		return validateGranularity(granularity)
	}

	available := cfg.EffectiveGranularities()
	for _, candidate := range []string{"1h", "1d"} {
		for _, value := range available {
			if value == candidate {
				return candidate, nil
			}
		}
	}
	if len(available) > 0 {
		return available[0], nil
	}
	return "1h", nil
}

func driverNameFromSource(source sourceConfig) string {
	return strings.ToLower(strings.TrimSpace(pickString(os.Getenv("TRIFLE_DRIVER"), source.Driver, "api")))
}

func maybeSuggestSetup(err error, driverName, targetName string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	messageLower := strings.ToLower(message)

	normalizedDriver := normalizeDriverName(driverName)
	if normalizedDriver == "" {
		normalizedDriver = "sqlite"
	}

	if !strings.Contains(messageLower, "no such table") &&
		!strings.Contains(messageLower, "doesn't exist") &&
		!strings.Contains(messageLower, "relation") {
		return err
	}

	switch normalizedDriver {
	case "sqlite", "postgres", "mysql":
		if strings.TrimSpace(targetName) == "" {
			targetName = "trifle_stats"
		}
		return fmt.Errorf("%s (run: trifle metrics setup --driver %s --table %s)", message, normalizedDriver, targetName)
	case "mongo":
		return fmt.Errorf("%s (run: trifle metrics setup --driver mongo)", message)
	}
	return err
}

func loadJSONPayload(rawJSON, filePath string) (any, error) {
	if filePath != "" {
		contents, err := os.ReadFile(filepath.Clean(filePath))
		if err != nil {
			return nil, fmt.Errorf("read payload file: %w", err)
		}
		rawJSON = string(contents)
	}

	if strings.TrimSpace(rawJSON) == "" {
		return nil, nil
	}

	var payload any
	if err := json.Unmarshal([]byte(rawJSON), &payload); err != nil {
		return nil, fmt.Errorf("parse JSON payload: %w", err)
	}

	return payload, nil
}

func ensureValuesMap(values any) (map[string]any, error) {
	switch payload := values.(type) {
	case map[string]any:
		return payload, nil
	default:
		return nil, fmt.Errorf("values must be a JSON object")
	}
}

func performLocalWrite(cfg *triflestats.Config, mode, key string, at time.Time, values map[string]any) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "track":
		return triflestats.Track(cfg, key, at, values)
	case "assert":
		return triflestats.Assert(cfg, key, at, values)
	default:
		return fmt.Errorf("invalid mode: %s (expected track or assert)", mode)
	}
}

func usage() {
	reset := "\x1b[0m"
	lines := []string{
		"████████╗██████╗ ██╗███████╗██╗     ███████╗",
		"╚══██╔══╝██╔══██╗██║██╔════╝██║     ██╔════╝",
		"   ██║   ██████╔╝██║█████╗  ██║     █████╗  ",
		"   ██║   ██╔══██╗██║██╔══╝  ██║     ██╔══╝  ",
		"   ██║   ██║  ██║██║██║     ███████╗███████╗",
		"   ╚═╝   ╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝╚══════╝",
	}
	start := [3]int{70, 236, 213}
	end := [3]int{0, 187, 167}
	lerp := func(a, b int, t float64) int {
		return int(float64(a) + (float64(b-a) * t) + 0.5)
	}
	fmt.Println()
	for i, line := range lines {
		var t float64
		if len(lines) > 1 {
			t = float64(i) / float64(len(lines)-1)
		}
		r := lerp(start[0], end[0], t)
		g := lerp(start[1], end[1], t)
		b := lerp(start[2], end[2], t)
		color := fmt.Sprintf("\x1b[38;2;%d;%d;%dm", r, g, b)
		fmt.Println(color + line + reset)
	}
	fmt.Println()
	fmt.Println("Trifle CLI — time-series metrics")
	fmt.Println()
	fmt.Println("Read metrics:")
	fmt.Println("  trifle metrics get --key event::logs --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println()
	fmt.Println("Format series:")
	fmt.Println("  trifle metrics timeline --key event::logs --value-path count --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println("  trifle metrics category --key event::logs --value-path duration --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println()
	fmt.Println("Aggregate series:")
	fmt.Println("  trifle metrics aggregate --key event::logs --value-path count --aggregator sum --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println()
	fmt.Println("Submit data:")
	fmt.Println("  trifle metrics push --key event::logs --values '{\"count\":1,\"duration\":2.4}'")
	fmt.Println("  trifle metrics push --key event::logs --values '{\"count\":1}' --at 2026-01-01T12:00:00Z")
	fmt.Println()
	fmt.Println("Local drivers:")
	fmt.Println("  trifle metrics setup --driver sqlite --db ./stats.db")
	fmt.Println("  trifle metrics get --driver sqlite --db ./stats.db --key event::logs --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println("  trifle metrics setup --driver postgres --host 127.0.0.1 --port 5432 --user postgres --password password --database trifle_stats")
	fmt.Println("  trifle metrics setup --driver mysql --host 127.0.0.1 --port 3306 --user root --password password --database trifle_stats")
	fmt.Println("  trifle metrics setup --driver mongo --dsn mongodb://127.0.0.1:27017 --database trifle_stats --collection trifle_stats")
	fmt.Println("  trifle metrics get --driver redis --prefix trifle:metrics --key event::logs --from 2026-01-01T00:00:00Z --to 2026-01-31T00:00:00Z --granularity 1d")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  metrics        Query or push metrics")
	fmt.Println("  transponders   Manage transponders")
	fmt.Println("  mcp            MCP server mode")
	fmt.Println("  version        Print version")
	fmt.Println()
	fmt.Println("Run 'trifle <command> --help' for details.")
	fmt.Println("Learn more at https://docs.trifle.io/trifle-cli")
}

func metricsUsage() {
	fmt.Println("trifle metrics <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  get       Fetch raw timeseries data")
	fmt.Println("  keys      List available metric keys")
	fmt.Println("  aggregate Aggregate a metric series")
	fmt.Println("  timeline  Format a metric timeline")
	fmt.Println("  category  Format a metric category breakdown")
	fmt.Println("  push      Submit a metric payload")
	fmt.Println("  setup     Initialize local storage (sqlite/postgres/mysql/mongo; redis is no-op)")
}

func transponderUsage() {
	fmt.Println("trifle transponders <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  list    List transponders")
	fmt.Println("  create  Create a transponder")
	fmt.Println("  update  Update a transponder")
	fmt.Println("  delete  Delete a transponder")
}

func exitError(err error) {
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		fmt.Fprintln(os.Stderr, apiErr.Error())
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	os.Exit(1)
}
