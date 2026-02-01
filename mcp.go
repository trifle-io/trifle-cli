package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/trifle-io/trifle-cli/internal/api"
	triflestats "github.com/trifle-io/trifle_stats_go"
)

const mcpProtocolVersion = "2024-11-05"

func runMCP(args []string) {
	rc, err := resolveCommandConfig(args)
	if err != nil {
		exitError(err)
	}

	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	addConfigFlag(fs, rc.ConfigPath)
	addSourceFlag(fs, rc.SourceName)
	opts := addCommonFlags(fs, &rc.Source)
	driverOpts := addDriverFlags(fs, &rc.Source)
	fs.Parse(args)

	driverName := strings.ToLower(strings.TrimSpace(driverOpts.Driver))
	if driverName == "" {
		driverName = "api"
	}

	if isLocalDriver(driverName) {
		local, err := loadLocalConfig(driverOpts)
		if err != nil {
			exitError(err)
		}

		state := &mcpState{
			Driver: local.DriverName,
			Local:  local,
		}

		if err := serveMCP(context.Background(), state); err != nil {
			exitError(err)
		}
		return
	}

	if err := ensureToken(opts, false); err != nil {
		exitError(err)
	}

	client, err := newClient(opts)
	if err != nil {
		exitError(err)
	}

	state := &mcpState{
		Driver: "api",
		API:    client,
	}

	if err := serveMCP(context.Background(), state); err != nil {
		exitError(err)
	}
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return fmt.Sprintf("rpc error %d", e.Code)
	}
	return e.Message
}

type toolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type toolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type initializeParams struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	ClientInfo      map[string]any `json:"clientInfo"`
}

type resourceDescriptor struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description"`
	MimeType    string `json:"mimeType,omitempty"`
}

type resourceReadParams struct {
	URI string `json:"uri"`
}

type contentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolResult struct {
	Content []contentItem `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type resourceReadResult struct {
	Contents []map[string]any `json:"contents"`
}

type mcpState struct {
	Driver string
	API    *api.Client
	Local  *localDriverRuntime
}

func serveMCP(ctx context.Context, state *mcpState) error {
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	decoder.UseNumber()
	encoder := json.NewEncoder(os.Stdout)

	for {
		var req rpcRequest
		if err := decoder.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if req.JSONRPC == "" {
			continue
		}

		response, err := handleMCPRequest(ctx, state, req)
		if err != nil {
			if len(req.ID) == 0 {
				continue
			}

			rpcErr, ok := err.(*rpcError)
			if !ok {
				rpcErr = &rpcError{Code: -32603, Message: err.Error()}
			}

			response = &rpcResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   rpcErr,
			}
		}

		if response == nil {
			continue
		}

		if err := encoder.Encode(response); err != nil {
			return err
		}

		if req.Method == "exit" {
			return nil
		}
	}
}

func handleMCPRequest(ctx context.Context, state *mcpState, req rpcRequest) (*rpcResponse, error) {
	switch req.Method {
	case "initialize":
		params := initializeParams{}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return nil, invalidParamsError("invalid initialize params")
			}
		}

		protocol := params.ProtocolVersion
		if protocol == "" {
			protocol = mcpProtocolVersion
		}

		result := map[string]any{
			"protocolVersion": protocol,
			"capabilities": map[string]any{
				"tools": map[string]any{
					"listChanged": false,
				},
				"resources": map[string]any{
					"listChanged": false,
				},
			},
			"serverInfo": map[string]any{
				"name":    "trifle-cli",
				"version": version,
			},
		}

		return rpcResult(req.ID, result), nil
	case "initialized":
		return nil, nil
	case "shutdown":
		return rpcResult(req.ID, map[string]any{}), nil
	case "exit":
		return rpcResult(req.ID, map[string]any{}), nil
	case "tools/list":
		return rpcResult(req.ID, map[string]any{"tools": toolDefinitions(state.Driver)}), nil
	case "tools/call":
		return handleToolCall(ctx, state, req)
	case "resources/list":
		return rpcResult(req.ID, map[string]any{"resources": resourceList(state.Driver)}), nil
	case "resources/read":
		return handleResourceRead(ctx, state, req)
	default:
		return nil, methodNotFoundError(fmt.Sprintf("method not found: %s", req.Method))
	}
}

func handleToolCall(ctx context.Context, state *mcpState, req rpcRequest) (*rpcResponse, error) {
	if len(req.Params) == 0 {
		return nil, invalidParamsError("missing params")
	}

	var params toolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return nil, invalidParamsError("invalid tool call params")
	}

	if params.Name == "" {
		return nil, invalidParamsError("tool name required")
	}

	if params.Arguments == nil {
		params.Arguments = map[string]any{}
	}

	result, err := executeTool(ctx, state, params.Name, params.Arguments)
	if err != nil {
		return rpcResult(req.ID, toolErrorResult(err)), nil
	}

	return rpcResult(req.ID, result), nil
}

func handleResourceRead(ctx context.Context, state *mcpState, req rpcRequest) (*rpcResponse, error) {
	if len(req.Params) == 0 {
		return nil, invalidParamsError("missing params")
	}

	var params resourceReadParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return nil, invalidParamsError("invalid resource params")
	}

	if params.URI == "" {
		return nil, invalidParamsError("uri required")
	}

	payload, err := readResource(ctx, state, params.URI)
	if err != nil {
		return rpcResult(req.ID, toolErrorResult(err)), nil
	}

	return rpcResult(req.ID, payload), nil
}

func executeTool(ctx context.Context, state *mcpState, name string, args map[string]any) (toolResult, error) {
	switch name {
	case "list_metrics":
		payload, err := listMetricsPayload(ctx, state, args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "fetch_series":
		payload, err := fetchSeriesPayload(ctx, state, args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "aggregate_series":
		payload, err := queryPayload(ctx, state, "aggregate", args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "format_timeline":
		payload, err := queryPayload(ctx, state, "timeline", args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "format_category":
		payload, err := queryPayload(ctx, state, "category", args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "write_metric":
		payload, err := writeMetricPayload(ctx, state, args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "list_transponders":
		payload, err := listTranspondersPayload(ctx, state)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	case "delete_transponder":
		payload, err := deleteTransponderPayload(ctx, state, args)
		if err != nil {
			return toolResult{}, err
		}
		return toolResultFromJSON(payload), nil
	default:
		return toolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
}

func listMetricsPayload(ctx context.Context, state *mcpState, args map[string]any) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return listMetricsPayloadLocal(state, args)
	}

	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityValue(ctx, client, getStringArg(args, "granularity"))
	if err != nil {
		return nil, err
	}

	params := map[string]string{
		"from":        from,
		"to":          to,
		"granularity": granularity,
	}

	var response metricsResponse
	if err := client.GetMetrics(ctx, params, &response); err != nil {
		return nil, err
	}

	entries := summarizeKeys(response.Data.Values)

	payload := map[string]any{
		"status": "ok",
		"timeframe": map[string]string{
			"from":        from,
			"to":          to,
			"granularity": granularity,
		},
		"paths":       entries,
		"total_paths": len(entries),
	}

	return payload, nil
}

func fetchSeriesPayload(ctx context.Context, state *mcpState, args map[string]any) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return fetchSeriesPayloadLocal(state, args)
	}

	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityValue(ctx, client, getStringArg(args, "granularity"))
	if err != nil {
		return nil, err
	}

	params := map[string]string{
		"from":        from,
		"to":          to,
		"granularity": granularity,
	}

	key := strings.TrimSpace(getStringArg(args, "key"))
	if key != "" {
		params["key"] = key
	}

	var response metricsResponse
	if err := client.GetMetrics(ctx, params, &response); err != nil {
		return nil, err
	}

	usedKey := key
	if usedKey == "" {
		usedKey = systemMetricsKey
	}

	payload := map[string]any{
		"status":     "ok",
		"metric_key": usedKey,
		"timeframe": map[string]string{
			"from":        from,
			"to":          to,
			"granularity": granularity,
		},
		"data": response.Data,
	}

	return payload, nil
}

func queryPayload(ctx context.Context, state *mcpState, mode string, args map[string]any) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return queryPayloadLocal(state, mode, args)
	}

	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	key := strings.TrimSpace(getStringArg(args, "key"))
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	valuePath := strings.TrimSpace(getStringArg(args, "value_path"))
	if valuePath == "" {
		return nil, fmt.Errorf("value_path is required")
	}

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityValue(ctx, client, getStringArg(args, "granularity"))
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"mode":        mode,
		"key":         key,
		"value_path":  valuePath,
		"from":        from,
		"to":          to,
		"granularity": granularity,
	}

	if mode == "aggregate" {
		aggregator := strings.TrimSpace(getStringArg(args, "aggregator"))
		if aggregator == "" {
			return nil, fmt.Errorf("aggregator is required")
		}
		payload["aggregator"] = aggregator
	}

	if slicesValue, ok := args["slices"]; ok {
		payload["slices"] = slicesValue
	}

	data, err := queryMetrics(ctx, client, payload)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func writeMetricPayload(ctx context.Context, state *mcpState, args map[string]any) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return writeMetricPayloadLocal(state, args)
	}

	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	key := strings.TrimSpace(getStringArg(args, "key"))
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("values is required")
	}

	at := strings.TrimSpace(getStringArg(args, "at"))
	if at == "" {
		at = time.Now().UTC().Format(time.RFC3339)
	} else if err := validateTimestamp("at", at); err != nil {
		return nil, err
	}

	payload := map[string]any{
		"key":    key,
		"at":     at,
		"values": values,
	}

	var response map[string]any
	if err := client.PostMetrics(ctx, payload, &response); err != nil {
		return nil, err
	}

	return response, nil
}

func listTranspondersPayload(ctx context.Context, state *mcpState) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return nil, fmt.Errorf("transponders are only available for api drivers")
	}
	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	var response map[string]any
	if err := client.GetTransponders(ctx, &response); err != nil {
		return nil, err
	}

	return response, nil
}

func deleteTransponderPayload(ctx context.Context, state *mcpState, args map[string]any) (map[string]any, error) {
	if state != nil && state.Local != nil {
		return nil, fmt.Errorf("transponders are only available for api drivers")
	}
	if state == nil || state.API == nil {
		return nil, fmt.Errorf("api client is not configured")
	}
	client := state.API

	id := strings.TrimSpace(getStringArg(args, "id"))
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	var response map[string]any
	if err := client.DeleteTransponder(ctx, id, &response); err != nil {
		return nil, err
	}

	return response, nil
}

func listMetricsPayloadLocal(state *mcpState, args map[string]any) (map[string]any, error) {
	if state == nil || state.Local == nil || state.Local.Config == nil {
		return nil, fmt.Errorf("local driver is not configured")
	}

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityLocal(getStringArg(args, "granularity"), state.Local.Config)
	if err != nil {
		return nil, err
	}

	fromTime, err := time.Parse(time.RFC3339Nano, from)
	if err != nil {
		return nil, err
	}
	toTime, err := time.Parse(time.RFC3339Nano, to)
	if err != nil {
		return nil, err
	}

	result, err := triflestats.Values(state.Local.Config, systemMetricsKey, fromTime, toTime, granularity, true)
	if err != nil {
		return nil, maybeSuggestSetup(err, state.Local.DriverName, state.Local.TableName)
	}

	entries := summarizeSystemKeys(result.Values)

	payload := map[string]any{
		"status": "ok",
		"timeframe": map[string]string{
			"from":        from,
			"to":          to,
			"granularity": granularity,
		},
		"paths":       entries,
		"total_paths": len(entries),
	}

	return payload, nil
}

func fetchSeriesPayloadLocal(state *mcpState, args map[string]any) (map[string]any, error) {
	if state == nil || state.Local == nil || state.Local.Config == nil {
		return nil, fmt.Errorf("local driver is not configured")
	}

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityLocal(getStringArg(args, "granularity"), state.Local.Config)
	if err != nil {
		return nil, err
	}

	fromTime, err := time.Parse(time.RFC3339Nano, from)
	if err != nil {
		return nil, err
	}
	toTime, err := time.Parse(time.RFC3339Nano, to)
	if err != nil {
		return nil, err
	}

	key := strings.TrimSpace(getStringArg(args, "key"))
	usedKey := key
	if usedKey == "" {
		usedKey = systemMetricsKey
	}

	result, err := triflestats.Values(state.Local.Config, usedKey, fromTime, toTime, granularity, false)
	if err != nil {
		return nil, maybeSuggestSetup(err, state.Local.DriverName, state.Local.TableName)
	}

	payload := map[string]any{
		"status":     "ok",
		"metric_key": usedKey,
		"timeframe": map[string]string{
			"from":        from,
			"to":          to,
			"granularity": granularity,
		},
		"data": map[string]any{
			"at":     result.At,
			"values": result.Values,
		},
	}

	return payload, nil
}

func queryPayloadLocal(state *mcpState, mode string, args map[string]any) (map[string]any, error) {
	if state == nil || state.Local == nil || state.Local.Config == nil {
		return nil, fmt.Errorf("local driver is not configured")
	}

	key := strings.TrimSpace(getStringArg(args, "key"))
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	valuePath := strings.TrimSpace(getStringArg(args, "value_path"))
	if valuePath == "" {
		return nil, fmt.Errorf("value_path is required")
	}

	if err := ensureNoWildcards(valuePath); err != nil {
		return nil, err
	}

	from, to, err := resolveTimeRange(getStringArg(args, "from"), getStringArg(args, "to"))
	if err != nil {
		return nil, err
	}

	granularity, err := resolveGranularityLocal(getStringArg(args, "granularity"), state.Local.Config)
	if err != nil {
		return nil, err
	}

	fromTime, err := time.Parse(time.RFC3339Nano, from)
	if err != nil {
		return nil, err
	}
	toTime, err := time.Parse(time.RFC3339Nano, to)
	if err != nil {
		return nil, err
	}

	seriesResult, err := triflestats.Values(state.Local.Config, key, fromTime, toTime, granularity, false)
	if err != nil {
		return nil, maybeSuggestSetup(err, state.Local.DriverName, state.Local.TableName)
	}

	series := triflestats.SeriesFromResult(seriesResult)
	available := series.AvailablePaths()
	slices := getIntArg(args, "slices", 1)
	if slices < 1 {
		slices = 1
	}

	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "aggregate":
		if len(available) == 0 {
			return nil, fmt.Errorf("no data available for path %s in the selected timeframe", valuePath)
		}
		if !containsString(available, valuePath) {
			return nil, fmt.Errorf("unknown path: %s", valuePath)
		}

		aggName := strings.ToLower(strings.TrimSpace(getStringArg(args, "aggregator")))
		if aggName == "" {
			return nil, fmt.Errorf("aggregator is required")
		}

		var values []any
		switch aggName {
		case "sum":
			values = series.AggregateSum(valuePath, slices)
		case "mean":
			values = series.AggregateMean(valuePath, slices)
		case "min":
			values = series.AggregateMin(valuePath, slices)
		case "max":
			values = series.AggregateMax(valuePath, slices)
		default:
			return nil, fmt.Errorf("unsupported aggregator %q", aggName)
		}

		values = normalizeNumericSlice(values)
		if len(values) == 0 {
			return nil, fmt.Errorf("no data available for path %s in the selected timeframe", valuePath)
		}

		payload := map[string]any{
			"status":          "ok",
			"aggregator":      aggName,
			"metric_key":      key,
			"value_path":      valuePath,
			"slices":          slices,
			"values":          values,
			"count":           len(values),
			"timeframe":       buildTimeframePayload(from, to, granularity),
			"available_paths": available,
			"matched_paths":   []string{valuePath},
		}

		if slices == 1 && len(values) > 0 && values[0] != nil {
			payload["value"] = values[0]
		}
		if table := buildSeriesTable(series, []string{valuePath}); table != nil {
			payload["table"] = table
		}
		return payload, nil
	case "timeline":
		formatted := series.FormatTimeline(valuePath, slices, nil)
		matched := filterAvailable(mapKeys(formatted), available)
		if len(matched) == 0 {
			return nil, fmt.Errorf("no matching data found for path %s in the selected timeframe", valuePath)
		}

		payload := map[string]any{
			"status":          "ok",
			"formatter":       "timeline",
			"metric_key":      key,
			"value_path":      valuePath,
			"slices":          slices,
			"timeframe":       buildTimeframePayload(from, to, granularity),
			"result":          formatted,
			"available_paths": available,
			"matched_paths":   matched,
		}

		if table := buildSeriesTable(series, matched); table != nil {
			payload["table"] = table
		}
		return payload, nil
	case "category":
		formatted := series.FormatCategory(valuePath, slices, nil)
		matched := filterAvailable(extractCategoryPaths(formatted), available)
		if len(matched) == 0 {
			return nil, fmt.Errorf("no matching data found for path %s in the selected timeframe", valuePath)
		}

		payload := map[string]any{
			"status":          "ok",
			"formatter":       "category",
			"metric_key":      key,
			"value_path":      valuePath,
			"slices":          slices,
			"timeframe":       buildTimeframePayload(from, to, granularity),
			"result":          formatted,
			"available_paths": available,
			"matched_paths":   matched,
		}

		if table := buildSeriesTable(series, matched); table != nil {
			payload["table"] = table
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", mode)
	}
}

func writeMetricPayloadLocal(state *mcpState, args map[string]any) (map[string]any, error) {
	if state == nil || state.Local == nil || state.Local.Config == nil {
		return nil, fmt.Errorf("local driver is not configured")
	}

	key := strings.TrimSpace(getStringArg(args, "key"))
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	values, ok := args["values"]
	if !ok {
		return nil, fmt.Errorf("values is required")
	}

	at := strings.TrimSpace(getStringArg(args, "at"))
	if at == "" {
		at = time.Now().UTC().Format(time.RFC3339)
	} else if err := validateTimestamp("at", at); err != nil {
		return nil, err
	}

	atTime, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, err
	}

	valuesMap, err := ensureValuesMap(values)
	if err != nil {
		return nil, err
	}

	if err := performLocalWrite(state.Local.Config, "track", key, atTime, valuesMap); err != nil {
		return nil, maybeSuggestSetup(err, state.Local.DriverName, state.Local.TableName)
	}

	response := map[string]any{
		"data": map[string]any{
			"key":    key,
			"at":     atTime,
			"values": valuesMap,
		},
	}

	return response, nil
}

func sourcePayloadFromConfig(cfg *triflestats.Config) map[string]any {
	available := []string{}
	defaultGranularity := ""
	if cfg != nil {
		available = cfg.EffectiveGranularities()
		if value, err := resolveGranularityLocal("", cfg); err == nil {
			defaultGranularity = value
		}
	}

	return map[string]any{
		"data": map[string]any{
			"default_granularity":     defaultGranularity,
			"available_granularities": available,
		},
	}
}

func readResource(ctx context.Context, state *mcpState, uri string) (resourceReadResult, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return resourceReadResult{}, err
	}

	if parsed.Scheme != "trifle" {
		return resourceReadResult{}, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
	}

	switch parsed.Host {
	case "source":
		if state != nil && state.Local != nil {
			payload := sourcePayloadFromConfig(state.Local.Config)
			return resourceResult(uri, payload)
		}
		if state == nil || state.API == nil {
			return resourceReadResult{}, fmt.Errorf("api client is not configured")
		}
		var response map[string]any
		if err := state.API.GetSource(ctx, &response); err != nil {
			return resourceReadResult{}, err
		}
		return resourceResult(uri, response)
	case "transponders":
		if state != nil && state.Local != nil {
			return resourceReadResult{}, fmt.Errorf("transponders are only available for api drivers")
		}
		if state == nil || state.API == nil {
			return resourceReadResult{}, fmt.Errorf("api client is not configured")
		}
		var response map[string]any
		if err := state.API.GetTransponders(ctx, &response); err != nil {
			return resourceReadResult{}, err
		}
		return resourceResult(uri, response)
	case "metrics":
		key := strings.TrimPrefix(parsed.Path, "/")
		query := parsed.Query()
		args := map[string]any{
			"from":        query.Get("from"),
			"to":          query.Get("to"),
			"granularity": query.Get("granularity"),
		}
		if key != "" {
			args["key"] = key
			payload, err := fetchSeriesPayload(ctx, state, args)
			if err != nil {
				return resourceReadResult{}, err
			}
			return resourceResult(uri, payload)
		}

		payload, err := listMetricsPayload(ctx, state, args)
		if err != nil {
			return resourceReadResult{}, err
		}
		return resourceResult(uri, payload)
	default:
		return resourceReadResult{}, fmt.Errorf("unknown resource: %s", parsed.Host)
	}
}

func resourceList(driverName string) []resourceDescriptor {
	resources := []resourceDescriptor{
		{
			URI:         "trifle://source",
			Name:        "Source configuration",
			Description: "Active analytics source configuration (defaults and granularities).",
			MimeType:    "application/json",
		},
		{
			URI:         "trifle://metrics",
			Name:        "Metrics listing",
			Description: "Available metrics from __system__key__ (use ?from&to RFC3339, granularity like 1h).",
			MimeType:    "application/json",
		},
		{
			URI:         "trifle://metrics/{key}",
			Name:        "Metric series",
			Description: "Raw series for a metric key (use ?from&to RFC3339, granularity like 1h).",
			MimeType:    "application/json",
		},
	}

	if strings.EqualFold(driverName, "api") || strings.TrimSpace(driverName) == "" {
		resources = append(resources, resourceDescriptor{
			URI:         "trifle://transponders",
			Name:        "Transponders",
			Description: "List transponders for the active source.",
			MimeType:    "application/json",
		})
	}

	return resources
}

func toolDefinitions(driverName string) []toolDefinition {
	timestampSchema := map[string]any{
		"type":        "string",
		"description": "RFC3339 timestamp (e.g. 2024-01-02T15:04:05Z).",
	}
	granularitySchema := map[string]any{
		"type":        "string",
		"description": "Granularity as <number><unit> (e.g. 1m, 1h, 1d).",
		"pattern":     "^\\d+(s|m|h|d|w|mo|q|y)$",
	}

	tools := []toolDefinition{
		{
			Name:        "list_metrics",
			Description: "List available metric keys from the system series.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"from":        timestampSchema,
					"to":          timestampSchema,
					"granularity": granularitySchema,
				},
			},
		},
		{
			Name:        "fetch_series",
			Description: "Fetch raw series data for a metric key.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":         map[string]any{"type": "string"},
					"from":        timestampSchema,
					"to":          timestampSchema,
					"granularity": granularitySchema,
				},
			},
		},
		{
			Name:        "aggregate_series",
			Description: "Aggregate a metric series (sum, mean, min, max).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":         map[string]any{"type": "string"},
					"value_path":  map[string]any{"type": "string"},
					"aggregator":  map[string]any{"type": "string", "enum": []string{"sum", "mean", "min", "max"}},
					"from":        timestampSchema,
					"to":          timestampSchema,
					"granularity": granularitySchema,
					"slices":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required": []string{"key", "value_path", "aggregator"},
			},
		},
		{
			Name:        "format_timeline",
			Description: "Format a metric series into timeline entries.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":         map[string]any{"type": "string"},
					"value_path":  map[string]any{"type": "string"},
					"from":        timestampSchema,
					"to":          timestampSchema,
					"granularity": granularitySchema,
					"slices":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required": []string{"key", "value_path"},
			},
		},
		{
			Name:        "format_category",
			Description: "Format a metric series into categorical totals.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key":         map[string]any{"type": "string"},
					"value_path":  map[string]any{"type": "string"},
					"from":        timestampSchema,
					"to":          timestampSchema,
					"granularity": granularitySchema,
					"slices":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required": []string{"key", "value_path"},
			},
		},
		{
			Name:        "write_metric",
			Description: "Write a metric event.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{"type": "string"},
					"at":  timestampSchema,
					"values": map[string]any{
						"type": []string{"object", "array", "string", "number", "boolean", "null"},
					},
				},
				"required": []string{"key", "values"},
			},
		},
	}

	if strings.EqualFold(driverName, "api") || strings.TrimSpace(driverName) == "" {
		tools = append(tools,
			toolDefinition{
				Name:        "list_transponders",
				Description: "List transponders for the active source.",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{},
				},
			},
			toolDefinition{
				Name:        "delete_transponder",
				Description: "Delete a transponder by id.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id": map[string]any{"type": "string"},
					},
					"required": []string{"id"},
				},
			},
		)
	}

	return tools
}

func toolResultFromJSON(payload any) toolResult {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return toolErrorResult(err)
	}

	return toolResult{
		Content: []contentItem{
			{Type: "text", Text: string(encoded)},
		},
	}
}

func toolErrorResult(err error) toolResult {
	return toolResult{
		Content: []contentItem{
			{Type: "text", Text: err.Error()},
		},
		IsError: true,
	}
}

func resourceResult(uri string, payload any) (resourceReadResult, error) {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return resourceReadResult{}, err
	}

	return resourceReadResult{
		Contents: []map[string]any{
			{
				"uri":      uri,
				"mimeType": "application/json",
				"text":     string(encoded),
			},
		},
	}, nil
}

func rpcResult(id json.RawMessage, result any) *rpcResponse {
	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
}

func invalidParamsError(message string) *rpcError {
	return &rpcError{Code: -32602, Message: message}
}

func methodNotFoundError(message string) *rpcError {
	return &rpcError{Code: -32601, Message: message}
}

func getStringArg(args map[string]any, key string) string {
	value, ok := args[key]
	if !ok || value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func getIntArg(args map[string]any, key string, fallback int) int {
	value, ok := args[key]
	if !ok || value == nil {
		return fallback
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			return int(parsed)
		}
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return parsed
		}
	}

	return fallback
}
