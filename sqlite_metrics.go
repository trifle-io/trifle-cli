package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	triflestats "github.com/trifle-io/trifle_stats_go"
)

func buildTimeframePayload(fromValue, toValue, granularity string) map[string]string {
	return map[string]string{
		"from":        fromValue,
		"to":          toValue,
		"label":       "custom",
		"granularity": granularity,
	}
}

func buildSeriesTable(series triflestats.Series, paths []string) map[string]any {
	paths = uniqueStrings(paths)
	if len(paths) == 0 || len(series.At) == 0 {
		return nil
	}

	columns := make([]any, 0, len(paths)+1)
	columns = append(columns, "at")
	for _, path := range paths {
		columns = append(columns, path)
	}

	rows := make([]any, 0, len(series.At))
	for i, at := range series.At {
		row := make([]any, 0, len(paths)+1)
		row = append(row, at.Format(time.RFC3339))

		var values map[string]any
		if i < len(series.Values) {
			values = series.Values[i]
		}

		for _, path := range paths {
			var cell any
			if values != nil {
				cell = triflestats.NormalizeNumeric(triflestats.FetchPath(values, path))
			}
			row = append(row, cell)
		}
		rows = append(rows, row)
	}

	return map[string]any{
		"columns": columns,
		"rows":    rows,
	}
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		seen[value] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func normalizeNumericSlice(values []any) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		normalized := triflestats.NormalizeNumeric(value)
		if normalized == nil {
			continue
		}
		out = append(out, normalized)
	}
	return out
}

func filterAvailable(paths, available []string) []string {
	if len(paths) == 0 || len(available) == 0 {
		return []string{}
	}
	allowed := map[string]struct{}{}
	for _, value := range available {
		allowed[value] = struct{}{}
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, ok := allowed[path]; ok {
			out = append(out, path)
		}
	}
	return uniqueStrings(out)
}

func extractCategoryPaths(result any) []string {
	switch value := result.(type) {
	case map[string]any:
		return mapKeys(value)
	case []map[string]any:
		keys := map[string]struct{}{}
		for _, entry := range value {
			for key := range entry {
				keys[key] = struct{}{}
			}
		}
		return sortedKeys(keys)
	case []any:
		keys := map[string]struct{}{}
		for _, entry := range value {
			if mapEntry, ok := entry.(map[string]any); ok {
				for key := range mapEntry {
					keys[key] = struct{}{}
				}
			}
		}
		return sortedKeys(keys)
	}
	return []string{}
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeys(keys map[string]struct{}) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func ensureNoWildcards(value string) error {
	if strings.Contains(value, "*") {
		return fmt.Errorf("wildcards are not supported in value_path %q", value)
	}
	return nil
}
