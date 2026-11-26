package main

import (
	"path/filepath"
	"reflect"
	"testing"

	triflestats "github.com/trifle-io/trifle_stats_go/v2"
)

func TestEscapedValuePathValidation(t *testing.T) {
	for _, path := range []string{`jobs.test\.rb`, `jobs.\*`, `jobs.test\*rb`} {
		if err := ensureNoWildcards(path); err != nil {
			t.Fatalf("rejected literal %q: %v", path, err)
		}
	}
	for _, path := range []string{"jobs.*", "jobs.test*rb", `jobs.\\*`} {
		if err := ensureNoWildcards(path); err == nil {
			t.Fatalf("accepted wildcard %q", path)
		}
	}
}

func TestSummarizeEscapedValuePaths(t *testing.T) {
	rows := []map[string]any{{"jobs": map[string]any{"test.rb": 2, "test": map[string]any{"rb": 3}, "*": 5, "test%2Erb": 7}}}
	entries := summarizeValuePaths(rows)
	got := []string{}
	for _, entry := range entries {
		got = append(got, entry.MetricKey)
	}
	want := []string{`jobs.\*`, "jobs.test%2Erb", "jobs.test.rb", `jobs.test\.rb`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths: %#v != %#v", got, want)
	}
}

func TestEscapedPathsSQLiteWriteAndQuery(t *testing.T) {
	local, err := loadLocalConfig(&driverOptions{
		Driver: "sqlite", DBPath: filepath.Join(t.TempDir(), "stats.sqlite"),
		Table: "escaped_paths", Joined: "full", Separator: "::", TimeZone: "GMT",
		BeginningOfWeek: "monday", Granularities: "1h", BufferMode: "off",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Config.Driver.(*triflestats.SQLiteDriver).DB.Close() })
	if err := local.Setup(); err != nil {
		t.Fatal(err)
	}
	state := &mcpState{Driver: "sqlite", Local: local}
	key := "jobs::Trifle.Monitors.Jobs.DispatchRunner"
	at := "2026-09-12T10:00:00Z"
	_, err = writeMetricPayloadLocal(state, map[string]any{
		"key": key, "at": at,
		"values": map[string]any{"jobs.test.rb": 1, `jobs.test\.rb`: 2, `jobs.\*`: 3, "jobs.percent%2E": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := []string{`jobs.\*`, "jobs.percent%2E", "jobs.test.rb", `jobs.test\.rb`}
	for path, want := range map[string]float64{"jobs.test.rb": 1, `jobs.test\.rb`: 2, `jobs.\*`: 3, "jobs.percent%2E": 4} {
		for _, mode := range []string{"aggregate", "timeline", "category"} {
			t.Run(mode+"/"+path, func(t *testing.T) {
				payload, err := queryPayloadLocal(state, mode, map[string]any{
					"key": key, "value_path": path, "from": at, "to": at, "granularity": "1h", "aggregator": "sum",
				})
				if err != nil {
					t.Fatal(err)
				}
				if payload["metric_key"] != key || !reflect.DeepEqual(payload["available_paths"], wantPaths) {
					t.Fatalf("query changed key/path identity: %#v", payload)
				}
				if !reflect.DeepEqual(payload["matched_paths"], []string{path}) {
					t.Fatalf("wrong matched paths: %#v", payload["matched_paths"])
				}
				rows := payload["table"].(map[string]any)["rows"].([]any)
				if len(rows) != 1 || rows[0].([]any)[1] != want {
					t.Fatalf("wrong query values: %#v", rows)
				}
			})
		}
	}
}
