package main

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	triflestats "github.com/trifle-io/trifle_stats_go"
)

func TestIsLocalDriver(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"sqlite":   true,
		"postgres": true,
		"mysql":    true,
		"redis":    true,
		"mongo":    true,
		"mongodb":  true,
		"api":      false,
		"":         false,
	}

	for input, want := range cases {
		got := isLocalDriver(input)
		if got != want {
			t.Fatalf("isLocalDriver(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestApplyBufferOptions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		driverName   string
		opts         driverOptions
		wantEnabled  bool
		wantDuration time.Duration
		wantSize     int
		wantAgg      bool
		wantAsync    bool
	}{
		{
			name:       "auto enables sqlite",
			driverName: "sqlite",
			opts: driverOptions{
				BufferMode:      "auto",
				BufferDuration:  2 * time.Second,
				BufferSize:      10,
				BufferAggregate: true,
				BufferAsync:     true,
			},
			wantEnabled:  true,
			wantDuration: 2 * time.Second,
			wantSize:     10,
			wantAgg:      true,
			wantAsync:    true,
		},
		{
			name:       "auto disables redis",
			driverName: "redis",
			opts: driverOptions{
				BufferMode:      "auto",
				BufferDuration:  3 * time.Second,
				BufferSize:      8,
				BufferAggregate: false,
				BufferAsync:     false,
			},
			wantEnabled:  false,
			wantDuration: 3 * time.Second,
			wantSize:     8,
			wantAgg:      false,
			wantAsync:    false,
		},
		{
			name:       "on enables non default driver",
			driverName: "redis",
			opts: driverOptions{
				BufferMode: "on",
			},
			wantEnabled: true,
		},
		{
			name:       "off disables even default driver",
			driverName: "postgres",
			opts: driverOptions{
				BufferMode: "off",
			},
			wantEnabled: false,
		},
		{
			name:       "allowlist disables excluded driver",
			driverName: "mysql",
			opts: driverOptions{
				BufferMode:    "on",
				BufferDrivers: "sqlite,postgres",
			},
			wantEnabled: false,
		},
		{
			name:       "allowlist keeps included driver",
			driverName: "mysql",
			opts: driverOptions{
				BufferMode:    "on",
				BufferDrivers: "sqlite,mysql",
			},
			wantEnabled: true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := triflestats.DefaultConfig()
			applyBufferOptions(cfg, &tt.opts, tt.driverName)

			if cfg.BufferEnabled != tt.wantEnabled {
				t.Fatalf("BufferEnabled = %v, want %v", cfg.BufferEnabled, tt.wantEnabled)
			}
			if tt.wantDuration != 0 && cfg.BufferDuration != tt.wantDuration {
				t.Fatalf("BufferDuration = %v, want %v", cfg.BufferDuration, tt.wantDuration)
			}
			if tt.wantSize != 0 && cfg.BufferSize != tt.wantSize {
				t.Fatalf("BufferSize = %d, want %d", cfg.BufferSize, tt.wantSize)
			}
			if cfg.BufferAggregate != tt.wantAgg {
				t.Fatalf("BufferAggregate = %v, want %v", cfg.BufferAggregate, tt.wantAgg)
			}
			if cfg.BufferAsync != tt.wantAsync {
				t.Fatalf("BufferAsync = %v, want %v", cfg.BufferAsync, tt.wantAsync)
			}
		})
	}
}

func TestLoadLocalConfigSQLiteRequiresDB(t *testing.T) {
	t.Parallel()

	_, err := loadLocalConfig(&driverOptions{
		Driver:          "sqlite",
		Joined:          "full",
		BeginningOfWeek: "monday",
	})
	if err == nil || !strings.Contains(err.Error(), "--db is required for sqlite driver") {
		t.Fatalf("expected sqlite db path error, got %v", err)
	}
}

func TestLoadLocalConfigSQLiteMemory(t *testing.T) {
	t.Parallel()

	local, err := loadLocalConfig(&driverOptions{
		Driver:          "sqlite",
		DBPath:          ":memory:",
		Table:           "metrics",
		Joined:          "full",
		Separator:       "::",
		TimeZone:        "UTC",
		BeginningOfWeek: "monday",
		BufferMode:      "off",
	})
	if err != nil {
		t.Fatalf("loadLocalConfig returned error: %v", err)
	}

	if _, ok := local.Config.Driver.(*triflestats.SQLiteDriver); !ok {
		t.Fatalf("expected sqlite driver, got %T", local.Config.Driver)
	}

	if err := local.Setup(); err != nil {
		t.Fatalf("local.Setup returned error: %v", err)
	}
}

func TestLoadLocalConfigRedis(t *testing.T) {
	t.Parallel()

	local, err := loadLocalConfig(&driverOptions{
		Driver:          "redis",
		Prefix:          "metrics:",
		Joined:          "full",
		Separator:       "::",
		TimeZone:        "UTC",
		BeginningOfWeek: "monday",
		BufferMode:      "off",
	})
	if err != nil {
		t.Fatalf("loadLocalConfig returned error: %v", err)
	}

	if _, ok := local.Config.Driver.(*triflestats.RedisDriver); !ok {
		t.Fatalf("expected redis driver, got %T", local.Config.Driver)
	}
	if local.TableName != "metrics:" {
		t.Fatalf("TableName = %q, want %q", local.TableName, "metrics:")
	}
	if err := local.Setup(); err != nil {
		t.Fatalf("redis setup should be no-op, got %v", err)
	}
}

func TestLoadLocalConfigPostgresAndMySQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		driver string
	}{
		{driver: "postgres"},
		{driver: "mysql"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.driver, func(t *testing.T) {
			t.Parallel()

			local, err := loadLocalConfig(&driverOptions{
				Driver:          tt.driver,
				Table:           "metrics",
				Joined:          "full",
				Separator:       "::",
				TimeZone:        "UTC",
				BeginningOfWeek: "monday",
				BufferMode:      "auto",
			})
			if err != nil {
				t.Fatalf("loadLocalConfig returned error: %v", err)
			}

			switch tt.driver {
			case "postgres":
				if _, ok := local.Config.Driver.(*triflestats.PostgresDriver); !ok {
					t.Fatalf("expected postgres driver, got %T", local.Config.Driver)
				}
			case "mysql":
				if _, ok := local.Config.Driver.(*triflestats.MySQLDriver); !ok {
					t.Fatalf("expected mysql driver, got %T", local.Config.Driver)
				}
			}
		})
	}
}

func TestResolveDatabaseName(t *testing.T) {
	t.Parallel()

	if got := resolveDatabaseName(&driverOptions{Database: "explicit"}, "fallback"); got != "explicit" {
		t.Fatalf("resolveDatabaseName explicit = %q, want %q", got, "explicit")
	}
	if got := resolveDatabaseName(&driverOptions{Driver: "postgres", DBPath: "legacy_db"}, "fallback"); got != "legacy_db" {
		t.Fatalf("resolveDatabaseName legacy = %q, want %q", got, "legacy_db")
	}
	if got := resolveDatabaseName(&driverOptions{Driver: "sqlite", DBPath: "stats.db"}, "fallback"); got != "fallback" {
		t.Fatalf("resolveDatabaseName sqlite fallback = %q, want %q", got, "fallback")
	}
}

func TestBuildDSNHelpers(t *testing.T) {
	t.Parallel()

	pg := buildPostgresDSN(&driverOptions{
		Host:     "db.example.com",
		Port:     "5432",
		User:     "user@name",
		Password: "pa:ss",
		Database: "stats/db",
	})
	if !strings.Contains(pg, "postgres://") || !strings.Contains(pg, "sslmode=disable") {
		t.Fatalf("unexpected postgres dsn: %s", pg)
	}
	if !strings.Contains(pg, "user%40name") || !strings.Contains(pg, "pa%3Ass") || !strings.Contains(pg, "stats%2Fdb") {
		t.Fatalf("postgres dsn should escape credentials and db name: %s", pg)
	}

	mysql := buildMySQLDSN(&driverOptions{
		Host:     "db.example.com",
		Port:     "3306",
		User:     "root",
		Password: "secret",
		Database: "stats",
	})
	if !strings.Contains(mysql, "root:secret@tcp(db.example.com:3306)/stats") {
		t.Fatalf("unexpected mysql dsn: %s", mysql)
	}
	if !strings.Contains(mysql, "parseTime=true") {
		t.Fatalf("mysql dsn missing parseTime flag: %s", mysql)
	}
}

func TestAddDriverFlagsRespectsConfigAndEnv(t *testing.T) {
	clearDriverEnv(t)

	cfg := &sourceConfig{
		Driver:          "postgres",
		DB:              "legacy-db",
		DSN:             "postgres://cfg",
		Host:            "cfg-host",
		Port:            "15432",
		User:            "cfg-user",
		Password:        "cfg-pass",
		Database:        "cfg-db",
		Table:           "cfg-table",
		Collection:      "cfg-collection",
		Prefix:          "cfg-prefix",
		Joined:          "partial",
		Separator:       "__",
		TimeZone:        "UTC",
		WeekStart:       "sunday",
		Granularities:   configStringSlice{"1h", "1d"},
		BufferMode:      "on",
		BufferDrivers:   configStringSlice{"postgres", "mysql"},
		BufferDuration:  "3s",
		BufferSize:      123,
		BufferAggregate: boolPtr(false),
		BufferAsync:     boolPtr(true),
	}

	fs := flag.NewFlagSet("driver", flag.ContinueOnError)
	opts := addDriverFlags(fs, cfg)

	if opts.Driver != "postgres" || opts.DSN != "postgres://cfg" || opts.Database != "cfg-db" {
		t.Fatalf("config values not applied: %+v", opts)
	}
	if opts.BufferMode != "on" || opts.BufferDrivers != "postgres,mysql" {
		t.Fatalf("buffer config values not applied: %+v", opts)
	}
	if opts.BufferDuration != 3*time.Second || opts.BufferSize != 123 {
		t.Fatalf("buffer duration/size not applied: %+v", opts)
	}
	if opts.BufferAggregate != false || opts.BufferAsync != true {
		t.Fatalf("buffer bool settings not applied: %+v", opts)
	}

	t.Setenv("TRIFLE_DRIVER", "redis")
	t.Setenv("TRIFLE_DSN", "redis://127.0.0.1:6379/1")
	t.Setenv("TRIFLE_BUFFER_MODE", "off")
	t.Setenv("TRIFLE_BUFFER_DRIVERS", "redis")

	fs2 := flag.NewFlagSet("driver-env", flag.ContinueOnError)
	optsEnv := addDriverFlags(fs2, cfg)

	if optsEnv.Driver != "redis" || optsEnv.DSN != "redis://127.0.0.1:6379/1" {
		t.Fatalf("env should override config: %+v", optsEnv)
	}
	if optsEnv.BufferMode != "off" || optsEnv.BufferDrivers != "redis" {
		t.Fatalf("env buffer settings should override config: %+v", optsEnv)
	}
}

func TestMaybeSuggestSetup(t *testing.T) {
	t.Parallel()

	sqliteErr := errors.New("SQL logic error: no such table: metrics")
	suggested := maybeSuggestSetup(sqliteErr, "sqlite", "metrics")
	if suggested == sqliteErr {
		t.Fatalf("expected setup hint for sqlite")
	}
	if !strings.Contains(suggested.Error(), "trifle metrics setup --driver sqlite --table metrics") {
		t.Fatalf("unexpected sqlite suggestion: %v", suggested)
	}

	pgErr := errors.New(`ERROR: relation "metrics" does not exist`)
	pgSuggested := maybeSuggestSetup(pgErr, "postgres", "metrics")
	if !strings.Contains(pgSuggested.Error(), "trifle metrics setup --driver postgres --table metrics") {
		t.Fatalf("unexpected postgres suggestion: %v", pgSuggested)
	}

	mysqlErr := errors.New("Error 1146 (42S02): Table 'db.metrics' doesn't exist")
	mysqlSuggested := maybeSuggestSetup(mysqlErr, "mysql", "metrics")
	if !strings.Contains(mysqlSuggested.Error(), "trifle metrics setup --driver mysql --table metrics") {
		t.Fatalf("unexpected mysql suggestion: %v", mysqlSuggested)
	}

	plainErr := errors.New("dial tcp timeout")
	plainSuggested := maybeSuggestSetup(plainErr, "redis", "")
	if plainSuggested != plainErr {
		t.Fatalf("non-setup errors should pass through unchanged")
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func clearDriverEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"TRIFLE_DRIVER",
		"TRIFLE_DB",
		"TRIFLE_DSN",
		"TRIFLE_HOST",
		"TRIFLE_PORT",
		"TRIFLE_USER",
		"TRIFLE_PASSWORD",
		"TRIFLE_DATABASE",
		"TRIFLE_TABLE",
		"TRIFLE_COLLECTION",
		"TRIFLE_PREFIX",
		"TRIFLE_JOINED",
		"TRIFLE_SEPARATOR",
		"TRIFLE_TIMEZONE",
		"TRIFLE_WEEK_START",
		"TRIFLE_GRANULARITIES",
		"TRIFLE_BUFFER_MODE",
		"TRIFLE_BUFFER_DRIVERS",
		"TRIFLE_BUFFER_DURATION",
		"TRIFLE_BUFFER_SIZE",
		"TRIFLE_BUFFER_AGGREGATE",
		"TRIFLE_BUFFER_ASYNC",
	} {
		t.Setenv(key, "")
	}
}
