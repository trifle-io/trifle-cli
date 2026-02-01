package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	triflestats "github.com/trifle-io/trifle_stats_go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type localDriverRuntime struct {
	Config     *triflestats.Config
	DriverName string
	TableName  string
	setupFn    func() error
}

func (r *localDriverRuntime) Setup() error {
	if r == nil || r.setupFn == nil {
		return nil
	}
	return r.setupFn()
}

func isLocalDriver(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "sqlite", "postgres", "mysql", "redis", "mongo", "mongodb":
		return true
	default:
		return false
	}
}

func normalizeDriverName(name string) string {
	value := strings.ToLower(strings.TrimSpace(name))
	if value == "mongodb" {
		return "mongo"
	}
	return value
}

func loadLocalConfig(opts *driverOptions) (*localDriverRuntime, error) {
	if opts == nil {
		return nil, fmt.Errorf("driver options required")
	}

	driverName := normalizeDriverName(opts.Driver)
	if !isLocalDriver(driverName) {
		return nil, fmt.Errorf("unsupported local driver: %s", opts.Driver)
	}

	joined, err := parseJoinedIdentifier(opts.Joined)
	if err != nil {
		return nil, err
	}
	weekStart, err := parseWeekday(opts.BeginningOfWeek)
	if err != nil {
		return nil, err
	}

	cfg := triflestats.DefaultConfig()
	cfg.TimeZone = opts.TimeZone
	cfg.Separator = opts.Separator
	cfg.JoinedIdentifier = joined
	cfg.BeginningOfWeek = weekStart
	cfg.Granularities = parseGranularities(opts.Granularities)
	applyBufferOptions(cfg, opts, driverName)

	runtime := &localDriverRuntime{
		Config:     cfg,
		DriverName: driverName,
		TableName:  strings.TrimSpace(opts.Table),
	}

	switch driverName {
	case "sqlite":
		if strings.TrimSpace(opts.DBPath) == "" {
			return nil, fmt.Errorf("--db is required for sqlite driver")
		}
		db, err := sql.Open("sqlite", opts.DBPath)
		if err != nil {
			return nil, err
		}
		driver := triflestats.NewSQLiteDriver(db, opts.Table, joined)
		driver.Separator = opts.Separator
		cfg.Driver = driver
		runtime.setupFn = driver.Setup
		runtime.TableName = driver.TableName
		return runtime, nil

	case "postgres":
		dsn := buildPostgresDSN(opts)
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, err
		}
		driver := triflestats.NewPostgresDriver(db, opts.Table, joined)
		driver.Separator = opts.Separator
		cfg.Driver = driver
		runtime.setupFn = driver.Setup
		runtime.TableName = driver.TableName
		return runtime, nil

	case "mysql":
		dsn := buildMySQLDSN(opts)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		driver := triflestats.NewMySQLDriver(db, opts.Table, joined)
		driver.Separator = opts.Separator
		cfg.Driver = driver
		runtime.setupFn = driver.Setup
		runtime.TableName = driver.TableName
		return runtime, nil

	case "redis":
		client, err := buildRedisClient(opts)
		if err != nil {
			return nil, err
		}
		driver := triflestats.NewRedisDriver(client, strings.TrimSpace(opts.Prefix))
		driver.Separator = opts.Separator
		cfg.Driver = driver
		runtime.TableName = strings.TrimSpace(opts.Prefix)
		return runtime, nil

	case "mongo":
		client, databaseName, collectionName, err := buildMongoCollection(opts)
		if err != nil {
			return nil, err
		}
		collection := client.Database(databaseName).Collection(collectionName)
		driver := triflestats.NewMongoDriver(collection, joined)
		driver.Separator = opts.Separator
		cfg.Driver = driver
		runtime.setupFn = func() error {
			return driver.Setup(context.Background())
		}
		runtime.TableName = collectionName
		return runtime, nil

	default:
		return nil, fmt.Errorf("unsupported local driver: %s", driverName)
	}
}

func applyBufferOptions(cfg *triflestats.Config, opts *driverOptions, driverName string) {
	if cfg == nil || opts == nil {
		return
	}
	cfg.BufferDuration = opts.BufferDuration
	cfg.BufferSize = opts.BufferSize
	cfg.BufferAggregate = opts.BufferAggregate
	cfg.BufferAsync = opts.BufferAsync

	mode := strings.ToLower(strings.TrimSpace(opts.BufferMode))
	switch mode {
	case "always", "on", "enabled", "true", "yes":
		cfg.BufferEnabled = true
	case "never", "off", "disabled", "false", "no":
		cfg.BufferEnabled = false
	case "", "auto", "default":
		cfg.BufferEnabled = driverName == "sqlite" || driverName == "postgres" || driverName == "mysql"
	default:
		cfg.BufferEnabled = driverName == "sqlite" || driverName == "postgres" || driverName == "mysql"
	}

	allowed := normalizeStringList(strings.Split(strings.TrimSpace(opts.BufferDrivers), ","))
	if len(allowed) > 0 {
		matched := false
		for _, value := range allowed {
			if normalizeDriverName(value) == driverName {
				matched = true
				break
			}
		}
		cfg.BufferEnabled = cfg.BufferEnabled && matched
	}
}

func buildPostgresDSN(opts *driverOptions) string {
	if strings.TrimSpace(opts.DSN) != "" {
		return strings.TrimSpace(opts.DSN)
	}

	host := firstNonEmpty(opts.Host, "127.0.0.1")
	port := firstNonEmpty(opts.Port, "5432")
	user := firstNonEmpty(opts.User, "postgres")
	password := firstNonEmpty(opts.Password, "password")
	database := resolveDatabaseName(opts, "trifle_stats")

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host,
		port,
		url.PathEscape(database),
	)
}

func buildMySQLDSN(opts *driverOptions) string {
	if strings.TrimSpace(opts.DSN) != "" {
		return strings.TrimSpace(opts.DSN)
	}

	host := firstNonEmpty(opts.Host, "127.0.0.1")
	port := firstNonEmpty(opts.Port, "3306")
	user := firstNonEmpty(opts.User, "root")
	password := firstNonEmpty(opts.Password, "password")
	database := resolveDatabaseName(opts, "trifle_stats")

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=UTC",
		user,
		password,
		host,
		port,
		database,
	)
}

func buildRedisClient(opts *driverOptions) (*redis.Client, error) {
	dsn := strings.TrimSpace(opts.DSN)
	if dsn != "" {
		if strings.Contains(dsn, "://") {
			parsed, err := redis.ParseURL(dsn)
			if err != nil {
				return nil, err
			}
			return redis.NewClient(parsed), nil
		}
		return redis.NewClient(&redis.Options{Addr: dsn}), nil
	}

	addr := net.JoinHostPort(firstNonEmpty(opts.Host, "127.0.0.1"), firstNonEmpty(opts.Port, "6379"))
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: strings.TrimSpace(opts.User),
		Password: strings.TrimSpace(opts.Password),
		DB:       parseIntOrDefault(opts.Database, 0),
	}), nil
}

func buildMongoCollection(opts *driverOptions) (*mongo.Client, string, string, error) {
	uri := strings.TrimSpace(opts.DSN)
	if uri == "" {
		uri = firstNonEmpty(opts.Host, "mongodb://127.0.0.1:27017")
		if !strings.Contains(uri, "://") {
			uri = "mongodb://" + uri
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, "", "", err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, "", "", err
	}

	databaseName := resolveDatabaseName(opts, "trifle_stats")
	collectionName := firstNonEmpty(strings.TrimSpace(opts.Collection), strings.TrimSpace(opts.Table), "trifle_stats")
	return client, databaseName, collectionName, nil
}

func resolveDatabaseName(opts *driverOptions, fallback string) string {
	if opts == nil {
		return fallback
	}
	if strings.TrimSpace(opts.Database) != "" {
		return strings.TrimSpace(opts.Database)
	}
	if strings.TrimSpace(opts.DBPath) != "" && normalizeDriverName(opts.Driver) != "sqlite" {
		return strings.TrimSpace(opts.DBPath)
	}
	return fallback
}

func parseBoolOrDefault(value string, fallback bool) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(trimmed)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseDurationOrDefault(value string, fallback time.Duration) time.Duration {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(trimmed)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseIntOrDefault(value string, fallback int) int {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return fallback
	}
	return parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
