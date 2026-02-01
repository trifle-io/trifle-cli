# Trifle CLI

Query and push time-series metrics from your terminal. Works with [Trifle App](https://trifle.io/product-app) via API or directly against local databases (SQLite, Postgres, MySQL, Redis, MongoDB). Ships an MCP server mode for AI agent integration.

Part of the [Trifle](https://trifle.io) ecosystem.

![Trifle CLI](cli.png)

## Install

Download a release from [GitHub Releases](https://github.com/trifle-io/trifle-cli/releases) and place `trifle` on your PATH.

Or build locally (Go 1.24+):

```sh
go build -o trifle .
```

## Quick Usage

### Query metrics from Trifle App

```sh
trifle metrics get \
  --key event::signup \
  --from 2026-02-10T00:00:00Z \
  --to 2026-02-16T00:00:00Z \
  --granularity 1h
```

### Track metrics locally

```sh
# Set up a local SQLite database
trifle metrics setup --driver sqlite --db ./stats.db

# Push a metric
trifle metrics push --driver sqlite --db ./stats.db \
  --key event::signup --values '{"count":1}'

# Query it back
trifle metrics get --driver sqlite --db ./stats.db \
  --key event::signup \
  --from 2026-02-10T00:00:00Z --to 2026-02-16T00:00:00Z \
  --granularity 1h
```

### Use with any database

```sh
# PostgreSQL
trifle metrics setup --driver postgres --dsn "postgres://user:pass@localhost:5432/myapp"

# MySQL
trifle metrics setup --driver mysql --host 127.0.0.1 --port 3306 --user root --database myapp

# MongoDB
trifle metrics setup --driver mongo --dsn mongodb://127.0.0.1:27017 --database myapp

# Redis
trifle metrics get --driver redis --prefix trifle:metrics --key event::signup \
  --from 2026-02-10T00:00:00Z --to 2026-02-16T00:00:00Z --granularity 1h
```

## MCP Server Mode

Run Trifle CLI as an MCP server so AI agents (Claude, GPT, etc.) can query and track metrics:

```sh
trifle mcp serve
```

This enables AI agents to read your analytics, track their own token/cost usage, and generate insights — all through the standard MCP protocol.

## Buffering

Configure write buffering for high-throughput local tracking:

```sh
trifle metrics push \
  --driver postgres --dsn "postgres://localhost:5432/myapp" \
  --key event::signup --values '{"count":1}' \
  --buffer-mode auto \
  --buffer-duration 2s \
  --buffer-size 500 \
  --buffer-aggregate
```

## Documentation

Full reference at **[docs.trifle.io/trifle-cli](https://docs.trifle.io/trifle-cli)**

## Trifle Ecosystem

| Component | What it does |
|-----------|-------------|
| **[Trifle App](https://trifle.io/product-app)** | Dashboards, alerts, scheduled reports, AI-powered chat. Cloud or self-hosted. |
| **[Trifle::Stats (Ruby)](https://github.com/trifle-io/trifle-stats)** | Time-series metrics library for Ruby. |
| **[Trifle.Stats (Elixir)](https://github.com/trifle-io/trifle_stats)** | Time-series metrics library for Elixir. |
| **[Trifle Stats (Go)](https://github.com/trifle-io/trifle_stats_go)** | Time-series metrics library for Go. |

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle-cli.

## License

Available under the terms of the [MIT License](https://opensource.org/licenses/MIT).
