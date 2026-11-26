# Trifle CLI

Query and push time-series metrics from your terminal through [Trifle App](https://trifle.io/product/app) or directly against local databases (SQLite, Postgres, MySQL, Redis, MongoDB). Search execution traces and download attachments from Trifle App. Ships an MCP server mode for AI agent integration.

Part of the [Trifle](https://trifle.io) ecosystem.

![Trifle CLI](cli.png)

## Install

**Homebrew** (macOS/Linux):
```sh
brew install trifle-io/trifle/trifle
```

**Shell script** (macOS/Linux):
```sh
curl -sSL https://get.trifle.io | sh
```

**Go**:
```sh
go install github.com/trifle-io/trifle-cli@latest
```

**Build locally** (Go 1.24+):
```sh
go build -o trifle .
```

## Quick Usage

### Bootstrap Trifle App access

```sh
# Login (stores one global organization token in config)
trifle auth login --url https://app.trifle.io --email user@example.com --password '<password>'

# Create sources; the login token is global and is granted access to each new source
trifle source create project --name "Agent Project"
trifle source list

# Select the source on every source-bound API request
trifle metrics get --source-id <PROJECT_ID> --key event::signup --granularity 1h

# Optional: mint and save a token that can read across all sources
trifle token create --wildcard-read --save

# Optional: upload a local SQLite file when creating a database source
trifle source create database --display-name "SQLite Upload" --driver sqlite --sqlite-file ./metrics.sqlite
```

### Query metrics from Trifle App

```sh
trifle metrics get \
  --source-id <PROJECT_ID> \
  --key event::signup \
  --from 2026-02-10T00:00:00Z \
  --to 2026-02-16T00:00:00Z \
  --granularity 1h
```

### Read traces from Trifle App

Use an active database source configured for Traces and an organization API token with read access to that source. With a management token, `trifle source list` shows database sources with `capabilities.traces` set to `true`.

```sh
export TRIFLE_URL=https://app.trifle.io
export TRIFLE_TOKEN='<ORG_TOKEN>'
export TRIFLE_SOURCE_ID='<DATABASE_SOURCE_ID>'

trifle traces search --path jobs/calculation --tag customer:42 --tag scheduled --tag-mode all --limit 10
trifle traces get --reference calc-42
trifle traces entries --reference calc-42 --part 1 --row 0 --limit 25
trifle traces attachments --reference calc-42
trifle traces download --reference calc-42 --attachment-id AAAAAAAAAAEAAAAAAAAAAg --output ./result.txt
```

Search returns one JSON page in newest-first order. Use `--from` for an inclusive start bound and `--to` for an exclusive end bound. Omit both to search the full trace index. Pass `pagination.next_cursor` as `--cursor` with the same source and filters to read the next page.

Entry pages continue at `pagination.next.part` and `pagination.next.row`, which you pass as `--part` and `--row`. Attachment pages continue at `pagination.next_after_part`, which you pass as `--after-part`. Continue through empty pages when a continuation value is present. A `null` continuation marks the end.

Use the attachment `id` from an entry or the attachment list to download the file. Choose a new path with `--output` or use `--output -` to stream bytes to stdout. Increase `--timeout` for downloads that need more than the default 30 seconds.

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
TRIFLE_URL=https://app.trifle.io \
TRIFLE_TOKEN=<ORG_TOKEN> \
TRIFLE_SOURCE_ID=<SOURCE_ID> \
trifle mcp
```

This enables AI agents to read your analytics, track their own token/cost usage, and generate insights through the standard MCP protocol.

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
| **[Trifle App](https://trifle.io/product/app)** | Dashboards, alerts, scheduled reports, AI-powered chat. Cloud or self-hosted. |
| **[Trifle::Stats (Ruby)](https://github.com/trifle-io/trifle-stats)** | Time-series metrics library for Ruby. |
| **[Trifle.Stats (Elixir)](https://github.com/trifle-io/trifle_stats)** | Time-series metrics library for Elixir. |
| **[Trifle Stats (Go)](https://github.com/trifle-io/trifle_stats_go)** | Time-series metrics library for Go. |

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle-cli.

## License

Available under the [Elastic License 2.0](https://www.elastic.co/licensing/elastic-license). See [LICENSE](LICENSE) for details.

## Testing unreleased escaped paths

The escaped-path changes are integrated against pushed Git commit `6ead1ebac6423758abd94b4b56ee423833bc34e4`, including the path-discovery correction. `go.mod` pins Go's resolved pseudo-version `v2.7.1-0.20260912090545-6ead1ebac642`; no package release is required.

Run `GOWORK=off go test ./...` to verify the fetched dependency without a local override. For future coordinated local changes, create a temporary Go workspace containing this repository and `../trifle_stats_go`, then run `GOWORK=/absolute/path/to/go.work go test ./...`. Always repeat verification with `GOWORK=off` after updating to an actual pushed commit or release.

Use `--value-path 'jobs.test\.rb'` for a dotted field and `'jobs.\*'` for a literal star on commands supporting value paths. JSON payloads require two backslashes, for example `--values '{"jobs.test\\.rb":1}'`. See [Value paths](https://docs.trifle.io/trifle-stats-go/guides/paths).
