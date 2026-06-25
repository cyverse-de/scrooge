# scrooge

A thin supervisor that boots a [DuckDB](https://duckdb.org) database and serves it
over the [Quack protocol](https://duckdb.org/docs/current/quack/overview) so other
DuckDB instances can connect over HTTP.

All data logic lives in SQL. The Python process only manages the database file
lifecycle and keeps the server alive:

1. Resolve configuration (CLI flags, with environment-variable fallbacks).
2. Note whether the database file already exists.
3. Open the database (DuckDB creates the file if it is missing).
4. If the database was created fresh and a schema SQL file is configured, run it once.
5. Run the boot script (`startup.sql`) on every start — it installs Quack and calls
   `quack_serve`. The reported listen URI and auth token are logged.
6. Block until `SIGINT`/`SIGTERM`, then `CHECKPOINT` and close cleanly.

## Why Python

Quack is a DuckDB core extension as of v1.5.3 (beta; stable targeted for v2.0.0). The
Go driver (`duckdb/duckdb-go`) currently bundles libduckdb v1.4.1, which predates
Quack, so it cannot load the extension. Python's `duckdb` package tracks releases and
supports 1.5.3+ today.

## Configuration

| Setting        | Flag           | Environment variable | Default       |
| -------------- | -------------- | -------------------- | ------------- |
| Database file  | `--database`   | `DUCKDB_DATABASE`    | _(required)_  |
| Schema SQL     | `--schema-sql` | `DUCKDB_SCHEMA_SQL`  | _(none)_      |
| Boot SQL       | `--boot-sql`   | `DUCKDB_BOOT_SQL`    | `startup.sql` |
| Quack token    | _(none)_       | `QUACK_TOKEN`        | _(required)_  |

Flags take precedence over environment variables. `QUACK_TOKEN` is intentionally
env-only — secrets passed as CLI flags are visible to anyone who can run `ps`. It is
**required** and must be at least 4 characters; scrooge fails fast otherwise rather
than start a server clients cannot authenticate against. scrooge injects it into the
boot script as the `quack_token` DuckDB variable (the embedded library has no `getenv`),
which `startup.sql` reads via `getvariable('quack_token')`.

The `--schema-sql` file runs **only** when the database is created fresh. Use it for
one-time schema and seed data. If your setup is purely idempotent DDL
(`CREATE TABLE IF NOT EXISTS`, ...) it is harmless either way.

## Running

```bash
QUACK_TOKEN=super_secret uv run scrooge \
    --database data/scrooge.duckdb \
    --schema-sql schema.sql
```

## Connecting from another DuckDB instance

From a DuckDB 1.5.3+ client:

```sql
CREATE SECRET quack_remote (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:HOST:9494' AS remote;
SELECT * FROM remote.your_table;
```

## Docker

```bash
docker build -t scrooge .
docker run --network host \
    -e QUACK_TOKEN=super_secret \
    -e DUCKDB_DATABASE=/data/scrooge.duckdb \
    -v "$PWD/data:/data" \
    scrooge
```

## Development

```bash
uv run pytest
uv run ruff check
uv run ruff format --check
uv run pyright
```
