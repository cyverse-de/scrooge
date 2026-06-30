# scrooge

A thin supervisor that boots a [DuckDB](https://duckdb.org) database and serves it
over the [Quack protocol](https://duckdb.org/docs/current/quack/overview) so other
DuckDB instances can connect over HTTP.

Most data logic lives in SQL. The Python process manages the database file lifecycle,
keeps the server alive, and (when an archive is configured) runs periodic log-retention
sweeps:

1. Resolve configuration (CLI flags, with environment-variable fallbacks).
2. Note whether the database file already exists.
3. Open the database (DuckDB creates the file if it is missing).
4. If the database was created fresh, run the schema SQL once (`schema.sql` by default).
5. Run the boot script (`startup.sql`) on every start — it installs Quack and calls
   `quack_serve`. The reported listen URI and auth token are logged.
6. Refresh the `all_logs` view, then sweep retention on each interval tick until
   `SIGINT`/`SIGTERM`, flush a final sweep, `CHECKPOINT`, and close cleanly.

## Why Python

Quack is a DuckDB core extension as of v1.5.3 (beta; stable targeted for v2.0.0). The
Go driver (`duckdb/duckdb-go`) currently bundles libduckdb v1.4.1, which predates
Quack, so it cannot load the extension. Python's `duckdb` package tracks releases and
supports 1.5.3+ today.

## Configuration

| Setting         | Flag               | Environment variable     | Default       |
| --------------- | ------------------ | ------------------------ | ------------- |
| Database file   | `--database`       | `DUCKDB_DATABASE`        | _(required)_  |
| Schema SQL      | `--schema-sql`     | `DUCKDB_SCHEMA_SQL`      | `schema.sql`  |
| Boot SQL        | `--boot-sql`       | `DUCKDB_BOOT_SQL`        | `startup.sql` |
| Quack token     | _(none)_           | `QUACK_TOKEN`            | _(required)_  |
| Archive root    | `--storage-dir`    | `SCROOGE_STORAGE_DIR`    | _(none)_      |
| Retention rows  | `--retention-rows` | `SCROOGE_RETENTION_ROWS` | `100000`      |
| Sweep interval  | `--sweep-interval` | `SCROOGE_SWEEP_INTERVAL` | `10.0`        |

Flags take precedence over environment variables. `QUACK_TOKEN` is intentionally
env-only — secrets passed as CLI flags are visible to anyone who can run `ps`. It is
**required** and must be at least 4 characters; scrooge fails fast otherwise rather
than start a server clients cannot authenticate against. scrooge injects it into the
boot script as the `quack_token` DuckDB variable (the embedded library has no `getenv`),
which `startup.sql` reads via `getvariable('quack_token')`.

The schema SQL file runs **only** when the database is created fresh. It defaults to
`schema.sql` (which defines the `logs` table; see below). Use it for one-time schema and
seed data — idempotent DDL (`CREATE TABLE IF NOT EXISTS`, ...) is harmless either way. A
missing **default** `schema.sql` is tolerated (the server starts without a `logs` table);
an explicitly configured schema that is missing is a hard error. Set the value to an empty
string to disable schema execution entirely.

`SCROOGE_STORAGE_DIR` must be an `irods://` URL or a bare iRODS path — archives are written
through the registered iRODS backend (see below). `SCROOGE_RETENTION_ROWS` and
`SCROOGE_SWEEP_INTERVAL` must be positive.

## Log aggregation & archival

scrooge collects logs shipped by [daffy](https://github.com/cyverse-de/daffy) instances
over Quack into a `logs` table (defined in `schema.sql`):

| Column         | Type        | Notes                          |
| -------------- | ----------- | ------------------------------ |
| `capture_time` | `TIMESTAMP` | when the line was captured     |
| `service`      | `VARCHAR`   | originating service            |
| `pod`          | `VARCHAR`   | Kubernetes pod (nullable)      |
| `node`         | `VARCHAR`   | Kubernetes node (nullable)     |
| `stream`       | `VARCHAR`   | `stdout`/`stderr`              |
| `level`        | `VARCHAR`   | log level (defaults to `''`)   |
| `message`      | `VARCHAR`   | the log line                   |
| `fields`       | `JSON`      | structured fields (nullable)   |

When `SCROOGE_STORAGE_DIR` is set, scrooge keeps the live table bounded: on each sweep,
any service whose live row count exceeds `SCROOGE_RETENTION_ROWS` has its oldest log-days
rolled out (oldest first) to per-service, per-day Parquet files
(`<storage-dir>/<service>/<YYYY-MM-DD>-<NNN>_<service>.parquet`) and deleted from the
live table. The archive root is a URL written through the registered filesystem — in
production an `irods://` path (see below). The `all_logs` view transparently unions the
live table with the Parquet archive, so queries see the full history regardless of what
has been rolled off. Archival is disabled when `SCROOGE_STORAGE_DIR` is unset; `all_logs`
is then just the live table.

### Archival design

**Sweep loop.** After the Quack server starts, the main thread runs a retention *sweep*
every `SCROOGE_SWEEP_INTERVAL` seconds until it receives `SIGINT`/`SIGTERM`, then runs one
final sweep before shutting down. A sweep is best-effort maintenance: any failure (archive
unreachable, a DuckDB error) is logged and swallowed so it can never take down the live
ingestion endpoint — the next interval simply retries.

**Rolling algorithm.** Each sweep finds every service whose live row count exceeds
`SCROOGE_RETENTION_ROWS` and, for each, exports its **oldest day first** to Parquet and
deletes those rows, repeating until the service is back under the threshold. The most
recent day is kept live where possible, so recent logs stay queryable without touching the
archive. Days, not arbitrary row batches, are the unit of eviction so each Parquet file
holds exactly one service-day.

**File layout.** Files are written as
`<storage-dir>/<service>/<YYYY-MM-DD>-<NNN>_<service>.parquet`, where `<NNN>` is a
zero-padded sequence that increments when a day is archived across more than one sweep
(e.g. late-arriving rows for an already-archived day). The next sequence number is computed
by **listing** the service directory and matching filenames — not by globbing — so glob
metacharacters in a service name (`*`, `?`, `[`) can't be misinterpreted and silently
overwrite an existing file.

**The `all_logs` view.** `all_logs` is a lazy DuckDB view defined as the live `logs` table
`UNION ALL` a `read_parquet()` over the archive glob (`union_by_name => true`). It is
(re)created at startup and after any sweep that wrote files, so queries always span live
plus archived history. When no archive is configured or none exists yet, the view is just
the live table. The view is a no-op if there is no `logs` table (e.g. a pre-existing
database, or a custom schema that doesn't define it), so startup never fails on it.

**Ordering and atomicity.** Export is *archive-then-delete*: a day's rows are copied to
Parquet first, then deleted from the live table. This ordering favors never losing rows. If
the `COPY` or `DELETE` raises, the just-written Parquet is removed so the still-live rows
aren't re-archived into a second file (which would double-count in `all_logs`). The one
residual risk is a hard kill (SIGKILL/OOM) *between* the `COPY` and the `DELETE`: the file
survives, the rows stay live, and the next sweep re-archives them into a new sequence file —
duplication in `all_logs`, never loss.

**Concurrency.** Sweeps run on a **dedicated DuckDB connection** (a `cursor()` of the
serving connection), separate from the one Quack appends on, so no single connection object
is ever used by two threads. The two connections share the same in-process database
instance and the registered iRODS filesystem; DuckDB's MVCC arbitrates between Quack's
appends and the sweep's `COPY`/`DELETE`. A sweep that loses a write–write race just raises
and is retried on the next interval.

### iRODS access (`irods://`)

scrooge registers the [ducktape](https://github.com/cyverse-de/ducktape) fsspec backend
on the DuckDB connection, so SQL can read and write iRODS data objects via `irods://`
paths (e.g. `read_parquet('irods:///zone/home/user/data.parquet')`). Registration is
lazy — no iRODS connection is opened until an `irods://` path is actually used.

Credentials are resolved from the environment. With `IRODS_HOST` set, scrooge connects
explicitly; otherwise ducktape falls back to the standard iRODS environment file
(`~/.irods/irods_environment.json` / `.irodsA`).

| Setting     | Environment variable | Default                  |
| ----------- | -------------------- | ------------------------ |
| iRODS host  | `IRODS_HOST`         | _(use env file instead)_ |
| iRODS port  | `IRODS_PORT`         | `1247`                   |
| iRODS user  | `IRODS_USER`         | _(from env file)_        |
| iRODS zone  | `IRODS_ZONE`         | _(from env file)_        |
| iRODS password | `IRODS_PASSWORD`  | _(from `.irodsA`)_       |

Like `QUACK_TOKEN`, `IRODS_PASSWORD` is env-only so the secret is not exposed via process
arguments.

## Running

```bash
QUACK_TOKEN=super_secret uv run scrooge \
    --database data/scrooge.duckdb \
    --schema-sql schema.sql \
    --storage-dir irods:///zone/home/user/scrooge
```

Omit `--storage-dir` to run without archival (the live `logs` table grows unbounded).

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
