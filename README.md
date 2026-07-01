# scrooge

A log aggregator built on [DuckDB](https://duckdb.org). It collects logs from across a
cluster into a single DuckDB database, serves that database to other DuckDB instances over
the [Quack protocol](https://duckdb.org/docs/current/quack/overview), and rolls older
logs out to [Parquet](https://parquet.apache.org/) in [iRODS](https://irods.org) so the
live table stays bounded.

> **Status: proof of concept — not production-ready.** scrooge is an early prototype under
> active development. It has been exercised end-to-end against a live iRODS deployment and
> real Fluent Bit and Quack clients, but it has not been hardened, performance-tuned, or
> stabilized for production use; configuration and behavior may change without notice. Use
> it for evaluation and experimentation only.

## What it does

scrooge is a thin Python supervisor around an embedded DuckDB. The Python process only
handles lifecycle and the one bit of data logic SQL can't express (retention); everything
else lives in SQL.

Logs reach scrooge two ways, both landing in the same `logs` table:

- **[daffy](https://github.com/cyverse-de/daffy)** instances ship logs over the Quack
  protocol (port `9494`).
- **[Fluent Bit](https://fluentbit.io)** posts logs to an HTTP endpoint (port `9595`).

A unified `all_logs` view spans the live table plus the Parquet archive, so queries see the
full history regardless of what has been rolled off.

On startup it:

1. Resolves configuration (CLI flags, with environment-variable fallbacks).
2. Opens the database (DuckDB creates the file if missing), running the schema SQL once if
   the database is fresh.
3. Runs the boot script (`startup.sql`) — installs Quack and calls `quack_serve`.
4. Starts the HTTP server: probes (`/healthz`, `/readyz`) and Prometheus `/metrics`
   always; the log-ingest route only if `SCROOGE_INGEST_TOKEN` is set.
5. Reconciles any export interrupted by a previous crash (see
   [Archival design](#archival-design)), then refreshes the `all_logs` view.
6. Sweeps retention every `SCROOGE_SWEEP_INTERVAL` seconds until `SIGINT`/`SIGTERM`, and
   finally checkpoints and shuts down cleanly.

## Quick start

```bash
QUACK_TOKEN=super_secret \
SCROOGE_INGEST_TOKEN=ingest_secret \
uv run scrooge \
    --database data/scrooge.duckdb \
    --schema-sql schema.sql \
    --storage-dir irods:///zone/home/user/scrooge
```

- Omit `--storage-dir` to run without archival (the live `logs` table grows unbounded).
- Omit `SCROOGE_INGEST_TOKEN` to run without the HTTP log-ingest route (the probes and
  `/metrics` are still served).

Both `QUACK_TOKEN` and `SCROOGE_INGEST_TOKEN` are **env-only** (never CLI flags) so the
secrets aren't visible in `ps`, and must be at least 4 characters.

## Configuration

Flags take precedence over environment variables.

### Core

| Setting       | Flag           | Environment variable | Default       |
| ------------- | -------------- | -------------------- | ------------- |
| Database file | `--database`   | `DUCKDB_DATABASE`    | _(required)_  |
| Schema SQL    | `--schema-sql` | `DUCKDB_SCHEMA_SQL`  | `schema.sql`  |
| Boot SQL      | `--boot-sql`   | `DUCKDB_BOOT_SQL`    | `startup.sql` |
| Quack token   | _(env-only)_   | `QUACK_TOKEN`        | _(required)_  |

The schema SQL runs **only** when the database is created fresh. It defaults to
`schema.sql` (which defines the `logs` table). A missing **default** `schema.sql` is
tolerated (the server starts without a `logs` table); an explicitly configured schema that
is missing is a hard error. Set the value to an empty string to disable schema execution.

### Retention / archival

| Setting        | Flag               | Environment variable     | Default       |
| -------------- | ------------------ | ------------------------ | ------------- |
| Archive root   | `--storage-dir`    | `SCROOGE_STORAGE_DIR`    | _(none → off)_ |
| Retention rows | `--retention-rows` | `SCROOGE_RETENTION_ROWS` | `100000`      |
| Sweep interval | `--sweep-interval` | `SCROOGE_SWEEP_INTERVAL` | `10.0`        |

`SCROOGE_STORAGE_DIR` must be an `irods://` URL or a bare iRODS path; `SCROOGE_RETENTION_ROWS`
and `SCROOGE_SWEEP_INTERVAL` must be positive.

### HTTP server (ingest, probes, metrics)

| Setting             | Flag                          | Environment variable               | Default                  |
| ------------------- | ----------------------------- | ---------------------------------- | ------------------------ |
| Ingest token        | _(env-only)_                  | `SCROOGE_INGEST_TOKEN`             | _(none → ingest off)_    |
| Bind host           | `--ingest-host`               | `SCROOGE_INGEST_HOST`              | `0.0.0.0`                |
| Bind port           | `--ingest-port`               | `SCROOGE_INGEST_PORT`              | `9595`                   |
| Path                | `--ingest-path`               | `SCROOGE_INGEST_PATH`              | `/logs`                  |
| Service label key   | `--ingest-service-label-key`  | `SCROOGE_INGEST_SERVICE_LABEL_KEY` | `app.kubernetes.io/name` |
| Access logs         | _(env-only)_                  | `SCROOGE_INGEST_ACCESS_LOG`        | `false`                  |

Leave `SCROOGE_INGEST_ACCESS_LOG` off in-cluster: scrooge's own access-log lines get
scraped by Fluent Bit and posted back to scrooge, so enabling it risks a log feedback
loop. It exists for debugging outside the log-collection path.

### iRODS

Credentials are resolved from the environment. With `IRODS_HOST` set, scrooge connects
explicitly; otherwise [ducktape](https://github.com/cyverse-de/ducktape) falls back to the
standard iRODS environment file (`~/.irods/irods_environment.json` / `.irodsA`).

| Setting        | Environment variable | Default                  |
| -------------- | -------------------- | ------------------------ |
| iRODS host     | `IRODS_HOST`         | _(use env file instead)_ |
| iRODS port     | `IRODS_PORT`         | `1247`                   |
| iRODS user     | `IRODS_USER`         | _(from env file)_        |
| iRODS zone     | `IRODS_ZONE`         | _(from env file)_        |
| iRODS password | `IRODS_PASSWORD`     | _(from `.irodsA`)_       |

Like the tokens, `IRODS_PASSWORD` is env-only so the secret isn't exposed via process
arguments.

## The `logs` table

All ingestion paths land in one table, defined in `schema.sql`:

| Column         | Type        | Notes                        |
| -------------- | ----------- | ---------------------------- |
| `capture_time` | `TIMESTAMP` | when the line was captured (UTC) |
| `service`      | `VARCHAR`   | originating service          |
| `pod`          | `VARCHAR`   | Kubernetes pod (nullable)    |
| `node`         | `VARCHAR`   | Kubernetes node (nullable)   |
| `stream`       | `VARCHAR`   | `stdout`/`stderr`            |
| `level`        | `VARCHAR`   | log level (defaults to `''`) |
| `message`      | `VARCHAR`   | the log line                 |
| `fields`       | `JSON`      | structured fields (nullable) |

`all_logs` is a view over the live table `UNION ALL` the Parquet archive.

## Reading the logs

From a DuckDB 1.5.3+ client over Quack:

```sql
CREATE SECRET quack_remote (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:HOST:9494' AS remote;
SELECT count(*) FROM remote.logs;
```

(The `all_logs` view, which also spans the Parquet archive, is queried on the server; remote
clients see the live `remote.logs` table.)

## Sending logs from Fluent Bit

Set `SCROOGE_INGEST_TOKEN` to enable the ingest route. The HTTP server exposes:

- `POST <path>` (default `/logs`) — requires `Authorization: Bearer <SCROOGE_INGEST_TOKEN>`.
  The body may be a JSON array (Fluent Bit `format json`) or newline-delimited JSON
  (`format json_lines`). Returns `204` on success, `401` on a bad/absent token, `400` on an
  unparseable body, `500` on a DB error (Fluent Bit retries non-2xx). Absent without a token.
- `GET /healthz` — unauthenticated `200 ok`: process liveness only, never probes the DB
  (a transient DB hiccup must not cause a restart loop).
- `GET /readyz` — `200` when the DB connection can execute a query, else `503`: wire this
  to the readiness probe so a pod with an unusable database is depooled and Fluent Bit
  fails over to buffering/retry. It deliberately does **not** check iRODS — an unreachable
  archive must not depool ingest (sweeps already tolerate it and retry).
- `GET /metrics` — Prometheus metrics (see [Metrics](#metrics)).

Each record maps onto `logs` as follows; the **entire original record** is preserved in
`fields`, so nothing is lost:

| Column         | Source in the Fluent Bit record |
| -------------- | ------------------------------- |
| `capture_time` | the timestamp field (`date`); epoch or ISO-8601, else receive time |
| `service`      | `kubernetes.labels["app.kubernetes.io/name"]`, else `kubernetes.container_name`, else `"unknown"` |
| `pod`          | `kubernetes.pod_name`           |
| `node`         | `kubernetes.host`               |
| `stream`       | `stream` (`stdout`/`stderr`)    |
| `level`        | `level` (default `""`)          |
| `message`      | `log` (trailing newline stripped) |
| `fields`       | the whole record                |

The `service` label key is configurable (`SCROOGE_INGEST_SERVICE_LABEL_KEY`); the default
`app.kubernetes.io/name` requires that workloads set that
[recommended label](https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/)
and that the Fluent Bit kubernetes filter runs with `Labels On`.

```ini
[FILTER]
    Name             kubernetes
    Match            kube.*
    Labels           On

[OUTPUT]
    Name             http
    Match            kube.*
    Host             scrooge
    Port             9595
    URI              /logs
    Format           json
    Header           Authorization Bearer ${SCROOGE_INGEST_TOKEN}
```

## Archival design

**Sweep loop.** After the servers start, the main thread runs a retention *sweep* every
`SCROOGE_SWEEP_INTERVAL` seconds until signalled, then runs one final sweep before
shutdown. A sweep is best-effort: any failure (archive unreachable, a DuckDB error) is
logged and swallowed so it can never take down the live ingestion endpoints — the next
interval retries.

**Rolling.** Each sweep finds every service whose live row count exceeds
`SCROOGE_RETENTION_ROWS` and exports its **oldest day first** to Parquet, deleting those
rows, until the service is back under the threshold. The most recent day is kept live where
possible. Files are written as
`<storage-dir>/<service>/<YYYY-MM-DD>-<NNN>_<service>.parquet`, where `<NNN>` is a sequence
that increments when a day is archived across more than one sweep (computed by listing the
directory, so glob metacharacters in a service name can't reset it).

**Ordering.** Export is *archive-then-delete*: rows are copied to Parquet first, then
deleted. This favors never losing rows; if the copy or delete fails, the just-written file
is removed so the still-live rows aren't re-archived into a second file.

**Crash safety.** The Parquet write can't join a DuckDB transaction, so each export keeps
a journal: an intent marker is committed to a small `pending_exports` table before the
copy, and the row delete plus marker removal then commit in one transaction. A marker that
survives a hard kill names a file whose rows are still live; on startup (and before each
sweep) scrooge removes that orphaned file and clears the marker, so rows are never lost
*and* never duplicated in `all_logs`. If the orphan can't be removed (archive unreachable),
the marker survives and sweeps pause rather than double-archive. `pending_exports` is
visible to Quack clients and is normally empty.

**Concurrency.** Quack ingestion, HTTP ingestion, and the sweep each run on their own
DuckDB connection (sharing the same in-process database instance), so no single connection
is used by two threads; DuckDB's MVCC arbitrates between them.

## Metrics

`GET /metrics` on the HTTP port serves Prometheus metrics from `prometheus_client`; point
a scrape config or ServiceMonitor at it. The `service` label is bounded by the set of
Kubernetes service names (plus `unknown`).

| Metric                          | Type      | Labels    | Meaning                                          |
| ------------------------------- | --------- | --------- | ------------------------------------------------ |
| `scrooge_ingest_rows_total`     | counter   | `service` | rows inserted via HTTP ingest                    |
| `scrooge_ingest_requests_total` | counter   | `outcome` | requests by `ok`/`empty`/`unauthorized`/`bad_payload`/`error` |
| `scrooge_sweep_duration_seconds`| histogram | —         | duration of successful sweeps (incl. view refresh) |
| `scrooge_sweep_failures_total`  | counter   | —         | sweeps that failed and will retry                |
| `scrooge_archive_files_total`   | counter   | `service` | Parquet files archived                           |
| `scrooge_archive_bytes_total`   | counter   | `service` | bytes archived (best-effort)                     |
| `scrooge_live_rows`             | gauge     | `service` | rows currently in the live `logs` table          |

Useful alerts: `scrooge_sweep_failures_total` increasing (archive unreachable),
`scrooge_live_rows` far above `SCROOGE_RETENTION_ROWS` (sweeps not keeping up), and
`rate(scrooge_ingest_rows_total[5m]) == 0` (ingest stalled).

## Deployment security

Both listeners — Quack on `9494` and HTTP on `9595` — are **plaintext by design**;
encryption and access control are delegated to cluster networking. Deployments **must**
restrict both ports to cluster-internal traffic (a `NetworkPolicy`, and/or a service mesh
providing mTLS). This is a hard requirement, not a hardening suggestion:

- `/healthz`, `/readyz`, and `/metrics` are unauthenticated (Kubernetes probes and
  Prometheus scrapes don't carry bearer tokens).
- The Quack and ingest tokens ride in cleartext on their connections.

`SCROOGE_INGEST_HOST` defaults to `0.0.0.0` deliberately — the process runs in a container
and the pod boundary is the unit of exposure. Set it to a specific interface when running
outside a container.

## iRODS access (`irods://`)

scrooge registers the [ducktape](https://github.com/cyverse-de/ducktape) fsspec backend on
the DuckDB connection, so SQL can read and write iRODS data objects via `irods://` paths
(e.g. `read_parquet('irods:///zone/home/user/data.parquet')`). Registration is lazy — no
iRODS connection is opened until an `irods://` path is actually used — so archival to iRODS
"just works" once `SCROOGE_STORAGE_DIR` points at one.

## Why Python

Quack is a DuckDB core extension as of v1.5.3 (beta; stable targeted for v2.0.0). The Go
driver (`duckdb/duckdb-go`) currently bundles libduckdb v1.4.1, which predates Quack, so it
cannot load the extension. Python's `duckdb` package tracks releases and supports 1.5.3+
today.

## Docker

```bash
docker build -t scrooge .
docker run --network host \
    -e QUACK_TOKEN=super_secret \
    -e SCROOGE_INGEST_TOKEN=ingest_secret \
    -e DUCKDB_DATABASE=/data/scrooge.duckdb \
    -v "$PWD/data:/data" \
    scrooge
```

The image is a multi-stage build (≈250 MB); `schema.sql` and `startup.sql` ship in it, so
the defaults work out of the box.

## Development

```bash
uv run pytest
uv run ruff check
uv run ruff format --check
uv run pyright
```

### Upgrading ducktape

The [ducktape](https://github.com/cyverse-de/ducktape) dependency is pinned to a release
tag in `pyproject.toml` (`[tool.uv.sources]`), so builds don't move when ducktape's `main`
does. To upgrade: edit the `tag = "vX.Y.Z"` value, run `uv lock`, and commit both
`pyproject.toml` and `uv.lock`.
