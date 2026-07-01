"""Prometheus metrics, shared process-wide via the default registry.

The sweep loop (main thread), retention exports, and the ingest app (uvicorn thread)
all import these module-level objects; prometheus_client is thread-safe and scrooge is
a single process, so no multiprocess mode is needed. Label cardinality stays bounded:
`service` is the set of Kubernetes service names (plus "unknown") and `outcome` is a
closed enum — never add per-pod, per-path, or per-status-code labels.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

INGEST_ROWS = Counter(
    "scrooge_ingest_rows_total",
    "Log rows successfully inserted via HTTP ingest",
    ["service"],
)

# outcome: ok | empty | unauthorized | bad_payload | error
INGEST_REQUESTS = Counter(
    "scrooge_ingest_requests_total",
    "HTTP ingest requests by outcome",
    ["outcome"],
)

SWEEP_DURATION = Histogram(
    "scrooge_sweep_duration_seconds",
    "Duration of successful retention sweeps, including the view refresh",
    buckets=(0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 120.0, 300.0),
)

SWEEP_FAILURES = Counter(
    "scrooge_sweep_failures_total",
    "Retention sweeps that failed and will be retried next interval",
)

ARCHIVE_FILES = Counter(
    "scrooge_archive_files_total",
    "Parquet files archived, per service",
    ["service"],
)

ARCHIVE_BYTES = Counter(
    "scrooge_archive_bytes_total",
    "Bytes archived to Parquet, per service (best-effort, from fs.size)",
    ["service"],
)

LIVE_ROWS = Gauge(
    "scrooge_live_rows",
    "Rows currently in the live logs table, per service",
    ["service"],
)
