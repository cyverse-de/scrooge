"""Command-line entrypoint for the scrooge Quack supervisor."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from scrooge.server import (
    ConfigError,
    resolve_config,
    resolve_ingest_token,
    resolve_token,
    run,
)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="scrooge",
        description="Boot a DuckDB database and serve it over the Quack protocol.",
    )
    parser.add_argument(
        "--database",
        help="path to the DuckDB file (env: DUCKDB_DATABASE)",
    )
    parser.add_argument(
        "--schema-sql",
        help="SQL run only when the database is created fresh (env: DUCKDB_SCHEMA_SQL)",
    )
    parser.add_argument(
        "--boot-sql",
        help="SQL run on every start; installs Quack and calls quack_serve "
        "(env: DUCKDB_BOOT_SQL, default: startup.sql)",
    )
    parser.add_argument(
        "--storage-dir",
        help="archive root URL (e.g. irods://...) for rolled-off Parquet logs; "
        "archival is disabled when unset (env: SCROOGE_STORAGE_DIR)",
    )
    parser.add_argument(
        "--retention-rows",
        type=int,
        help="per-service live-row threshold that triggers archival "
        "(env: SCROOGE_RETENTION_ROWS, default: 100000)",
    )
    parser.add_argument(
        "--sweep-interval",
        type=float,
        help="seconds between retention sweeps "
        "(env: SCROOGE_SWEEP_INTERVAL, default: 10.0)",
    )
    parser.add_argument(
        "--ingest-host",
        help="bind host for the HTTP log-ingest endpoint "
        "(env: SCROOGE_INGEST_HOST, default: 0.0.0.0). Enable the endpoint by setting "
        "SCROOGE_INGEST_TOKEN.",
    )
    parser.add_argument(
        "--ingest-port",
        type=int,
        help="bind port for the HTTP log-ingest endpoint "
        "(env: SCROOGE_INGEST_PORT, default: 9595)",
    )
    parser.add_argument(
        "--ingest-path",
        help="path for the HTTP log-ingest endpoint "
        "(env: SCROOGE_INGEST_PATH, default: /logs)",
    )
    parser.add_argument(
        "--ingest-service-label-key",
        help="pod label key used for the `service` column "
        "(env: SCROOGE_INGEST_SERVICE_LABEL_KEY, default: app.kubernetes.io/name)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    try:
        config = resolve_config(
            database=args.database,
            schema_sql=args.schema_sql,
            boot_sql=args.boot_sql,
            storage_dir=args.storage_dir,
            retention_rows=args.retention_rows,
            sweep_interval=args.sweep_interval,
            ingest_host=args.ingest_host,
            ingest_port=args.ingest_port,
            ingest_path=args.ingest_path,
            ingest_service_label_key=args.ingest_service_label_key,
            env=os.environ,
        )
        token = resolve_token(os.environ)
        ingest_token = resolve_ingest_token(os.environ)
        run(config, token=token, ingest_token=ingest_token, env=os.environ)
    except ConfigError as exc:
        logging.getLogger("scrooge").error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
