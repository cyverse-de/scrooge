"""Command-line entrypoint for the scrooge Quack supervisor."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from scrooge.server import ConfigError, resolve_config, resolve_token, run


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
            env=os.environ,
        )
        token = resolve_token(os.environ)
        run(config, token=token)
    except ConfigError as exc:
        logging.getLogger("scrooge").error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
