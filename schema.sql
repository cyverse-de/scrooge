-- Schema: runs once, when the database is created fresh. Defines the canonical `logs`
-- table that daffy instances ship into over the Quack protocol. The `all_logs` view
-- (which unions live rows with the Parquet archive) is managed at runtime by the
-- supervisor, since its definition depends on whether any archive files exist.
CREATE TABLE IF NOT EXISTS logs (
    capture_time TIMESTAMP NOT NULL,
    service      VARCHAR   NOT NULL,
    pod          VARCHAR,
    node         VARCHAR,
    stream       VARCHAR   NOT NULL,
    level        VARCHAR   NOT NULL DEFAULT '',
    message      VARCHAR   NOT NULL,
    fields       JSON
);
