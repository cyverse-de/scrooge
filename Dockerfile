FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# git is required to fetch the ducktape dependency (a git source) during uv sync.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first so they cache independently of source changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project --frozen

COPY . .
RUN uv sync --no-dev --frozen

# Quack's default bind port (override the bind address in startup.sql if needed) and the
# HTTP log-ingest port (enabled by SCROOGE_INGEST_TOKEN; SCROOGE_INGEST_PORT to change).
EXPOSE 9494 9595

ENTRYPOINT ["uv", "run", "--no-dev", "scrooge"]
