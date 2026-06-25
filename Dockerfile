FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies first so they cache independently of source changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project --frozen

COPY . .
RUN uv sync --no-dev --frozen

# Quack's default bind port; override the bind address in startup.sql if needed.
EXPOSE 9494

ENTRYPOINT ["uv", "run", "--no-dev", "scrooge"]
