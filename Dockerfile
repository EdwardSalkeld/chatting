FROM python:3.13-slim AS base

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm ca-certificates \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ app/

ENTRYPOINT ["uv", "run", "python", "-m"]
CMD ["app.main_message_handler"]

FROM base AS test

RUN uv sync --locked --no-install-project
COPY tests/ tests/
COPY configs/ configs/
