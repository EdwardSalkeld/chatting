FROM python:3.13-slim AS base

WORKDIR /app

ARG APP_UID=10001
ARG APP_GID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm ca-certificates \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && groupadd --gid "${APP_GID}" chatting \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --home-dir /home/chatting --shell /usr/sbin/nologin chatting \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ app/
RUN mkdir -p /data /workspace /var/lib/chatting/telegram-attachments /home/chatting/.codex /home/chatting/.claude \
    && chown -R chatting:chatting /data /workspace /var/lib/chatting /home/chatting \
    && chmod -R a-w /app

ENV HOME=/home/chatting
USER chatting:chatting

ENTRYPOINT ["uv", "run", "python", "-m"]
CMD ["app.main_message_handler"]

FROM base AS test

RUN uv sync --locked --no-install-project
COPY tests/ tests/
COPY configs/ configs/
