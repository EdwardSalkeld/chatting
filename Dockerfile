FROM python:3.13-slim AS base

ARG GO_VERSION=1.26.1
ARG GO_ARCHIVE_SHA256=031f088e5d955bab8657ede27ad4e3bc5b7c1ba281f05f245bcc304f327c987a
ARG GO_ARCHIVE=go${GO_VERSION}.linux-amd64.tar.gz

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm ca-certificates curl gh git gosu ripgrep \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Debian login shells reset PATH, so link Go tools into /usr/local/bin as well.
RUN set -eux; \
    archive_url="https://go.dev/dl/${GO_ARCHIVE}"; \
    archive_path="/tmp/${GO_ARCHIVE}"; \
    curl -fsSL "${archive_url}" -o "${archive_path}"; \
    printf '%s  %s\n' "${GO_ARCHIVE_SHA256}" "${archive_path}" | sha256sum -c -; \
    rm -rf /usr/local/go; \
    tar -C /usr/local -xzf "${archive_path}"; \
    rm -f "${archive_path}"; \
    ln -sf /usr/local/go/bin/* /usr/local/bin/

ENV PATH=/usr/local/go/bin:${PATH}

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ app/

ENV HOME=/home/chatting
RUN mkdir -p /home/chatting && chmod 755 /home/chatting

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/app/.venv/bin/python", "-m", "app.main_message_handler"]

FROM base AS prod
RUN uv sync --locked
RUN chmod -R a+rX /app
RUN chmod -R a+rX /home/chatting

FROM base AS test

RUN uv sync --locked --no-install-project
COPY tests/ tests/
COPY configs/ configs/
