FROM python:3.13-slim AS base

ARG GO_VERSION=1.26.1
ARG GO_ARCHIVE_SHA256=031f088e5d955bab8657ede27ad4e3bc5b7c1ba281f05f245bcc304f327c987a
ARG GO_ARCHIVE=go${GO_VERSION}.linux-amd64.tar.gz
ARG NIX_VERSION=2.34.0
ARG NIX_ARCHIVE_SHA256=5676b0887f1274e62edd175b6611af49aa8170c69c16877aa9bc6cebceb19855
ARG NIX_ARCHIVE=nix-${NIX_VERSION}-x86_64-linux.tar.xz

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bubblewrap nodejs npm ca-certificates curl gh git gosu ripgrep sqlite3 xz-utils \
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
ENV HOME=/home/chatting

RUN useradd -m -d /home/chatting -s /bin/sh chatting \
    && mkdir -p /nix \
    && chown chatting:chatting /nix

USER chatting

RUN set -eux; \
    archive_url="https://releases.nixos.org/nix/nix-${NIX_VERSION}/${NIX_ARCHIVE}"; \
    archive_path="/tmp/${NIX_ARCHIVE}"; \
    curl -fsSL "${archive_url}" -o "${archive_path}"; \
    printf '%s  %s\n' "${NIX_ARCHIVE_SHA256}" "${archive_path}" | sha256sum -c -; \
    tar -xJf "${archive_path}" -C /tmp; \
    rm -f "${archive_path}"; \
    cd "/tmp/nix-${NIX_VERSION}-x86_64-linux"; \
    ./install --no-daemon --yes --no-channel-add --no-modify-profile; \
    rm -rf "/tmp/nix-${NIX_VERSION}-x86_64-linux"

USER root

RUN mkdir -p /etc/nix \
    && printf '%s\n%s\n' \
      'experimental-features = nix-command flakes' \
      'accept-flake-config = true' \
      > /etc/nix/nix.conf \
    && ln -sf /home/chatting/.nix-profile/bin/* /usr/local/bin/

ENV PATH=/home/chatting/.nix-profile/bin:/usr/local/go/bin:${PATH}
ENV NIX_SSL_CERT_FILE=/home/chatting/.nix-profile/etc/ssl/certs/ca-bundle.crt

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ app/
COPY go/handler/ go/handler/

RUN cd go/handler \
    && go build -trimpath -o /usr/local/bin/chatting-handler ./cmd/chatting-handler

RUN chmod 755 /home/chatting

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["chatting-handler", "--config", "/config/handler.json"]

FROM base AS prod
RUN uv sync --locked
RUN chmod -R a+rX /app
RUN chmod -R a+rX /home/chatting

FROM base AS test

RUN uv sync --locked --no-install-project
COPY tests/ tests/
COPY configs/ configs/
