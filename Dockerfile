FROM python:3.13-slim AS base

ARG GO_VERSION=1.26.1
ARG GO_ARCHIVE_SHA256=031f088e5d955bab8657ede27ad4e3bc5b7c1ba281f05f245bcc304f327c987a
ARG GO_ARCHIVE=go${GO_VERSION}.linux-amd64.tar.gz

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm ca-certificates gh git gosu ripgrep \
    && npm install -g @openai/codex @anthropic-ai/claude-code \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN python - <<'PY'
import hashlib
import os
import shutil
import tarfile
import urllib.request

archive = os.environ["GO_ARCHIVE"]
expected_sha256 = os.environ["GO_ARCHIVE_SHA256"]
url = f"https://go.dev/dl/{archive}"
download_path = f"/tmp/{archive}"

with urllib.request.urlopen(url, timeout=60) as response, open(download_path, "wb") as destination:
    shutil.copyfileobj(response, destination)

digest = hashlib.sha256()
with open(download_path, "rb") as archive_file:
    for chunk in iter(lambda: archive_file.read(1024 * 1024), b""):
        digest.update(chunk)

if digest.hexdigest() != expected_sha256:
    raise SystemExit("Go archive checksum mismatch")

shutil.rmtree("/usr/local/go", ignore_errors=True)
with tarfile.open(download_path, "r:gz") as tarball:
    tarball.extractall("/usr/local")
os.remove(download_path)
PY

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
