FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY app/ app/

ENTRYPOINT ["uv", "run", "python", "-m"]
CMD ["app.main_message_handler"]
