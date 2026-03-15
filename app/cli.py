"""Preferred admin/query CLI entrypoint for split-mode deployment."""

from __future__ import annotations

from app.main import main


if __name__ == "__main__":
    raise SystemExit(main())
