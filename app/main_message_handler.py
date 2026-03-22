"""Compatibility wrapper for the handler entrypoint."""

import sys

from app.handler import main as _impl

sys.modules[__name__] = _impl

if __name__ == "__main__":
    raise SystemExit(_impl.main())
