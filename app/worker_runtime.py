"""Compatibility wrapper for worker runtime helpers."""

import sys

from app.worker import runtime as _impl

sys.modules[__name__] = _impl
