"""Compatibility wrapper for handler runtime helpers."""

import sys

from app.handler import runtime as _impl

sys.modules[__name__] = _impl
