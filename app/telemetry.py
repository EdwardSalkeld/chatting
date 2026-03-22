"""Compatibility wrapper for handler telemetry helpers."""

import sys

from app.handler import telemetry as _impl

sys.modules[__name__] = _impl
