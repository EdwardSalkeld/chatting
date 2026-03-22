"""Compatibility wrapper for handler GitHub ingress helpers."""

import sys

from app.handler import github_ingress as _impl

sys.modules[__name__] = _impl
