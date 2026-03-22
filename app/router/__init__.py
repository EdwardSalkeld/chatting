"""Compatibility package for worker routers."""

from pathlib import Path

__path__.append(str(Path(__file__).resolve().parent.parent / "worker" / "router"))

from app.router.base import Router
from app.router.rule_based import RuleBasedRouter

__all__ = ["Router", "RuleBasedRouter"]
