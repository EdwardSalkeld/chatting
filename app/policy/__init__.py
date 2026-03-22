"""Compatibility package for worker policy implementations."""

from pathlib import Path

__path__.append(str(Path(__file__).resolve().parent.parent / "worker" / "policy"))

from app.policy.allowlist import AllowlistPolicyEngine
from app.policy.base import PolicyEngine

__all__ = ["AllowlistPolicyEngine", "PolicyEngine"]
