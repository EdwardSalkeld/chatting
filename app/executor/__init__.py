"""Executor package."""

from app.executor.base import Executor
from app.executor.stub import StubExecutor

__all__ = ["Executor", "StubExecutor"]
