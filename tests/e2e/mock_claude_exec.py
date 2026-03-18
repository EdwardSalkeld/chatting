#!/usr/bin/env python3
"""Wrapper that patches the Anthropic client with a mock, then runs claude_exec."""
import os
import sys
import types
from unittest.mock import MagicMock

# Ensure the repo root is on sys.path so `app` is importable
sys.path.insert(0, os.getcwd())

# Build a fake anthropic module before claude_exec imports it
fake_anthropic = types.ModuleType("anthropic")

mock_response = MagicMock()
mock_response.content = [MagicMock(text="Mock Claude reply: the answer is 4.")]

mock_client_instance = MagicMock()
mock_client_instance.messages.create.return_value = mock_response

mock_client_class = MagicMock(return_value=mock_client_instance)
fake_anthropic.Anthropic = mock_client_class

sys.modules["anthropic"] = fake_anthropic

from app.executor.claude_exec import main  # noqa: E402

raise SystemExit(main())
