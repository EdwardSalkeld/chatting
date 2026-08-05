"""Executor wrapper for opt-in Telegram reply recovery."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from app.models import ExecutionResult, PromptContext, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.executor.base import Executor

_SUPERVISED_RECOVERY_INSTRUCTION = (
    "The earlier executor pass finished without publishing any visible reply. "
    "Do not redo side effects or rerun the task. Use the captured transcript below "
    "to send exactly one visible reply with python3 -P -m app.main_reply, then stop."
)


@dataclass
class SupervisedReplyRecoveryExecutor:
    """Wrap another executor and retry once for missing visible replies."""

    inner: Executor
    store: SQLiteStateStore
    last_recovery_attempted: bool = field(init=False, default=False)
    last_launch_count: int = field(init=False, default=0)

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        self.last_recovery_attempted = False
        self.last_launch_count = 1
        task_id = f"task:{envelope.id}"
        before_count = self.store.count_task_main_reply_egress_events(task_id=task_id)
        first_result = self.inner.execute(envelope)
        after_first_count = self.store.count_task_main_reply_egress_events(
            task_id=task_id
        )
        if first_result.errors or after_first_count > before_count:
            return first_result

        self.last_recovery_attempted = True
        self.last_launch_count = 2
        recovery_envelope = _build_supervised_recovery_envelope(
            original_envelope=envelope,
            execution_result=first_result,
        )
        second_result = self.inner.execute(recovery_envelope)
        return ExecutionResult(
            errors=list(second_result.errors),
            stdout=_merge_stream(
                first_result.stdout, second_result.stdout, label="stdout"
            ),
            stderr=_merge_stream(
                first_result.stderr, second_result.stderr, label="stderr"
            ),
        )


def _build_supervised_recovery_envelope(
    *,
    original_envelope: TaskEnvelope,
    execution_result: ExecutionResult,
) -> TaskEnvelope:
    transcript_parts = []
    if execution_result.stdout:
        transcript_parts.append("Captured stdout:\n" + execution_result.stdout.strip())
    if execution_result.stderr:
        transcript_parts.append("Captured stderr:\n" + execution_result.stderr.strip())
    transcript = "\n\n".join(part for part in transcript_parts if part.strip())
    recovery_instruction = _SUPERVISED_RECOVERY_INSTRUCTION
    if transcript:
        recovery_instruction = recovery_instruction + "\n\n" + transcript
    prompt_context = original_envelope.prompt_context
    return replace(
        original_envelope,
        prompt_context=PromptContext(
            global_instructions=list(prompt_context.global_instructions),
            source_instructions=list(prompt_context.source_instructions),
            reply_channel_instructions=list(prompt_context.reply_channel_instructions),
            task_instructions=list(prompt_context.task_instructions)
            + [recovery_instruction],
        ),
    )


def _merge_stream(
    first: str | None,
    second: str | None,
    *,
    label: str,
) -> str | None:
    parts = []
    if first:
        parts.append(f"First pass {label}:\n{first.strip()}")
    if second:
        parts.append(f"Recovery pass {label}:\n{second.strip()}")
    if not parts:
        return None
    return "\n\n".join(parts)


__all__ = ["SupervisedReplyRecoveryExecutor"]
