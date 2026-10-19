"""Choose a backend once per execution using the durable conversation setting."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from app.models import ExecutionResult, TaskEnvelope, UsageReport
from app.state import SQLiteStateStore
from app.worker.executor.app_server import AppServerExecutor
from app.worker.executor.codex import CodexExecutor


@dataclass(frozen=True)
class ConversationExecutor:
    legacy: CodexExecutor
    persistent: AppServerExecutor
    store: SQLiteStateStore

    def select_backend(self, *, conversation_id: str, backend: str) -> CodexExecutor:
        if backend == "app-server":
            return self.persistent.for_conversation(conversation_id)
        return self.legacy

    def for_workspace(self, *, work_item_id: str) -> ConversationExecutor:
        return replace(
            self,
            legacy=self.legacy.for_workspace(work_item_id=work_item_id),
            persistent=self.persistent.for_workspace(work_item_id=work_item_id),
        )

    def for_model(self, tier: str) -> ConversationExecutor:
        return replace(
            self,
            legacy=self.legacy.for_model(tier),
            persistent=self.persistent.for_model(tier),
        )

    def with_output_callback(
        self, callback: Callable[[str, str], None]
    ) -> ConversationExecutor:
        return replace(
            self,
            legacy=self.legacy.with_output_callback(callback),
            persistent=self.persistent.with_output_callback(callback),
        )

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        task = self.store.get_inbox_task(task_id=f"task:{envelope.id}")
        if (
            task
            and self.store.get_conversation_executor(
                conversation_id=task.conversation_id
            )
            == "app-server"
        ):
            return self.select_backend(
                conversation_id=task.conversation_id, backend="app-server"
            ).execute(envelope)
        return self.legacy.execute(envelope)

    def usage_report(self) -> UsageReport:
        return self.legacy.usage_report()
