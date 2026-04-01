"""Broker queue constants for split process architecture."""

TASK_QUEUE_NAME = "chatting.tasks.v1"
EGRESS_QUEUE_NAME = "chatting.egress.v1"
AUXILIARY_INGRESS_QUEUE_NAME = "chatting.auxiliary-ingress.v1"

__all__ = ["TASK_QUEUE_NAME", "EGRESS_QUEUE_NAME", "AUXILIARY_INGRESS_QUEUE_NAME"]
