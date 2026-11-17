export interface ReplyChannel {
  type: string
  target: string
  metadata?: Record<string, unknown>
}
export interface WorkItem {
  work_item_id: string
  origin_conversation_id: string
  reply: ReplyChannel
  run_count: number
  queued: number
  model_tier: string
  last_run_at: string | null
  explicit_finish: boolean
  created_at?: string
  running?: number
  /** When the oldest queued task for this item was staged; null when nothing is queued. */
  oldest_queued_at?: string | null
}
export interface Executor {
  task_id: string
  work_item_id: string | null
  source: string
  attempt: number
  started_at: string
  preview?: string
  phase?: string
}
export interface ItemsSnapshot {
  items: WorkItem[]
  active_executors: Executor[]
}
export interface RunCard {
  run_id: string
  task_id: string
  created_at: string
  source: string
  latency_ms: number
  result_status: string
  preview: string
}
export interface ItemSnapshot {
  item: WorkItem
  runs: RunRow[]
  active_executors: Executor[]
}
export interface AutoModelDecision {
  tier: string
  reason?: string
  confidence?: number | string
  [key: string]: unknown
}
export interface RunHeader {
  run_id: string
  task_id: string | null
  work_item_id: string
  preview?: string
  request: string
  status: string
  source: string
  started_at: string
  finished_at?: string
  duration_ms: number
  attempt_count: number | null
  reason_codes: string[]
  executor_backend?: string | null
  model_tier?: string | null
  executed_model_tier?: string | null
  auto_model_decision?: AutoModelDecision | null
  escalation_reason?: string | null
  last_error?: string | null
  last_error_stage?: string | null
  execution_errors?: string[]
  egress_message_count?: number | null
  executor_launch_count?: number | null
  max_attempts?: number | null
  supervised_recovery_used?: boolean | null
}
export interface TokenTotals {
  input_tokens: number | null
  cached_input_tokens: number | null
  output_tokens: number | null
  reasoning_output_tokens: number | null
  total_tokens: number | null
}
export interface RunRow extends RunCard {
  work_item_id: string | null
  task_id: string
  executed_model_tier: string | null
  model: string | null
  tokens: TokenTotals | null
  attempt_count?: number | null
  reason_codes?: string[]
  last_error?: string | null
  last_error_stage?: string | null
  execution_errors?: string[]
}
export interface RunsQuery {
  limit?: number
  status?: string
  source?: string
  work_item_id?: string
  since?: string
}
export interface RunsSnapshot {
  runs: RunRow[]
  limit: number
  truncated: boolean
}
export interface DeadLetter {
  dead_letter_id: number
  run_id: string
  task_id: string | null
  work_item_id: string | null
  envelope_id: string | null
  source: string | null
  received_at: string | null
  reply_channel_type: string | null
  reply_channel_target: string | null
  reason_codes: string[]
  last_error: string | null
  attempt_count: number
  status: string
  replayed_run_id: string | null
  created_at: string
}
export interface DeadLettersSnapshot {
  dead_letters: DeadLetter[]
  limit: number
  truncated: boolean
}
export interface QueueSummary {
  queued: number
  running: number
  oldest_queued_at?: string | null
}
export interface OutboxSummary {
  pending: number
  oldest_created_at: string | null
}
export interface ActivitySnapshot {
  current_executor: Partial<Executor> & { active: boolean; phase?: string }
  active_executors: Executor[]
  queue: QueueSummary
  outbox?: OutboxSummary
  current_run: Record<string, unknown> | null
  recent_activity: (ActivityEvent & { source?: string | null; is_internal?: boolean })[]
  history_limit: number
  history_truncated: boolean
  include_internal: boolean
}
export interface ActivityEvent {
  activity_id: number
  phase: string
  summary: string
  occurred_at: string
  task_id?: string
  run_id?: string
  detail: Record<string, unknown>
}
export interface EventsSnapshot {
  events: ActivityEvent[]
  active: boolean
}
export interface Schedule {
  schedule_id: string
  version: number
  status: string
  job_name: string
  content: string
  cron: string
  timezone: string
  context_refs: string[]
  prompt_context: string[]
  reply_channel_type: string
  reply_channel_target: string
  created_at: string
  created_by: string
}
export interface Reminder {
  reminder_id: string
  revision: number
  status: string
  run_at: string
  prompt: string
  reply_channel: ReplyChannel
  context_refs: string[]
  prompt_context: string[]
  created_from_task_id: string
  created_by: string
  idempotency_key: string
  created_at: string
  updated_at: string
  fired_at: string | null
  cancelled_at: string | null
}
export interface InboxRow {
  /** Message id; older handlers omit it. */
  id?: string
  received: string
  received_at: string
  sender: string
  subject: string
  decision: string
  score: string
  why: string
  link: string
  unclassified: boolean
}
export interface InboxSnapshot {
  dry_run: boolean
  threshold: string
  counts: Record<string, number>
  awaiting: number
  stuck: number
  oldest_wait: string
  dead_letters: string
  last_error: string
  rows: InboxRow[]
}
/** A mailbox or Telegram effect whose outcome is unknown, e.g. interrupted mid-flight. */
export interface InboxEffect {
  id: string
  message_id: string
  kind: string
  category?: string
  target: string
  status: string
  receipt?: string
  note?: string
}
