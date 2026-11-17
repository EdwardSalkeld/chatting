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
  runs: RunCard[]
  active_executors: Executor[]
}
export interface RunHeader {
  run_id: string
  task_id: string | null
  work_item_id: string
  request: string
  status: string
  source: string
  started_at: string
  duration_ms: number
  attempt_count: number | null
  reason_codes: string[]
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
