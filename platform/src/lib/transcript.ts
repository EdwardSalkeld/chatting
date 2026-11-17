import type { ActivityEvent } from './types'

export type EntryKind = 'assistant' | 'command' | 'system' | 'delivery' | 'error'
export const ENTRY_KINDS: { kind: EntryKind; label: string }[] = [
  { kind: 'assistant', label: 'Assistant' },
  { kind: 'command', label: 'Commands' },
  { kind: 'system', label: 'System' },
  { kind: 'delivery', label: 'Delivery' },
  { kind: 'error', label: 'Errors' },
]

export interface TranscriptEntry {
  key: string
  kind: EntryKind
  label: string
  body: string
  at: string
  tone?: 'ok' | 'warn' | 'danger'
  runID?: string
  command?: string
  output?: string
  exit?: number | null
  durationMs?: number
  running?: boolean
}
export interface TokenUsage {
  input: number
  cached: number
  output: number
  reasoning: number
  total: number
}
export interface SessionMeta {
  model?: string
  reroute?: { from?: string; to?: string; reason?: string; count: number }
  tokens?: TokenUsage
  contextWindow?: number
  contextTokens?: number
  attempts: number
  runID?: string
  finishedStatus?: string
}
export interface Transcript {
  entries: TranscriptEntry[]
  meta: SessionMeta
}

interface NativeItem {
  id?: string
  type?: string
  text?: string
  command?: string
  exit_code?: number | null
  exitCode?: number | null
  durationMs?: number
  aggregated_output?: string
  aggregatedOutput?: string
  changes?: { path: string }[]
}
interface NativeUsage {
  inputTokens?: number
  cachedInputTokens?: number
  outputTokens?: number
  reasoningOutputTokens?: number
  totalTokens?: number
  input_tokens?: number
  cached_input_tokens?: number
  output_tokens?: number
  reasoning_output_tokens?: number
  total_tokens?: number
}
interface NativeEvent {
  method?: string
  type?: string
  message?: string
  item?: NativeItem
  usage?: NativeUsage
  error?: { message?: string }
  params?: {
    threadId?: string
    item?: NativeItem
    turn?: { status?: string }
    error?: { message?: string }
    model?: string
    fromModel?: string
    toModel?: string
    reason?: string
    tokenUsage?: { last?: NativeUsage; total?: NativeUsage; modelContextWindow?: number }
  }
}

function usage(raw: NativeUsage | undefined): TokenUsage {
  const u = raw || {}
  const input = u.inputTokens ?? u.input_tokens ?? 0,
    output = u.outputTokens ?? u.output_tokens ?? 0
  return {
    input,
    cached: u.cachedInputTokens ?? u.cached_input_tokens ?? 0,
    output,
    reasoning: u.reasoningOutputTokens ?? u.reasoning_output_tokens ?? 0,
    total: u.totalTokens ?? u.total_tokens ?? input + output,
  }
}
function addUsage(a: TokenUsage | undefined, b: TokenUsage): TokenUsage {
  if (!a) return b
  return {
    input: a.input + b.input,
    cached: a.cached + b.cached,
    output: a.output + b.output,
    reasoning: a.reasoning + b.reasoning,
    total: a.total + b.total,
  }
}

const STDERR_ERROR = /error|panic|fatal|traceback|exception|killed|denied/i

// Rebuild from durable events so split stdout lines survive polling boundaries.
export function transcript(events: ActivityEvent[], finished: boolean): Transcript {
  const entries: TranscriptEntry[] = []
  const meta: SessionMeta = { attempts: 0 }
  const commands = new Map<string, TranscriptEntry>()
  // app-server totals are cumulative per thread; a retried attempt starts a new thread.
  const threadTotals = new Map<string, TokenUsage>()
  let execTokens: TokenUsage | undefined
  let buffer = '',
    bufferAt = '',
    bufferKey = ''
  function add(entry: Omit<TranscriptEntry, 'body'> & { body?: string }) {
    const full = { body: '', ...entry }
    entries.push(full)
    return full
  }
  function output(line: string, at: string, key: string) {
    if (!line.trim()) return
    let entry: NativeEvent
    try {
      entry = JSON.parse(line) as NativeEvent
    } catch {
      add({ key, kind: 'command', label: 'Output', body: line, at })
      return
    }
    if (!entry || typeof entry !== 'object') {
      add({ key, kind: 'command', label: 'Output', body: line, at })
      return
    }
    const name = entry.method || entry.type || ''
    const params = entry.params || {}
    const item = params.item || entry.item || {}
    const completed = name === 'item/completed' || name === 'item.completed'
    const started = name === 'item/started' || name === 'item.started'
    if (name === 'thread/tokenUsage/updated') {
      const info = params.tokenUsage || {}
      if (info.total) threadTotals.set(params.threadId || '', usage(info.total))
      if (info.modelContextWindow) meta.contextWindow = info.modelContextWindow
      if (info.last) meta.contextTokens = usage(info.last).total
      return
    }
    if (name === 'chatting/modelSelected') {
      if (params.model && params.model !== meta.model) {
        if (!meta.model)
          add({ key, kind: 'system', label: 'Model selected', body: params.model, at })
        meta.model = params.model
      }
      return
    }
    if (name === 'model/rerouted') {
      meta.reroute = {
        from: params.fromModel || meta.model,
        to: params.toModel,
        reason: params.reason,
        count: (meta.reroute?.count || 0) + 1,
      }
      meta.model = params.toModel || meta.model
      add({
        key,
        kind: 'system',
        tone: 'warn',
        label: 'Model rerouted',
        body: `${params.fromModel || '?'} → ${params.toModel || '?'}${params.reason ? ` · ${params.reason}` : ''}`,
        at,
      })
      return
    }
    if (name === 'turn.completed' && entry.usage) {
      execTokens = addUsage(execTokens, usage(entry.usage))
      return
    }
    if (completed && ['agentMessage', 'agent_message'].includes(item.type || '')) {
      add({ key, kind: 'assistant', label: 'Assistant', body: item.text || '', at })
      return
    }
    if (
      (started || completed) &&
      ['commandExecution', 'command_execution'].includes(item.type || '')
    ) {
      const id = item.id || item.command || key
      let row = commands.get(id)
      if (!row || !row.running) {
        row = add({
          key,
          kind: 'command',
          label: 'Command',
          at,
          command: item.command || '',
          running: true,
        })
        commands.set(id, row)
      }
      if (completed) {
        const exit = item.exitCode ?? item.exit_code
        row.running = false
        row.exit = exit ?? null
        row.output = item.aggregatedOutput ?? item.aggregated_output ?? ''
        row.durationMs = item.durationMs ?? (Date.parse(at) - Date.parse(row.at) || undefined)
        row.body = row.output
        if (exit != null && exit !== 0) row.tone = 'danger'
      }
      return
    }
    if (completed && ['file_change', 'fileChange'].includes(item.type || '')) {
      add({
        key,
        kind: 'command',
        label: 'Files changed',
        body: item.changes?.map((change) => change.path).join('\n') || '',
        at,
      })
      return
    }
    if (name === 'turn/completed' || name === 'turn.failed') {
      const status = name === 'turn.failed' ? 'failed' : params.turn?.status || 'completed'
      if (status !== 'completed')
        add({
          key,
          kind: 'error',
          tone: 'danger',
          label: `Turn ${status}`,
          body: entry.error?.message || '',
          at,
        })
      return
    }
    if (name === 'error') {
      add({
        key,
        kind: 'error',
        tone: 'danger',
        label: 'Executor error',
        body: params.error?.message || entry.message || entry.error?.message || line,
        at,
      })
      return
    }
    if (
      name.includes('/delta') ||
      [
        'turn/started',
        'turn.started',
        'thread/started',
        'thread.started',
        'turn.completed',
      ].includes(name) ||
      started
    )
      return
    // Keep unrecognised native output inspectable instead of silently dropping it.
    add({ key, kind: 'system', label: name || 'Executor event', body: line, at })
  }
  for (const event of events) {
    const detail = event.detail || {}
    const raw = detail.content ?? detail.body ?? detail.error
    const at = event.occurred_at
    if (event.phase === 'executor_stdout' && typeof raw === 'string') {
      buffer += raw
      bufferAt = at
      const parts = buffer.split('\n')
      buffer = parts.pop() || ''
      parts.forEach((line, i) => output(line, at, `${event.activity_id}:${i}`))
      bufferKey = `${event.activity_id}:${parts.length}`
      continue
    }
    const text =
      typeof raw === 'string' ? raw.replace(/\n+$/, '') : raw ? JSON.stringify(raw, null, 2) : ''
    const key = String(event.activity_id)
    const base = { key, at, runID: event.run_id || undefined, label: event.summary || event.phase }
    if (event.run_id) meta.runID = event.run_id
    switch (event.phase) {
      case 'task_received':
        add({
          ...base,
          kind: 'system',
          label: 'Task received',
          body: String(detail.request ?? text),
        })
        break
      case 'executor_started':
        meta.attempts++
        add({ ...base, kind: 'system', body: '' })
        break
      case 'executor_stderr': {
        const serious = STDERR_ERROR.test(text) && !/^(\s*WARN\b.*\n?)+$/.test(text)
        add({
          ...base,
          kind: serious ? 'error' : 'system',
          tone: serious ? 'danger' : 'warn',
          label: 'stderr',
          body: text,
        })
        break
      }
      case 'executor_failed_attempt':
        add({ ...base, kind: 'error', tone: 'danger', body: text })
        break
      case 'task_finished': {
        const status = String(detail.result_status || '')
        meta.finishedStatus = status || undefined
        const reasons = Array.isArray(detail.reason_codes) ? detail.reason_codes.join(', ') : ''
        const failed = !!status && status !== 'success'
        add({
          ...base,
          kind: failed ? 'error' : 'system',
          tone: failed ? 'danger' : 'ok',
          body: reasons,
        })
        break
      }
      default:
        add({
          ...base,
          kind: event.phase.startsWith('egress_') ? 'delivery' : 'system',
          body: text,
        })
    }
  }
  if (buffer.trim()) {
    if (finished) output(buffer, bufferAt, bufferKey)
    else {
      try {
        JSON.parse(buffer)
        output(buffer, bufferAt, bufferKey)
      } catch {
        /* Incomplete JSON waits for the next chunk. */
      }
    }
  }
  let tokens = execTokens
  for (const total of threadTotals.values()) tokens = addUsage(tokens, total)
  meta.tokens = tokens
  return { entries, meta }
}

export function parseShow(value: string | null): Set<EntryKind> {
  const all = ENTRY_KINDS.map((k) => k.kind)
  if (!value) return new Set(all)
  if (value === 'none') return new Set()
  return new Set(value.split(',').filter((v): v is EntryKind => all.includes(v as EntryKind)))
}
export function formatShow(show: Set<EntryKind>): string | null {
  const kinds = ENTRY_KINDS.map((k) => k.kind).filter((k) => show.has(k))
  if (kinds.length === ENTRY_KINDS.length) return null
  return kinds.length ? kinds.join(',') : 'none'
}

export function matchesFind(entry: TranscriptEntry, needle: string) {
  if (!needle) return true
  const n = needle.toLowerCase()
  return [entry.label, entry.body, entry.command || ''].some((s) => s.toLowerCase().includes(n))
}

export function entryText(entry: TranscriptEntry) {
  if (entry.kind === 'command' && entry.command != null)
    return `$ ${entry.command}${entry.exit != null ? `  (exit ${entry.exit})` : ''}${entry.output ? `\n${entry.output}` : ''}`
  return entry.body ? `${entry.label}: ${entry.body}` : entry.label
}

export interface FoldedEntry {
  entry: TranscriptEntry
  /** All consecutive entries folded into this row, oldest first; length 1 when not folded. */
  members: TranscriptEntry[]
}

const foldKey = (e: TranscriptEntry) =>
  [e.kind, e.label, e.command ?? '', e.exit ?? '', e.body].join('\u0000').replace(/\d+/g, '#')

/**
 * Folds runs of consecutive entries that differ only in their numbers (timestamps, counters),
 * e.g. a heartbeat line every few seconds. Errors and running commands are never folded, so
 * error navigation and live command state stay per entry.
 */
export function foldRepeats(entries: readonly TranscriptEntry[]): FoldedEntry[] {
  const out: FoldedEntry[] = []
  let key = ''
  for (const entry of entries) {
    const foldable = entry.kind !== 'error' && !entry.running
    const k = foldable ? foldKey(entry) : ''
    const last = out[out.length - 1]
    if (foldable && last && k === key) last.members.push(entry)
    else out.push({ entry, members: [entry] })
    key = k
  }
  return out
}
