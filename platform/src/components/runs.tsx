import { useCallback, useMemo, useState, type ReactNode } from 'react'
import { cn } from '@/lib/utils'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Bell, CalendarClock, Cog, Mail, MessageSquare } from 'lucide-react'
import { FactsRow, StatusBadge, shortAge, useNow, type Fact } from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { api, useDeadLetters } from '@/lib/api'
import {
  encode,
  fmtLatency,
  fmtTokens,
  fmtWhen,
  fullTime,
  isFailure,
  itemName,
  timeMs,
} from '@/lib/format'
import { statusLabel } from '@/lib/status'
import { transcript, type TranscriptEntry } from '@/lib/transcript'
import type {
  ActivityEvent,
  DeadLetter,
  EventsSnapshot,
  Executor,
  RunRow,
  WorkItem,
} from '@/lib/types'

const SOURCE_ICONS: Record<string, typeof MessageSquare> = {
  im: MessageSquare,
  telegram: MessageSquare,
  cron: CalendarClock,
  reminder: Bell,
  email: Mail,
  internal: Cog,
}

export function SourceLabel({ source }: { source: string | null | undefined }) {
  const value = source || '—'
  const Icon = SOURCE_ICONS[value]
  return (
    <span className="source-label" title={value}>
      {Icon && <Icon aria-hidden />}
      <span>{value}</span>
    </span>
  )
}

export function RunStatus({ status, title }: { status: string; title?: string }) {
  return (
    <StatusBadge
      status={status}
      kind="run"
      title={
        title ||
        (status === 'success'
          ? 'Execution completed successfully. Reply delivery is tracked separately.'
          : undefined)
      }
    />
  )
}

/** The one-line reason a run failed, from the executor errors, last error or reason codes. */
export function runError(run: Pick<RunRow, 'execution_errors' | 'last_error' | 'reason_codes'>) {
  return (
    run.execution_errors?.[0] ||
    run.last_error ||
    (run.reason_codes?.length ? run.reason_codes.join(', ') : undefined)
  )
}

export function tokenTitle(tokens: RunRow['tokens'], model?: string | null) {
  if (!tokens) return undefined
  const t = tokens
  return [
    `in ${t.input_tokens ?? '—'} · cached ${t.cached_input_tokens ?? '—'} · out ${t.output_tokens ?? '—'} · reasoning ${t.reasoning_output_tokens ?? '—'} · total ${t.total_tokens ?? '—'}`,
    model,
  ]
    .filter(Boolean)
    .join(' · ')
}

type Entry =
  { kind: 'live'; key: string; executor: Executor } | { kind: 'run'; key: string; run: RunRow }

const startedAt = (entry: Entry) =>
  entry.kind === 'run' ? entry.run.created_at : entry.executor.started_at
const workItemOf = (entry: Entry) =>
  entry.kind === 'run' ? entry.run.work_item_id : entry.executor.work_item_id
const onRun =
  <V,>(fn: (run: RunRow) => V) =>
  (entry: Entry) =>
    entry.kind === 'run' ? fn(entry.run) : undefined
const isLive = (entry: Entry) => entry.kind === 'live'

/**
 * Run history shared by /runs and the work item page. Running executors are pinned as the first
 * rows; dead-letter rows expand to show the dead letter.
 */
export function RunTable({
  runs,
  live = [],
  deadLetters,
  items,
  label,
  loading,
  empty,
  sortParam = 'sort',
  showRequest = true,
}: {
  runs: RunRow[] | undefined
  live?: Executor[]
  deadLetters?: Map<string, DeadLetter>
  /** Pass to show the Item column. */
  items?: Map<string, WorkItem>
  label: string
  loading?: boolean
  empty: ReactNode
  sortParam?: string
  /** Hide the request column, e.g. when every run repeats the item's prompt. */
  showRequest?: boolean
}) {
  const now = useNow(1000)

  const columns = useMemo<Column<Entry>[]>(() => {
    const itemLabel = (id: string | null) => {
      if (!id) return '—'
      const item = items?.get(id)
      return item ? itemName(item) : id
    }
    const cols: (Column<Entry> | false)[] = [
      {
        key: 'when',
        header: 'When',
        width: 112,
        narrowWidth: 100,
        sortValue: onRun((r) => timeMs(r.created_at)),
        sortDescFirst: true,
        cell: (e) => <time className="num">{fmtWhen(startedAt(e))}</time>,
        title: (e) => fullTime(startedAt(e)),
      },
      {
        key: 'status',
        header: 'Status',
        width: 116,
        narrowWidth: 104,
        sortValue: onRun((r) => r.result_status),
        cell: (e) =>
          e.kind === 'run' ? (
            <RunStatus status={e.run.result_status} title={runError(e.run)} />
          ) : (
            <StatusBadge status="live" kind="run">
              Live {shortAge(now - timeMs(e.executor.started_at))}
            </StatusBadge>
          ),
      },
      !!items && {
        key: 'item',
        header: 'Item',
        width: 200,
        narrowWidth: 'auto',
        sortValue: (e) => itemLabel(workItemOf(e)),
        cell: (e) => {
          const id = workItemOf(e)
          return id ? (
            <Link className="cell-primary" to={`/items/${encode(id)}`}>
              {itemLabel(id)}
            </Link>
          ) : (
            '—'
          )
        },
        title: (e) => itemLabel(workItemOf(e)) || undefined,
      },
      {
        key: 'source',
        header: 'Source',
        width: 92,
        priority: 2,
        sortValue: (e) => (e.kind === 'run' ? e.run.source : e.executor.source),
        cell: (e) => <SourceLabel source={e.kind === 'run' ? e.run.source : e.executor.source} />,
      },
      {
        key: 'latency',
        header: 'Latency',
        width: 92,
        align: 'right',
        priority: 2,
        sortValue: onRun((r) => r.latency_ms),
        sortDescFirst: true,
        cell: (e) =>
          e.kind === 'run' ? (
            fmtLatency(e.run.latency_ms)
          ) : (
            <span className="muted">{fmtLatency(now - timeMs(e.executor.started_at))}</span>
          ),
      },
      {
        key: 'model',
        header: 'Model',
        width: 128,
        priority: 3,
        sortValue: onRun((r) => r.model || r.executed_model_tier),
        cell: (e) => {
          if (e.kind === 'live') return <span className="muted">—</span>
          const r = e.run
          return r.model || r.executed_model_tier ? (
            <span className="model-cell">
              {r.model && <span className="mono">{r.model}</span>}
              {r.executed_model_tier && <span className="muted">{r.executed_model_tier}</span>}
            </span>
          ) : (
            <span className="muted">—</span>
          )
        },
        title: (e) =>
          e.kind === 'run'
            ? [e.run.model, e.run.executed_model_tier && `tier ${e.run.executed_model_tier}`]
                .filter(Boolean)
                .join(' · ') || undefined
            : undefined,
      },
      {
        key: 'attempts',
        header: <span title="Attempts">Att</span>,
        width: 60,
        align: 'right',
        priority: 3,
        sortValue: (e) => (e.kind === 'run' ? e.run.attempt_count : e.executor.attempt),
        sortDescFirst: true,
        cell: (e) => {
          const n = e.kind === 'run' ? e.run.attempt_count : e.executor.attempt
          return n == null ? (
            <span className="muted">—</span>
          ) : (
            <span className="num" data-tone={n > 1 ? 'warn' : undefined}>
              {n}
            </span>
          )
        },
        title: (e) => (e.kind === 'live' ? `attempt ${e.executor.attempt}` : undefined),
      },
      {
        key: 'tokens',
        header: 'Tokens',
        width: 84,
        align: 'right',
        priority: 3,
        mono: true,
        sortValue: onRun((r) => r.tokens?.total_tokens),
        sortDescFirst: true,
        cell: (e) => (e.kind === 'run' ? fmtTokens(e.run.tokens?.total_tokens) : '—'),
        title: (e) => (e.kind === 'run' ? tokenTitle(e.run.tokens) : undefined),
      },
      showRequest && {
        key: 'request',
        header: 'Request',
        priority: items ? 2 : undefined,
        cell: (e) => (
          <span className="request-cell">
            {e.kind === 'run'
              ? e.run.preview || e.run.task_id || e.run.run_id
              : e.executor.preview || e.executor.phase || 'Task in progress'}
          </span>
        ),
        title: (e) => (e.kind === 'run' ? e.run.preview : e.executor.preview) || undefined,
      },
    ]
    return cols.filter((c): c is Column<Entry> => !!c)
  }, [items, now, showRequest])

  const entries = useMemo<Entry[] | undefined>(() => {
    if (!runs) return undefined
    return [
      ...[...live]
        .sort((a, b) => timeMs(b.started_at) - timeMs(a.started_at))
        .map((executor): Entry => ({ kind: 'live', key: `live:${executor.task_id}`, executor })),
      ...runs.map((run): Entry => ({ kind: 'run', key: run.run_id, run })),
    ]
  }, [runs, live])

  const expand = useCallback(
    (entry: Entry) => {
      if (entry.kind !== 'run') return null
      const dl = deadLetters?.get(entry.run.run_id)
      if (dl) return <DeadLetterDetail dl={dl} />
      return isFailure(entry.run.result_status) ? (
        <RunFailureDetail run={entry.run} />
      ) : (
        <RunSuccessDetail run={entry.run} />
      )
    },
    [deadLetters],
  )

  return (
    <DataTable
      label={label}
      columns={columns}
      rows={entries}
      rowKey={(e) => e.key}
      rowHref={(e) =>
        e.kind === 'live' ? `/tasks/${encode(e.executor.task_id)}` : `/runs/${encode(e.run.run_id)}`
      }
      rowTone={(e) => (e.kind === 'live' ? 'live' : undefined)}
      pinned={isLive}
      loading={loading}
      empty={empty}
      sortParam={sortParam}
      defaultSort="-when"
      expand={expand}
      expandLabel="run details"
      rowLabel={(e) =>
        e.kind === 'live'
          ? `Live run, ${e.executor.preview || e.executor.task_id}, started ${fmtWhen(e.executor.started_at)}`
          : `${statusLabel(e.run.result_status)} run, ${items && e.run.work_item_id ? `${items.get(e.run.work_item_id) ? itemName(items.get(e.run.work_item_id)!) : e.run.work_item_id}, ` : ''}${fmtWhen(e.run.created_at)}`
      }
    />
  )
}

const EVENT_PAGES = 20

export async function taskEvents(taskID: string) {
  const events: ActivityEvent[] = []
  for (let page = 0; page < EVENT_PAGES; page++) {
    const after = events.length ? events[events.length - 1].activity_id : 0
    const data = await api<EventsSnapshot>(`worker/tasks/${encode(taskID)}/events?after=${after}`)
    events.push(...data.events)
    if (data.events.length < 100) break
  }
  return events
}

function lastFailedCommand(entries: TranscriptEntry[]) {
  return entries.findLast((e) => e.kind === 'command' && e.exit != null && e.exit !== 0)
}

const firstLine = (text: string) => text.trim().split('\n')[0]

// The executor's closing "task finished with execution_error" line restates the outcome, not a cause.
const isCauseError = (e: TranscriptEntry) =>
  e.kind === 'error' && !!e.body.trim() && !/^task finished/i.test(e.label)

/**
 * The best explanation a transcript offers for a failure the run did not record: an error
 * line, then stderr, then a command that never finished, then the executor's last message.
 */
function transcriptReason(entries: TranscriptEntry[]): { text: string; error: boolean } | null {
  const error = entries.find(isCauseError)
  if (error) return { text: `${error.label}: ${firstLine(error.body)}`, error: true }
  const stderr = entries.findLast((e) => e.label === 'stderr' && e.body.trim())
  if (stderr)
    return {
      text: `stderr: ${firstLine(stderr.body.split('\n').filter(Boolean).at(-1) || '')}`,
      error: true,
    }
  const interrupted = entries.findLast((e) => e.interrupted)
  if (interrupted) return { text: `A command never finished: ${interrupted.command}`, error: false }
  const message = entries.findLast((e) => e.kind === 'assistant' && e.body.trim())
  if (message) return { text: `Its last message: “${firstLine(message.body)}”`, error: false }
  return null
}

function useTranscript(taskID: string | null | undefined) {
  const events = useQuery({
    queryKey: ['task-events', taskID],
    queryFn: () => taskEvents(taskID!),
    enabled: !!taskID,
    staleTime: Infinity,
  })
  const entries = useMemo(
    () => (events.data ? transcript(events.data, true).entries : undefined),
    [events.data],
  )
  return { events, entries }
}

function TranscriptLink({ runID, errors }: { runID: string; errors?: number }) {
  return (
    <Link className="text-link dl-open" to={`/runs/${encode(runID)}`}>
      Open transcript
      {errors ? ` (${errors} ${errors === 1 ? 'error' : 'errors'})` : ''}
    </Link>
  )
}

/**
 * Why a run failed: executor errors and the last error, or, when the run recorded none, the
 * first error in its transcript; plus the last failing command and its output.
 */
function RunFailureDetail({ run }: { run: RunRow }) {
  const { events, entries } = useTranscript(run.task_id)
  const command = useMemo(() => (entries ? lastFailedCommand(entries) : undefined), [entries])
  const errorEntries = useMemo(() => entries?.filter(isCauseError) || [], [entries])
  const recorded = [
    ...(run.execution_errors || []),
    run.last_error
      ? `${run.last_error}${run.last_error_stage ? ` · stage ${run.last_error_stage}` : ''}`
      : '',
  ].filter(Boolean)
  const reason = !recorded.length && entries ? transcriptReason(entries) : null
  const fromTranscript = reason?.error ? reason.text : ''
  const errors = recorded.length ? recorded : fromTranscript ? [fromTranscript] : []
  const output = (command?.output || '').replace(/\n+$/, '').split('\n').slice(-6).join('\n')
  return (
    <div className="dl-detail is-failed">
      {errors.length > 0 ? (
        <pre className="dl-error" data-tone="danger">
          {errors.join('\n')}
          {fromTranscript && <span className="dl-source"> · from transcript</span>}
        </pre>
      ) : events.isPending ? (
        <span className="muted">Loading transcript…</span>
      ) : reason ? (
        <span className="muted">No error was recorded for this run. {reason.text}</span>
      ) : (
        <span className="muted">
          The run ended as {statusLabel(run.result_status).toLowerCase()} without recording an
          error, and its transcript has no error, stderr or unfinished command.
        </span>
      )}
      <FactsRow
        items={[
          {
            label: 'Reason',
            value: run.reason_codes?.join(', '),
            mono: true,
            tone: 'warn',
            hidden: !run.reason_codes?.length,
          },
          {
            label: 'Attempts',
            value: run.attempt_count ?? '—',
            tone: (run.attempt_count || 0) > 1 ? 'warn' : undefined,
          },
          {
            label: 'Last failed command',
            value: command ? (
              <span className="fail-cmd">
                <code className="truncate">{command.command}</code>
                <span className="t-exit" data-tone="danger">
                  exit {command.exit}
                </span>
              </span>
            ) : (
              <span className="muted">transcript unavailable</span>
            ),
            title: command?.command,
            hidden: events.isPending || (!command && !events.isError),
          },
          {
            label: 'Finished',
            value: fmtWhen(run.created_at, { seconds: true }),
            title: fullTime(run.created_at),
          },
        ]}
      />
      {output && <pre className="dl-output">{output}</pre>}
      <TranscriptLink runID={run.run_id} errors={errorEntries.length} />
    </div>
  )
}

/** Successful runs expand to the same facts the table hides on narrow screens. */
function RunSuccessDetail({ run }: { run: RunRow }) {
  const t = run.tokens
  return (
    <div className="dl-detail is-ok">
      {run.preview && <p className="dl-request">{run.preview}</p>}
      <FactsRow
        items={[
          { label: 'Source', value: run.source },
          { label: 'Latency', value: fmtLatency(run.latency_ms) },
          { label: 'Model', value: run.model, mono: true, hidden: !run.model },
          { label: 'Tier', value: run.executed_model_tier, hidden: !run.executed_model_tier },
          {
            label: 'Tokens',
            value: t
              ? `${fmtTokens(t.total_tokens)} · in ${fmtTokens(t.input_tokens)} · cached ${fmtTokens(t.cached_input_tokens)} · out ${fmtTokens(t.output_tokens)}`
              : null,
            mono: true,
            hidden: !t,
          },
          { label: 'Attempts', value: run.attempt_count, hidden: run.attempt_count == null },
          {
            label: 'Finished',
            value: fmtWhen(run.created_at, { seconds: true }),
            title: fullTime(run.created_at),
          },
        ]}
      />
      <TranscriptLink runID={run.run_id} />
    </div>
  )
}

function DeadLetterDetail({ dl }: { dl: DeadLetter }) {
  const { entries } = useTranscript(dl.task_id)
  const errors = entries?.filter((e) => e.kind === 'error').length
  return (
    <div className="dl-detail">
      {dl.last_error && (
        <pre className="dl-error" data-tone="danger">
          {dl.last_error}
        </pre>
      )}
      <FactsRow
        items={[
          {
            label: 'Reason',
            value: dl.reason_codes.length ? dl.reason_codes.join(', ') : '—',
            mono: true,
            tone: 'danger',
          },
          { label: 'Attempts', value: dl.attempt_count },
          {
            label: 'Status',
            value: dl.replayed_run_id ? 'Replayed' : statusLabel(dl.status),
          },
          { label: 'Source', value: dl.source || '—' },
          {
            label: 'Reply',
            value: [dl.reply_channel_type, dl.reply_channel_target].filter(Boolean).join(' · '),
            mono: true,
          },
          { label: 'Received', value: fmtWhen(dl.received_at), title: fullTime(dl.received_at) },
          { label: 'Envelope', value: dl.envelope_id, mono: true, title: dl.envelope_id || '' },
          {
            label: 'Replayed as',
            value: dl.replayed_run_id && (
              <Link className="text-link" to={`/runs/${encode(dl.replayed_run_id)}`}>
                {dl.replayed_run_id}
              </Link>
            ),
            hidden: !dl.replayed_run_id,
          },
        ]}
      />
      <TranscriptLink runID={dl.run_id} errors={errors} />
    </div>
  )
}

export function useDeadLetterMap() {
  const query = useDeadLetters(1000)
  const map = useMemo(
    () => new Map((query.data?.dead_letters || []).map((dl) => [dl.run_id, dl])),
    [query.data],
  )
  return [map, query] as const
}

/** Summary strip: run count, failure rate, latency percentiles and tokens. */
/** Figures for finished runs; `runs` is undefined while loading or unavailable, shown as "—". */
export function RunSummary({
  runs,
  live = 0,
  extra = [],
  after = [],
}: {
  runs: RunRow[] | undefined
  /** Live runs in view, which the figures leave out; shown beside the finished count. */
  live?: number
  extra?: Fact[]
  after?: Fact[]
}) {
  const [all, setAll] = useState(false)
  const facts = useMemo(() => {
    if (!runs) return null
    const latencies = runs.map((r) => r.latency_ms).sort((a, b) => a - b)
    const pick = (p: number) =>
      latencies.length
        ? latencies[Math.min(latencies.length - 1, Math.floor(p * latencies.length))]
        : null
    const failed = runs.filter((r) => r.result_status === 'execution_error').length
    const dead = runs.filter((r) => r.result_status === 'dead_letter').length
    const tokens = runs.reduce((sum, r) => sum + (r.tokens?.total_tokens || 0), 0)
    return { p50: pick(0.5), p95: pick(0.95), failed, dead, tokens }
  }, [runs])
  return (
    <div className={cn('run-summary-wrap', all && 'is-all')}>
      <FactsRow
        className="run-summary"
        items={[
          ...extra,
          {
            label: 'Finished',
            value: runs ? (
              <>
                {runs.length}
                {live > 0 && <span className="muted"> · {live} live</span>}
              </>
            ) : null,
            title:
              live > 0
                ? `${runs?.length ?? '—'} finished and ${live} live; the figures here cover finished runs only`
                : 'Finished runs in view',
          },
          {
            label: 'Failed',
            value:
              runs && facts
                ? runs.length
                  ? `${facts.failed} · ${((facts.failed / runs.length) * 100).toFixed(1)}%`
                  : '—'
                : null,
            tone: facts?.failed ? 'warn' : undefined,
          },
          {
            label: 'Dead letters',
            value: facts ? facts.dead : null,
            tone: facts?.dead ? 'danger' : undefined,
            priority: 2,
          },
          { label: 'p50', value: facts ? fmtLatency(facts.p50) : null },
          { label: 'p95', value: facts ? fmtLatency(facts.p95) : null, priority: 2 },
          {
            label: 'Tokens',
            value: facts?.tokens ? fmtTokens(facts.tokens) : '—',
            mono: true,
            priority: 2,
          },
          ...after.map((fact) => ({ priority: 2 as const, ...fact })),
        ]}
      />
      <button type="button" className="facts-more" aria-expanded={all} onClick={() => setAll(!all)}>
        {all ? 'Less' : 'More'}
      </button>
    </div>
  )
}
