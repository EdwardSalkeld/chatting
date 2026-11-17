import { useCallback, useMemo, type ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { Bell, CalendarClock, Cog, Mail, MessageSquare } from 'lucide-react'
import { FactsRow, StatusBadge, shortAge, useNow, type Fact } from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { useDeadLetters } from '@/lib/api'
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
import type { DeadLetter, Executor, RunRow, WorkItem } from '@/lib/types'

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

const SHORT_STATUS: Record<string, string> = { execution_error: 'Error' }

export function RunStatus({ status }: { status: string }) {
  return (
    <StatusBadge status={status} kind="run" title={statusLabel(status)}>
      {SHORT_STATUS[status]}
    </StatusBadge>
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
            <RunStatus status={e.run.result_status} />
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
        narrowWidth: '100%',
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
          if (e.kind === 'live') return <span className="muted">attempt {e.executor.attempt}</span>
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
      {
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
  }, [items, now])

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
      const dl = entry.kind === 'run' ? deadLetters?.get(entry.run.run_id) : undefined
      return dl ? <DeadLetterDetail dl={dl} /> : null
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
      expand={deadLetters ? expand : undefined}
      expandLabel="dead letter"
    />
  )
}

function DeadLetterDetail({ dl }: { dl: DeadLetter }) {
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
            label: 'reason',
            value: dl.reason_codes.length ? dl.reason_codes.join(', ') : '—',
            mono: true,
            tone: 'danger',
          },
          { label: 'attempts', value: dl.attempt_count },
          { label: 'status', value: statusLabel(dl.status) },
          { label: 'source', value: dl.source || '—' },
          {
            label: 'reply',
            value: [dl.reply_channel_type, dl.reply_channel_target].filter(Boolean).join(' · '),
            mono: true,
          },
          { label: 'received', value: fmtWhen(dl.received_at), title: fullTime(dl.received_at) },
          { label: 'envelope', value: dl.envelope_id, mono: true, title: dl.envelope_id || '' },
          {
            label: 'replayed as',
            value: dl.replayed_run_id && (
              <Link className="text-link" to={`/runs/${encode(dl.replayed_run_id)}`}>
                {dl.replayed_run_id}
              </Link>
            ),
            hidden: !dl.replayed_run_id,
          },
        ]}
      />
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
export function RunSummary({ runs, extra = [] }: { runs: RunRow[]; extra?: Fact[] }) {
  const facts = useMemo(() => {
    const latencies = runs.map((r) => r.latency_ms).sort((a, b) => a - b)
    const pick = (p: number) =>
      latencies.length
        ? latencies[Math.min(latencies.length - 1, Math.floor(p * latencies.length))]
        : null
    const failed = runs.filter((r) => isFailure(r.result_status)).length
    const dead = runs.filter((r) => r.result_status === 'dead_letter').length
    const tokens = runs.reduce((sum, r) => sum + (r.tokens?.total_tokens || 0), 0)
    return { p50: pick(0.5), p95: pick(0.95), failed, dead, tokens }
  }, [runs])
  return (
    <FactsRow
      className="run-summary"
      items={[
        ...extra,
        { label: 'runs', value: runs.length },
        {
          label: 'failed',
          value: runs.length
            ? `${facts.failed} · ${((facts.failed / runs.length) * 100).toFixed(1)}%`
            : '—',
          tone: facts.failed ? 'warn' : undefined,
        },
        { label: 'dead letters', value: facts.dead, tone: facts.dead ? 'danger' : undefined },
        { label: 'p50', value: fmtLatency(facts.p50) },
        { label: 'p95', value: fmtLatency(facts.p95) },
        { label: 'tokens', value: facts.tokens ? fmtTokens(facts.tokens) : '—', mono: true },
      ]}
    />
  )
}
