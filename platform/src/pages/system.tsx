import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { FileCode } from 'lucide-react'
import {
  AgeChip,
  ErrorStrip,
  FilterChips,
  FactsRow,
  InlineEmpty,
  Panel,
  SearchInput,
  StatusDot,
  shortAge,
  useNow,
  usePageTitle,
  type Tone,
} from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { useActivity, useHandlerMetrics, useRuns, useWorkerMetrics } from '@/lib/api'
import { encode, fmtBytes, fmtTokens, fmtUptime } from '@/lib/format'
import { parseProm, promSamples, promValue, sampleKey, type PromMetrics } from '@/lib/prom'
import { useUrlParams, useUrlState } from '@/lib/url'
import type { Executor } from '@/lib/types'
import { usageByModel, type ModelUsage } from '@/lib/usage'
import '@/styles/system.css'

const H = 'chatting_message_handler_'
const SQLITE = 'chatting_sqlite_file_size_bytes'
const SQLITE_OK = 'chatting_sqlite_file_stat_success'
const RATE_WINDOW = 5 * 60_000
const METRICS_INTERVAL = 10_000
const ACTIVITY_INTERVAL = 4_000

interface KvRow {
  label: string
  value: ReactNode
  /** Metric name, used for search and the tooltip. */
  metric?: string
  rate?: number
  tone?: Tone
  title?: string
  sub?: boolean
}
interface KvGroup {
  title: string
  rows: KvRow[]
}

const int = (n: number | undefined) =>
  n == null || !Number.isFinite(n) ? '—' : Math.round(n).toLocaleString()

function stamp(ms: number | undefined) {
  if (ms == null || !Number.isFinite(ms)) return '—'
  return new Date(ms).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** Per-minute counter rates over the last few minutes of polls. */
function useRates(metrics: PromMetrics | undefined, at: number) {
  const [history, setHistory] = useState<{ at: number; values: Map<string, number> }[]>([])
  useEffect(() => {
    if (!metrics || !at) return
    const values = new Map<string, number>()
    for (const sample of metrics.samples)
      if (metrics.type[sample.name] === 'counter') values.set(sampleKey(sample), sample.value)
    setHistory((previous) =>
      previous.at(-1)?.at === at
        ? previous
        : [...previous.filter((s) => at - s.at <= RATE_WINDOW), { at, values }],
    )
  }, [metrics, at])
  return useMemo(() => {
    const rates = new Map<string, number>()
    if (history.length < 2) return rates
    const first = history[0],
      last = history[history.length - 1]
    const minutes = (last.at - first.at) / 60_000
    if (minutes <= 0) return rates
    for (const [key, value] of last.values) {
      const before = first.values.get(key)
      if (before != null && value >= before) rates.set(key, (value - before) / minutes)
    }
    return rates
  }, [history])
}

class Reader {
  used = new Set<string>()
  metrics: PromMetrics | undefined
  rates: Map<string, number>
  constructor(metrics: PromMetrics | undefined, rates: Map<string, number>) {
    this.metrics = metrics
    this.rates = rates
  }
  value(name: string) {
    this.used.add(name)
    return promValue(this.metrics, name)
  }
  row(label: string, name: string, extra: Partial<KvRow> = {}): KvRow {
    const value = this.value(name)
    return {
      label,
      value: int(value),
      metric: name,
      rate: this.rates.get(name),
      title: this.metrics?.help[name],
      ...extra,
    }
  }
  sqlite(component: string): KvRow[] {
    this.used.add(SQLITE).add(SQLITE_OK)
    const samples = promSamples(this.metrics, SQLITE, { component })
    const rows = samples.map((sample) => {
      const ok = promValue(this.metrics, SQLITE_OK, sample.labels)
      return {
        label: sample.labels.file || 'file',
        value: ok === 0 ? 'unreadable' : fmtBytes(sample.value),
        tone: ok === 0 ? ('danger' as const) : undefined,
        metric: sampleKey(sample),
        title: this.metrics?.help[SQLITE],
      }
    })
    if (samples.length > 1)
      rows.push({
        label: 'total',
        value: fmtBytes(samples.reduce((sum, s) => sum + s.value, 0)),
        tone: undefined,
        metric: SQLITE,
        title: 'Sum of the files above',
      })
    return rows
  }
  rest(prefix: string): KvRow[] {
    return (this.metrics?.samples || [])
      .filter((s) => !this.used.has(s.name))
      .map((s) => {
        const key = sampleKey(s)
        return {
          label: key.startsWith(prefix) ? key.slice(prefix.length) : key,
          value: Number.isInteger(s.value) ? int(s.value) : s.value.toFixed(3),
          metric: key,
          rate: this.rates.get(key),
          title: this.metrics?.help[s.name],
        }
      })
  }
}

function handlerGroups(metrics: PromMetrics | undefined, rates: Map<string, number>, now: number) {
  const r = new Reader(metrics, rates)
  const started = r.value(`${H}process_start_time_seconds`)
  const up = started != null ? (now - started * 1000) / 1000 : r.value(`${H}uptime_seconds`)
  r.value(`${H}uptime_seconds`)
  const lastLoop = r.value(`${H}last_loop_completed_timestamp_seconds`)
  const loopAge = lastLoop != null ? now - lastLoop * 1000 : undefined
  const dropped = (metrics?.samples || [])
    .map((s) => /^chatting_message_handler_egress_dropped_(.+)_total$/.exec(s.name))
    .filter((m): m is RegExpExecArray => !!m)
  const late = r.value(`${H}reminder_late_total`)
  const droppedTotal = r.value(`${H}egress_dropped_total`)
  const groups: KvGroup[] = [
    {
      title: 'Process',
      rows: [
        { label: 'uptime', value: fmtUptime(up), metric: `${H}uptime_seconds` },
        {
          label: 'started',
          value: stamp(started != null ? started * 1000 : undefined),
          metric: `${H}process_start_time_seconds`,
        },
        {
          label: 'last loop',
          value: loopAge == null ? '—' : `${shortAge(loopAge)} ago`,
          tone:
            loopAge == null
              ? undefined
              : loopAge > 300_000
                ? 'danger'
                : loopAge > 60_000
                  ? 'warn'
                  : 'ok',
          metric: `${H}last_loop_completed_timestamp_seconds`,
          title: lastLoop != null ? stamp(lastLoop * 1000) : undefined,
        },
        (() => {
          // Until two polls give a live rate, show the average since start.
          const loops = r.value(`${H}loops_total`)
          const live = rates.get(`${H}loops_total`)
          const average = loops != null && up ? loops / (up / 60) : undefined
          return r.row('loops', `${H}loops_total`, {
            value: (
              <>
                {int(loops)}
                {live == null && average != null && (
                  <span className="sys-sub"> · ~{average.toFixed(1)}/m avg</span>
                )}
              </>
            ),
          })
        })(),
      ],
    },
    { title: 'Ingress', rows: [r.row('published', `${H}ingress_published_total`)] },
    {
      title: 'Reminders',
      rows: [
        r.row('created', `${H}reminder_created_total`),
        r.row('cancelled', `${H}reminder_cancelled_total`),
        r.row('due', `${H}reminder_due_total`),
        r.row('late', `${H}reminder_late_total`, { tone: late ? 'warn' : undefined }),
        r.row('retried', `${H}reminder_retried_total`),
        r.row('published', `${H}reminder_published_total`),
        r.row('fired', `${H}reminder_fired_total`),
      ],
    },
    {
      title: 'Egress',
      rows: [
        r.row('received', `${H}egress_received_total`),
        r.row('dispatched', `${H}egress_dispatched_total`),
        r.row('incremental', `${H}egress_incremental_dispatched_total`),
        r.row('messages', `${H}egress_message_dispatched_total`),
        r.row('completions', `${H}egress_completion_applied_total`),
        r.row('deduped', `${H}egress_deduped_total`),
        (() => {
          const pct = r.value(`${H}egress_dedupe_hit_rate_pct`)
          return {
            label: 'dedupe hit rate',
            value:
              pct == null ? (
                '—'
              ) : (
                <>
                  {pct.toFixed(1)}%<span className="sys-sub"> of received were repeats</span>
                </>
              ),
            metric: `${H}egress_dedupe_hit_rate_pct`,
            title: metrics?.help[`${H}egress_dedupe_hit_rate_pct`],
          }
        })(),
        r.row('dropped', `${H}egress_dropped_total`, {
          tone: droppedTotal ? 'warn' : undefined,
        }),
        ...dropped.map(([name, reason]) => {
          const value = r.value(name)
          return r.row(reason.replaceAll('_', ' '), name, {
            tone: value ? 'warn' : undefined,
            sub: true,
          })
        }),
      ],
    },
    { title: 'SQLite', rows: r.sqlite('handler') },
  ]
  const other = r.rest(H)
  if (other.length) groups.push({ title: 'Other', rows: other })
  return groups
}

function workerGroups(
  metrics: PromMetrics | undefined,
  rates: Map<string, number>,
  activity: ReturnType<typeof useActivity>['data'],
  now: number,
) {
  const r = new Reader(metrics, rates)
  const outboxAge = activity?.outbox?.oldest_created_at
    ? now - new Date(activity.outbox.oldest_created_at).getTime()
    : undefined
  const lastEvent = activity?.recent_activity[0]?.occurred_at
  const lastEventAge = lastEvent ? now - new Date(lastEvent).getTime() : undefined
  const groups: KvGroup[] = [
    {
      title: 'Queue',
      rows: [
        {
          label: 'queued',
          value: int(activity?.queue.queued),
          tone: activity?.queue.queued ? 'pending' : undefined,
        },
        { label: 'running', value: int(activity?.queue.running) },
        {
          label: 'live executors',
          value: int(activity?.active_executors.length),
          tone: activity?.active_executors.length ? 'live' : undefined,
        },
        {
          label: 'current phase',
          value: (activity?.current_executor.phase || 'idle').replaceAll('_', ' '),
        },
      ],
    },
    {
      title: 'Outbox',
      rows: [
        {
          label: 'pending publish',
          value: activity?.outbox ? int(activity.outbox.pending) : '—',
          tone: activity?.outbox?.pending ? 'pending' : undefined,
        },
        {
          label: 'oldest',
          value: outboxAge == null ? '—' : `${shortAge(outboxAge)} ago`,
          tone: outboxAge != null && outboxAge > 5 * 60_000 ? 'warn' : undefined,
          title: activity?.outbox?.oldest_created_at || undefined,
        },
      ],
    },
    {
      title: 'Activity',
      rows: [
        {
          label: 'last event',
          value: lastEventAge == null ? '—' : `${shortAge(lastEventAge)} ago`,
          title: lastEvent,
        },
        {
          label: 'activity buffer',
          value: activity
            ? `${activity.recent_activity.length}/${activity.history_limit}${activity.history_truncated || activity.recent_activity.length >= activity.history_limit ? ' (full)' : ''}`
            : '—',
          title:
            'Recent events the worker keeps in memory for this page; older events stay in the database',
        },
      ],
    },
    { title: 'SQLite', rows: r.sqlite('worker') },
  ]
  const other = r.rest('chatting_worker_')
  if (other.length) groups.push({ title: 'Other', rows: other })
  return groups
}

function filterGroups(groups: KvGroup[], q: string) {
  const needle = q.trim().toLowerCase()
  if (!needle) return groups
  return groups
    .map((group) => ({
      ...group,
      rows: group.title.toLowerCase().includes(needle)
        ? group.rows
        : group.rows.filter((row) =>
            `${row.label} ${row.metric || ''}`.toLowerCase().includes(needle),
          ),
    }))
    .filter((group) => group.rows.length)
}

function Groups({ groups, empty }: { groups: KvGroup[]; empty: string }) {
  if (!groups.length) return <InlineEmpty>{empty}</InlineEmpty>
  return (
    <div className="sys-groups">
      {groups.map((group) => (
        <section className="sys-group" key={group.title}>
          <h3>{group.title}</h3>
          <dl>
            {group.rows.map((row) => (
              <div
                key={row.metric || row.label}
                className="sys-kv"
                title={[row.metric, row.title].filter(Boolean).join('\n') || undefined}
              >
                <dt className={row.sub ? 'is-sub' : undefined}>{row.label}</dt>
                <dd className="num" data-tone={row.tone}>
                  {row.value}
                </dd>
                <dd className="sys-rate num">
                  {row.rate
                    ? `+${row.rate < 10 ? row.rate.toFixed(1) : Math.round(row.rate)}/m`
                    : ''}
                </dd>
              </div>
            ))}
          </dl>
        </section>
      ))}
    </div>
  )
}

export function System() {
  usePageTitle('System')
  const handler = useHandlerMetrics()
  const worker = useWorkerMetrics()
  const activity = useActivity()
  const now = useNow(1000)
  const [params, update] = useUrlParams()
  const [q, setQ] = useUrlState('q')
  const raw = params.get('show') === 'raw'

  const handlerMetrics = useMemo(
    () => (handler.data ? parseProm(handler.data) : undefined),
    [handler.data],
  )
  const workerMetrics = useMemo(
    () => (worker.data ? parseProm(worker.data) : undefined),
    [worker.data],
  )
  const handlerRates = useRates(handlerMetrics, handler.dataUpdatedAt)
  const workerRates = useRates(workerMetrics, worker.dataUpdatedAt)

  const hGroups = filterGroups(handlerGroups(handlerMetrics, handlerRates, now), q)
  const wGroups = filterGroups(workerGroups(workerMetrics, workerRates, activity.data, now), q)

  const started = promValue(handlerMetrics, `${H}process_start_time_seconds`)
  const lastLoop = promValue(handlerMetrics, `${H}last_loop_completed_timestamp_seconds`)
  const loopAge = lastLoop != null ? now - lastLoop * 1000 : undefined
  const executors = activity.data?.active_executors

  const handlerTone: Tone = handler.error
    ? 'danger'
    : !handler.data
      ? 'pending'
      : loopAge != null && loopAge > 60_000
        ? 'warn'
        : 'ok'
  const workerTone: Tone = activity.error ? 'danger' : !activity.data ? 'pending' : 'ok'

  return (
    <div className="sys-page">
      <ErrorStrip query={handler} label="Handler metrics" />
      <ErrorStrip query={worker} label="Worker metrics" />
      <ErrorStrip query={activity} label="Worker activity" />
      <FactsRow
        className="sys-facts"
        items={[
          {
            label: 'Handler',
            value: (
              <span className="sys-state">
                <StatusDot tone={handlerTone} />
                {handler.error ? 'unavailable' : handler.data ? 'up' : 'loading'}
              </span>
            ),
          },
          {
            label: 'Uptime',
            value: started != null ? fmtUptime((now - started * 1000) / 1000) : null,
          },
          {
            label: 'Last loop',
            value: loopAge != null ? `${shortAge(loopAge)} ago` : null,
            tone: handlerTone === 'warn' ? 'warn' : undefined,
          },
          {
            label: 'Worker',
            value: (
              <span className="sys-state">
                <StatusDot tone={workerTone} />
                {activity.error ? 'unavailable' : activity.data ? 'up' : 'loading'}
              </span>
            ),
          },
          {
            label: 'Queue',
            value: activity.data
              ? `${activity.data.queue.queued} queued · ${activity.data.queue.running} running`
              : null,
          },
          {
            label: 'Live',
            value: executors ? executors.length : null,
            tone: executors?.length ? 'live' : undefined,
          },
          {
            label: 'Outbox',
            value: activity.data?.outbox ? activity.data.outbox.pending : null,
            tone: activity.data?.outbox?.pending ? 'pending' : undefined,
          },
        ]}
      />
      <div className="toolbar sys-toolbar">
        <SearchInput
          className="sys-search"
          value={q}
          onChange={(value) => setQ(value, { replace: true })}
          placeholder="Filter metrics…"
        />
        <div className="toolbar-end">
          <button
            type="button"
            className="filter-chip"
            aria-pressed={raw}
            onClick={() => update({ show: raw ? null : 'raw' })}
          >
            <FileCode size={14} aria-hidden />
            Show raw
          </button>
        </div>
      </div>
      <div className="sys-grid">
        <Panel
          title="Handler"
          meta="/metrics"
          actions={
            <AgeChip
              updatedAt={handler.dataUpdatedAt}
              interval={METRICS_INTERVAL}
              error={!!handler.error}
            />
          }
        >
          {!handler.data && !handler.error ? (
            <div className="sys-skeleton" aria-hidden />
          ) : (
            <Groups groups={hGroups} empty={q ? 'No handler metrics match' : 'No metrics'} />
          )}
        </Panel>
        <Panel
          title="Worker"
          meta="/api/worker/activity · /metrics"
          actions={
            <AgeChip
              updatedAt={activity.dataUpdatedAt}
              interval={ACTIVITY_INTERVAL}
              error={!!activity.error}
            />
          }
        >
          {!activity.data && !activity.error ? (
            <div className="sys-skeleton" aria-hidden />
          ) : (
            <Groups groups={wGroups} empty={q ? 'No worker metrics match' : 'No metrics'} />
          )}
        </Panel>
      </div>
      <Panel title="Executors" count={executors?.length ?? null} meta="Live executor processes">
        <ExecutorTable executors={executors} loading={activity.isPending} now={now} />
      </Panel>
      <TokensByModel now={now} />
      {raw && (
        <div className="sys-grid">
          <RawPanel title="Handler /metrics" text={handler.data} q={q} />
          <RawPanel title="Worker /metrics" text={worker.data} q={q} />
        </div>
      )}
    </div>
  )
}

const WINDOWS: Record<string, { ms: number; label: string }> = {
  '24h': { ms: 86400e3, label: 'last 24 hours' },
  '7d': { ms: 7 * 86400e3, label: 'last 7 days' },
  '30d': { ms: 30 * 86400e3, label: 'last 30 days' },
}

function TokensByModel({ now }: { now: number }) {
  const minute = Math.floor(now / 60000) * 60000
  const [rawWindow, setWindow] = useUrlState('window', '7d')
  const window = WINDOWS[rawWindow] ? rawWindow : '7d'
  const { ms, label: windowLabel } = WINDOWS[window]
  const query = useRuns({ limit: 1000, since: new Date(minute - ms).toISOString() })
  const rows = useMemo(() => query.data && usageByModel(query.data.runs), [query.data])
  const total = rows?.reduce((sum, r) => sum + r.total, 0) || 0
  const n = (value: number) => <span className="num">{fmtTokens(value)}</span>
  const columns: Column<ModelUsage>[] = [
    { key: 'model', header: 'Model', mono: true, sortValue: (r) => r.model, cell: (r) => r.model },
    {
      key: 'runs',
      header: 'Runs',
      width: 72,
      align: 'right',
      sortValue: (r) => r.runs,
      sortDescFirst: true,
      cell: (r) => r.runs,
    },
    {
      key: 'input',
      header: 'In',
      width: 84,
      align: 'right',
      priority: 2,
      sortValue: (r) => r.input,
      sortDescFirst: true,
      cell: (r) => n(r.input),
    },
    {
      key: 'cached',
      header: 'Cached',
      width: 96,
      align: 'right',
      priority: 3,
      sortValue: (r) => r.cached,
      sortDescFirst: true,
      cell: (r) => n(r.cached),
      title: (r) => (r.input ? `${Math.round((r.cached / r.input) * 100)}% of input` : undefined),
    },
    {
      key: 'output',
      header: 'Out',
      width: 84,
      align: 'right',
      sortValue: (r) => r.output,
      sortDescFirst: true,
      cell: (r) => n(r.output),
    },
    {
      key: 'reasoning',
      header: 'Reasoning',
      width: 120,
      align: 'right',
      priority: 3,
      sortValue: (r) => r.reasoning,
      sortDescFirst: true,
      cell: (r) => n(r.reasoning),
    },
    {
      key: 'total',
      header: 'Total',
      width: 92,
      align: 'right',
      sortValue: (r) => r.total,
      sortDescFirst: true,
      cell: (r) => n(r.total),
    },
    {
      key: 'share',
      header: 'Share',
      width: 76,
      align: 'right',
      priority: 2,
      sortValue: (r) => r.total,
      cell: (r) => (total ? `${Math.round((r.total / total) * 100)}%` : '—'),
    },
  ]
  return (
    <Panel
      id="tokens"
      title="Tokens by model"
      meta={`${windowLabel}${query.data?.truncated ? ' · latest 1000 runs' : ''}`}
      count={rows?.length ?? null}
      actions={
        <FilterChips
          label="Token window"
          value={window}
          onChange={(value) => setWindow(value, { replace: true })}
          options={Object.keys(WINDOWS).map((value) => ({ value, label: value }))}
        />
      }
    >
      <ErrorStrip query={query} label="Runs" />
      <DataTable
        label={`Tokens by model, ${windowLabel}`}
        columns={columns}
        rows={rows}
        rowKey={(r) => r.model}
        loading={query.isPending}
        sortParam="tsort"
        defaultSort="-total"
        skeletonRows={3}
        empty={`No token usage recorded in the ${windowLabel}`}
      />
    </Panel>
  )
}

function ExecutorTable({
  executors,
  loading,
  now,
}: {
  executors: Executor[] | undefined
  loading: boolean
  now: number
}) {
  const columns: Column<Executor>[] = [
    {
      key: 'task',
      header: 'Task',
      cell: (e) => (
        <span className="cell-flex">
          <StatusDot tone="live" label="Live" />
          <span className="truncate cell-primary">{e.preview || e.task_id}</span>
        </span>
      ),
      title: (e) => e.preview,
    },
    {
      key: 'id',
      header: 'Task id',
      width: '26%',
      mono: true,
      priority: 3,
      cell: (e) => <span className="truncate sys-mono">{e.task_id}</span>,
      title: (e) => e.task_id,
    },
    {
      key: 'item',
      header: 'Item',
      width: 160,
      mono: true,
      priority: 2,
      cell: (e) => <span className="truncate sys-mono">{e.work_item_id || '—'}</span>,
      title: (e) => e.work_item_id || undefined,
    },
    { key: 'source', header: 'Source', width: 72, priority: 2, cell: (e) => e.source },
    {
      key: 'phase',
      header: 'Phase',
      width: 140,
      priority: 3,
      cell: (e) => <span className="truncate">{(e.phase || '—').replaceAll('_', ' ')}</span>,
    },
    { key: 'attempt', header: 'Try', width: 48, align: 'right', cell: (e) => e.attempt },
    {
      key: 'age',
      header: 'Age',
      width: 72,
      align: 'right',
      cell: (e) => shortAge(now - new Date(e.started_at).getTime()),
      title: (e) => e.started_at,
    },
  ]
  return (
    <DataTable
      label="Live executors"
      columns={columns}
      rows={executors}
      rowKey={(e) => e.task_id}
      rowHref={(e) => `/tasks/${encode(e.task_id)}`}
      loading={loading}
      sortParam={false}
      skeletonRows={3}
      empty="No executors running"
    />
  )
}

function RawPanel({ title, text, q }: { title: string; text: string | undefined; q: string }) {
  const needle = q.trim().toLowerCase()
  const shown = useMemo(() => {
    if (!text) return ''
    if (!needle) return text.trimEnd()
    return text
      .split('\n')
      .filter((line) => line.toLowerCase().includes(needle))
      .join('\n')
  }, [text, needle])
  return (
    <Panel title={title}>
      {text == null ? (
        <InlineEmpty>Not loaded</InlineEmpty>
      ) : shown ? (
        <pre className="sys-raw">{shown}</pre>
      ) : (
        <InlineEmpty>No lines match</InlineEmpty>
      )}
    </Panel>
  )
}
