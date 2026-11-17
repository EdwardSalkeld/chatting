import { useMemo } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { DataTable, type Column } from '@/components/table'
import {
  ErrorStrip,
  FactsRow,
  FilterChips,
  InlineEmpty,
  SearchInput,
  StatusBadge,
  shortAge,
  useNow,
  usePageTitle,
} from '@/components/shared'
import { ApiError, api, useItems, useRuns } from '@/lib/api'
import type { Executor, ItemSnapshot, RunRow, WorkItem } from '@/lib/types'
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
import { useUrlParams, useUrlState } from '@/lib/url'
import {
  RunStatus,
  RunSummary,
  RunTable,
  SourceLabel,
  runError,
  useDeadLetterMap,
} from '@/components/runs'
import '@/styles/runs.css'
import '@/styles/work.css'

interface Row {
  item: WorkItem
  name: string
  live?: Executor
  source: string
  last?: RunRow
  failures: number
  tokens7d: number
}

const WEEK = 7 * 86400e3
const STATUS_RANK = { live: 0, queued: 1, error: 2, idle: 3 } as const
type RowStatus = keyof typeof STATUS_RANK

function rowStatus(row: Row): RowStatus {
  if (row.live || row.item.running) return 'live'
  if (row.item.queued) return 'queued'
  if (isFailure(row.last?.result_status)) return 'error'
  return 'idle'
}
const time = timeMs

function sourceFromReply(item: WorkItem) {
  const type = item.reply?.type
  if (type === 'email' || type === 'internal') return type
  return type ? 'im' : 'internal'
}

function replyText(item: WorkItem) {
  const meta = item.reply?.metadata
  const thread = meta?.message_thread_id != null ? ` #${String(meta.message_thread_id)}` : ''
  return `${item.reply?.type || 'internal'}${thread}`
}

export function WorkItems() {
  usePageTitle('Work items')
  const query = useItems()
  const runsQuery = useRuns({ limit: 1000 })
  const [params, update] = useUrlParams()
  const [q, setQ] = useUrlState('q')
  const filter = params.get('filter') || ''
  const now = useNow(1000)

  const rows = useMemo<Row[] | undefined>(() => {
    if (!query.data) return undefined
    const runsByItem = new Map<string, RunRow[]>()
    for (const run of runsQuery.data?.runs || []) {
      if (!run.work_item_id) continue
      const list = runsByItem.get(run.work_item_id) || []
      list.push(run)
      runsByItem.set(run.work_item_id, list)
    }
    const cutoff = Date.now() - WEEK
    return query.data.items.map((item: WorkItem) => {
      const runs = runsByItem.get(item.work_item_id) || []
      const live = query.data.active_executors.find((e) => e.work_item_id === item.work_item_id)
      const last = runs.reduce<RunRow | undefined>(
        (best, run) => (!best || time(run.created_at) > time(best.created_at) ? run : best),
        undefined,
      )
      return {
        item,
        name: itemName(item),
        live,
        last,
        source: live?.source || last?.source || sourceFromReply(item),
        failures: runs.filter((r) => isFailure(r.result_status)).length,
        tokens7d: runs
          .filter((r) => time(r.created_at) >= cutoff)
          .reduce((sum, r) => sum + (r.tokens?.total_tokens || 0), 0),
      }
    })
  }, [query.data, runsQuery.data])

  const searched = useMemo(() => {
    if (!rows) return undefined
    const needle = q.toLowerCase()
    return rows.filter(
      (row) =>
        !needle ||
        `${row.name} ${row.item.work_item_id} ${row.item.reply?.type} ${row.item.reply?.target} ${row.source}`
          .toLowerCase()
          .includes(needle),
    )
  }, [rows, q])
  const counts = { live: 0, queued: 0, error: 0, idle: 0 }
  for (const row of searched || []) counts[rowStatus(row)]++
  const visible = searched?.filter((row) => !filter || rowStatus(row) === filter)

  const columns: Column<Row>[] = [
    {
      key: 'name',
      header: 'Name',
      sortValue: (r) => r.name,
      cell: (r) => (
        <span className="item-name">
          <Link className="cell-primary truncate" to={`/items/${encode(r.item.work_item_id)}`}>
            {r.name}
          </Link>
          <span className="cell-secondary item-reply" data-priority="3">
            {replyText(r.item)}
          </span>
        </span>
      ),
      title: (r) =>
        `${r.name}\n${r.item.work_item_id} · ${r.item.reply?.type} ${r.item.reply?.target}`,
    },
    {
      key: 'source',
      header: 'Source',
      width: 96,
      priority: 2,
      sortValue: (r) => r.source,
      cell: (r) => <SourceLabel source={r.source} />,
    },
    {
      key: 'status',
      header: 'Status',
      width: 112,
      sortValue: (r) => STATUS_RANK[rowStatus(r)] * 1e13 + (1e13 - time(r.item.last_run_at)),
      cell: (r) => {
        const status = rowStatus(r)
        if (status === 'live')
          return (
            <StatusBadge status="live" kind="run">
              Live{r.live ? ` ${shortAge(now - time(r.live.started_at))}` : ''}
            </StatusBadge>
          )
        if (status === 'queued') return <StatusBadge status="queued" kind="run" />
        if (status === 'error')
          return <RunStatus status={r.last!.result_status} title={runError(r.last!)} />
        return <StatusBadge status="idle" kind="run" />
      },
      title: (r) =>
        r.live ? `Started ${fullTime(r.live.started_at)} · attempt ${r.live.attempt}` : undefined,
    },
    {
      key: 'model',
      header: 'Model',
      width: 76,
      priority: 3,
      sortValue: (r) => r.item.model_tier,
      cell: (r) => r.item.model_tier,
    },
    {
      key: 'runs',
      header: 'Runs',
      width: 68,
      align: 'right',
      priority: 2,
      sortValue: (r) => r.item.run_count,
      sortDescFirst: true,
      cell: (r) => (
        <span className="num">
          {r.item.run_count}
          {r.failures > 0 && (
            <span className="item-failures" title={`${r.failures} failed`}>
              {' '}
              ·{r.failures}
            </span>
          )}
        </span>
      ),
    },
    {
      key: 'queued',
      header: 'Queued',
      width: 80,
      align: 'right',
      priority: 3,
      sortValue: (r) => r.item.queued,
      sortDescFirst: true,
      cell: (r) =>
        r.item.queued ? (
          <span className="num">{r.item.queued}</span>
        ) : (
          <span className="muted">0</span>
        ),
    },
    {
      key: 'last',
      header: 'Last run',
      width: 104,
      sortValue: (r) => time(r.item.last_run_at) || null,
      sortDescFirst: true,
      cell: (r) =>
        r.item.last_run_at ? (
          <time className="num">{shortAge(now - time(r.item.last_run_at))} ago</time>
        ) : (
          <span className="muted">—</span>
        ),
      title: (r) => fullTime(r.item.last_run_at),
    },
    {
      key: 'result',
      header: 'Last result',
      width: 168,
      priority: 2,
      sortValue: (r) => r.last?.result_status,
      cell: (r) =>
        r.last ? (
          <span className="cell-flex">
            <RunStatus status={r.last.result_status} />
            <span className="num muted">{fmtLatency(r.last.latency_ms)}</span>
          </span>
        ) : (
          <span className="muted">—</span>
        ),
    },
    {
      key: 'tokens',
      header: 'Tokens 7d',
      width: 108,
      align: 'right',
      priority: 3,
      mono: true,
      sortValue: (r) => r.tokens7d || null,
      sortDescFirst: true,
      cell: (r) => (r.tokens7d ? fmtTokens(r.tokens7d) : <span className="muted">—</span>),
    },
  ]

  const clear = () => update({ q: null, filter: null })
  return (
    <div className="work-page">
      <ErrorStrip query={query} label="Worker" />
      <div className="toolbar">
        <FilterChips
          label="Work item filter"
          value={filter}
          onChange={(value) => update({ filter: value === filter ? null : value || null })}
          options={[
            { value: '', label: 'All', count: searched?.length ?? null },
            { value: 'live', label: 'Running', count: counts.live, tone: 'live' },
            { value: 'queued', label: 'Queued', count: counts.queued, tone: 'pending' },
            { value: 'error', label: 'Failed', count: counts.error, tone: 'warn' },
            { value: 'idle', label: 'Idle', count: counts.idle },
          ]}
        />
        <SearchInput
          className="toolbar-end work-search"
          value={q}
          onChange={(v) => setQ(v, { replace: true })}
          placeholder="Search work items…"
        />
      </div>
      <section className="panel">
        <DataTable
          label="Work items"
          columns={columns}
          rows={visible}
          rowKey={(r) => r.item.work_item_id}
          rowHref={(r) => `/items/${encode(r.item.work_item_id)}`}
          rowTone={(r) => (rowStatus(r) === 'live' ? 'live' : undefined)}
          rowVersion={(r) => `${rowStatus(r)}:${r.item.run_count}:${r.item.queued}`}
          loading={query.isPending}
          defaultSort="status"
          skeletonRows={12}
          empty={
            q || filter ? (
              <>
                No work items match ·{' '}
                <button type="button" onClick={clear}>
                  Clear filters
                </button>
              </>
            ) : (
              'No work items yet'
            )
          }
        />
      </section>
    </div>
  )
}

export function WorkItemDetail() {
  const { id = '' } = useParams()
  const query = useQuery({
    queryKey: ['item', id],
    queryFn: () => api<ItemSnapshot>(`worker/items/${encode(id)}/runs`),
    refetchInterval: 4000,
    placeholderData: (previous) => previous,
  })
  const [deadLetters] = useDeadLetterMap()
  const [params, update] = useUrlParams()
  const [q, setQ] = useUrlState('q')
  const status = params.get('status') || ''
  const data = query.data
  const item = data?.item
  usePageTitle(item ? itemName(item) : 'Work item', id)

  const runs = data?.runs
  const searched = useMemo(() => {
    if (!runs) return undefined
    const needle = q.toLowerCase()
    return runs.filter(
      (r) =>
        !needle ||
        `${r.preview} ${r.run_id} ${r.task_id} ${r.model || ''}`.toLowerCase().includes(needle),
    )
  }, [runs, q])
  const live = (data?.active_executors || []).filter(
    (e) => !q || `${e.preview} ${e.task_id}`.toLowerCase().includes(q.toLowerCase()),
  )
  const count = (fn: (r: RunRow) => boolean) => (searched || []).filter(fn).length
  const visible = searched?.filter(
    (r) =>
      !status ||
      (status === 'error'
        ? isFailure(r.result_status)
        : status === 'live'
          ? false
          : r.result_status === status),
  )
  const sources = [
    ...new Set([...(data?.active_executors || []), ...(runs || [])].map((r) => r.source)),
  ]
  const tiers = [...new Set((runs || []).map((r) => r.executed_model_tier).filter(Boolean))]
  const models = [...new Set((runs || []).map((r) => r.model).filter(Boolean))]
  const prompts = new Set((runs || []).map((r) => r.preview))
  const sharedPrompt = runs && runs.length > 1 && prompts.size === 1 ? runs[0].preview : undefined
  const lastFailure = runs?.find((r) => isFailure(r.result_status))
  const lastFailureText = lastFailure && (runError(lastFailure) || lastFailure.result_status)
  const notFound = query.error instanceof ApiError && query.error.status === 404

  if (!data)
    return (
      <div className="item-page">
        {!notFound && <ErrorStrip query={query} label="Worker" />}
        {query.isPending ? (
          <section className="panel">
            <RunTable label="Runs" runs={undefined} loading empty="" />
          </section>
        ) : (
          <InlineEmpty action={<Link to="/items">Back to work items</Link>}>
            Work item not found
          </InlineEmpty>
        )}
      </div>
    )

  const meta = data.item.reply?.metadata
  return (
    <div className="item-page">
      <ErrorStrip query={query} label="Worker" />
      <FactsRow
        className="item-facts"
        items={[
          { label: 'reply', value: data.item.reply?.type || 'internal' },
          {
            label: 'target',
            value: data.item.reply?.target,
            mono: true,
            hidden: !data.item.reply?.target,
          },
          {
            label: 'thread',
            value: meta?.message_thread_id != null ? String(meta.message_thread_id) : null,
            mono: true,
            hidden: meta?.message_thread_id == null,
          },
          { label: 'source', value: sources.join(', ') || '—' },
          {
            label: 'model',
            value: [data.item.model_tier, tiers.length ? `ran ${tiers.join('/')}` : '']
              .filter(Boolean)
              .join(' · '),
          },
          { label: 'models', value: models.join(', '), mono: true, hidden: !models.length },
          { label: 'explicit finish', value: data.item.explicit_finish ? 'on' : 'off' },
          {
            label: 'queued',
            value: data.item.queued,
            tone: data.item.queued ? 'pending' : undefined,
          },
          {
            label: 'created',
            value: fmtWhen(item?.created_at),
            title: fullTime(item?.created_at),
            hidden: !item?.created_at,
          },
        ]}
      />
      {(lastFailure || sharedPrompt) && (
        <dl className="facts-row item-facts-2">
          {lastFailure && (
            <div className="item-last-error">
              <dt>last error</dt>
              <dd>
                <Link className="text-link" to={`/runs/${encode(lastFailure.run_id)}`}>
                  {fmtWhen(lastFailure.created_at)}
                </Link>{' '}
                <span className="mono" data-tone="danger" title={lastFailureText}>
                  {lastFailureText}
                </span>
              </dd>
            </div>
          )}
          {sharedPrompt && (
            <div className="item-prompt">
              <dt>prompt</dt>
              <dd title={sharedPrompt}>{sharedPrompt}</dd>
            </div>
          )}
        </dl>
      )}
      <div className="toolbar">
        <FilterChips
          label="Run filter"
          value={status}
          onChange={(value) => update({ status: value === status ? null : value || null })}
          options={[
            { value: '', label: 'Runs', count: (searched?.length || 0) + live.length },
            { value: 'live', label: 'Live', count: live.length, tone: 'live' },
            {
              value: 'success',
              label: 'Success',
              count: count((r) => r.result_status === 'success'),
            },
            {
              value: 'error',
              label: 'Failed',
              count: count((r) => isFailure(r.result_status)),
              tone: 'warn',
            },
          ]}
        />
        <Link className="text-link" to={`/runs?item=${encode(id)}`}>
          Open in Runs
        </Link>
        <SearchInput
          className="toolbar-end work-search"
          value={q}
          onChange={(v) => setQ(v, { replace: true })}
          placeholder="Search runs…"
        />
      </div>
      {runs && runs.length > 0 && <RunSummary runs={visible || []} />}
      <section className="panel">
        <RunTable
          label="Run history"
          runs={visible}
          live={!status || status === 'live' ? live : []}
          showRequest={!sharedPrompt}
          deadLetters={deadLetters}
          empty={
            q || status ? (
              <>
                No runs match ·{' '}
                <button type="button" onClick={() => update({ q: null, status: null })}>
                  Clear filters
                </button>
              </>
            ) : (
              'No runs yet'
            )
          }
        />
      </section>
    </div>
  )
}
