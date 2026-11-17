import { useMemo, useState } from 'react'
import { SlidersHorizontal, X } from 'lucide-react'
import { ErrorStrip, FilterChips, SearchInput, useNow, usePageTitle } from '@/components/shared'
import { RunSummary, RunTable, useDeadLetterMap } from '@/components/runs'
import { useDayRuns, useItems, useRuns } from '@/lib/api'
import type { RunRow, WorkItem } from '@/lib/types'
import { fmtTokens, itemName, timeMs } from '@/lib/format'
import { useUrlParams, useUrlState } from '@/lib/url'
import '@/styles/runs.css'

const SINCE: Record<string, { label: string; ms: number }> = {
  '1h': { label: 'Last hour', ms: 3600e3 },
  '24h': { label: 'Last 24h', ms: 86400e3 },
  '7d': { label: 'Last 7 days', ms: 7 * 86400e3 },
  '30d': { label: 'Last 30 days', ms: 30 * 86400e3 },
}
const STATUS_ALIASES: Record<string, string[]> = {
  error: ['execution_error', 'dead_letter'],
  failed: ['execution_error', 'dead_letter'],
  errors: ['execution_error'],
}
const LIMIT = 1000

export function Runs() {
  usePageTitle('Runs')
  const [params, update] = useUrlParams()
  const [q, setQ] = useUrlState('q')
  const [filtersOpen, setFiltersOpen] = useState(false)
  const source = params.get('source') || ''
  const since = SINCE[params.get('since') || ''] ? params.get('since')! : ''
  const itemFilter = params.get('item') || ''
  const statuses = useMemo(
    () =>
      (params.get('status') || '')
        .split(',')
        .flatMap((s) => STATUS_ALIASES[s] || [s])
        .filter(Boolean),
    [params],
  )

  // Bucket `since` to the minute so the query key (and the request) stays stable between renders.
  const minute = Math.floor(useNow(60000) / 60000) * 60000
  const sinceIso = since ? new Date(minute - SINCE[since].ms).toISOString() : undefined
  const runsQuery = useRuns({ limit: LIMIT, since: sinceIso })
  const itemsQuery = useItems()
  const dayRuns = useDayRuns().data?.runs
  const dayTokens = dayRuns?.reduce((sum, r) => sum + (r.tokens?.total_tokens || 0), 0)
  const [deadLetters, dlQuery] = useDeadLetterMap()

  const items = useMemo(
    () => new Map((itemsQuery.data?.items || []).map((item) => [item.work_item_id, item])),
    [itemsQuery.data],
  )
  const all = runsQuery.data?.runs
  const live = itemsQuery.data?.active_executors || []

  const scoped = useMemo(
    () =>
      (all || []).filter(
        (r) => (!itemFilter || r.work_item_id === itemFilter) && matches(r, q, items),
      ),
    [all, itemFilter, q, items],
  )
  const liveScoped = live.filter(
    (e) =>
      (!itemFilter || e.work_item_id === itemFilter) &&
      (!source || e.source === source) &&
      (!since || timeMs(e.started_at) >= minute - SINCE[since].ms) &&
      matchesText(
        [
          e.preview,
          e.task_id,
          e.work_item_id && items.get(e.work_item_id) && itemName(items.get(e.work_item_id)!),
        ],
        q,
      ),
  )
  const bySource = scoped.filter((r) => !source || r.source === source)
  const visible = bySource.filter((r) => !statuses.length || statuses.includes(r.result_status))
  const showLive = !statuses.length || statuses.includes('live')

  const count = (status: string) => bySource.filter((r) => r.result_status === status).length
  const sourceCounts = useMemo(() => {
    const counts = new Map<string, number>()
    for (const r of scoped) counts.set(r.source, (counts.get(r.source) || 0) + 1)
    return [...counts].sort((a, b) => b[1] - a[1])
  }, [scoped])

  const filtered = !!(q || source || since || itemFilter || statuses.length)
  const clear = () =>
    update({ q: null, source: null, since: null, item: null, status: null }, { replace: false })

  function toggleStatus(value: string) {
    if (!value) return update({ status: null })
    const next = statuses.includes(value)
      ? statuses.filter((s) => s !== value)
      : [...statuses, value]
    update({ status: next.join(',') || null })
  }

  const itemOf = itemFilter ? items.get(itemFilter) : undefined
  return (
    <div className="runs-page">
      <ErrorStrip query={runsQuery} label="Worker" />
      {!runsQuery.error && <ErrorStrip query={dlQuery} label="Dead letters" tone="warn" />}
      <div className="toolbar runs-toolbar">
        <FilterChips
          className="chip-strip"
          label="Run status"
          value={statuses.length ? statuses : ['']}
          onChange={toggleStatus}
          options={[
            { value: '', label: 'All', count: bySource.length + liveScoped.length },
            { value: 'live', label: 'Live', count: liveScoped.length, tone: 'live' },
            { value: 'success', label: 'Success', count: count('success') },
            {
              value: 'execution_error',
              label: 'Failed',
              count: count('execution_error'),
              tone: 'warn',
            },
            {
              value: 'dead_letter',
              label: 'Dead letters',
              count: count('dead_letter'),
              tone: 'danger',
            },
          ]}
        />
        <button
          type="button"
          className="filter-chip filters-toggle"
          aria-expanded={filtersOpen}
          aria-controls="runs-selects"
          onClick={() => setFiltersOpen(!filtersOpen)}
        >
          <SlidersHorizontal size={14} aria-hidden />
          Filters
          {(source ? 1 : 0) + (since ? 1 : 0) > 0 && (
            <span className="chip-count">{(source ? 1 : 0) + (since ? 1 : 0)}</span>
          )}
        </button>
        <div className="toolbar-selects" id="runs-selects" data-open={filtersOpen || undefined}>
          <select
            className="toolbar-select"
            aria-label="Source"
            value={source}
            onChange={(e) => update({ source: e.target.value || null })}
          >
            <option value="">All sources</option>
            {sourceCounts.map(([value, n]) => (
              <option key={value} value={value}>
                {value} ({n})
              </option>
            ))}
            {source && !sourceCounts.some(([value]) => value === source) && (
              <option value={source}>{source} (0)</option>
            )}
          </select>
          <select
            className="toolbar-select"
            aria-label="Time range"
            value={since}
            onChange={(e) => update({ since: e.target.value || null })}
          >
            <option value="">All time</option>
            {Object.entries(SINCE).map(([value, { label }]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </div>
        {itemFilter && (
          <button
            type="button"
            className="filter-chip runs-item-chip"
            aria-pressed
            onClick={() => update({ item: null })}
            title="Remove item filter"
          >
            <span className="truncate">Item: {itemOf ? itemName(itemOf) : itemFilter}</span>
            <X size={12} aria-hidden />
          </button>
        )}
        <SearchInput
          className="toolbar-end runs-search"
          value={q}
          onChange={(v) => setQ(v, { replace: true })}
          placeholder="Search runs…"
        />
      </div>
      <div className="runs-meta">
        <RunSummary
          runs={visible}
          after={[
            {
              label: 'Tokens 24h',
              value: dayTokens ? fmtTokens(dayTokens) : '—',
              mono: true,
              title: 'All runs in the last 24 hours, regardless of filters',
            },
          ]}
        />
        {runsQuery.data?.truncated && (
          <span className="runs-truncated" data-tone="warn">
            Showing the latest {LIMIT}; narrow the time range for older runs
          </span>
        )}
      </div>
      <section className="panel">
        <RunTable
          label="Runs"
          runs={all ? visible : undefined}
          live={showLive ? liveScoped : []}
          deadLetters={deadLetters}
          items={items}
          loading={runsQuery.isPending}
          empty={
            filtered ? (
              <>
                No runs match ·{' '}
                <button type="button" onClick={clear}>
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

function matchesText(values: (string | null | undefined | false)[], q: string) {
  if (!q) return true
  const needle = q.toLowerCase()
  return values.some((v) => v && v.toLowerCase().includes(needle))
}

function matches(run: RunRow, q: string, items: Map<string, WorkItem>) {
  if (!q) return true
  const item = run.work_item_id ? items.get(run.work_item_id) : undefined
  return matchesText(
    [run.preview, run.run_id, run.task_id, run.work_item_id, run.model, item && itemName(item)],
    q,
  )
}
