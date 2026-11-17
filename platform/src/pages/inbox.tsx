import { useMemo } from 'react'
import { ChevronDown, ChevronUp, ExternalLink } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import {
  ErrorStrip,
  ErrorText,
  FactsRow,
  FilterChips,
  InlineEmpty,
  Panel,
  SearchInput,
  StatusBadge,
  shortAge,
  useNow,
  usePageTitle,
} from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { ApiError, useInbox } from '@/lib/api'
import { fmtWhen, safeMailLink } from '@/lib/format'
import { isEditableTarget } from '@/lib/keys'
import { parseSort, sortRows, useUrlParams, useUrlState } from '@/lib/url'
import type { InboxRow } from '@/lib/types'
import '@/styles/inbox.css'

const CATEGORIES = ['waiting', 'review', 'action', 'order', 'news'] as const
const DEFAULT_SORT = '-received'

interface Row extends InboxRow {
  key: string
  ms: number
  confidence: number | null
}

function received(ms: number, now: number) {
  return Number.isFinite(ms) ? fmtWhen(ms, { now }) : '—'
}
function Header({ long, short }: { long: string; short: string }) {
  return (
    <>
      <span className="h-long">{long}</span>
      <span className="h-short" aria-hidden>
        {short}
      </span>
    </>
  )
}
function fullDate(ms: number) {
  return Number.isFinite(ms)
    ? new Date(ms).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' })
    : undefined
}
function score(value: string) {
  const n = value ? Number(value) : NaN
  return Number.isFinite(n) ? n : null
}
function reason(row: Row, now: number) {
  if (!row.unclassified) return row.why || 'No reason recorded'
  const age = Number.isFinite(row.ms) ? ` · ${shortAge(now - row.ms)}` : ''
  return `${row.why || 'Awaiting classification'}${age}`
}

export function InboxPage() {
  usePageTitle('Inbox')
  const query = useInbox()
  const now = useNow(30000)
  const [params, update] = useUrlParams()
  const [q, setQ] = useUrlState('q')
  const rawCategory = params.get('category') || ''
  const category = (CATEGORIES as readonly string[]).includes(rawCategory) ? rawCategory : ''
  const data = query.data
  const threshold = score(data?.threshold || '')

  const rows = useMemo<Row[]>(() => {
    // The API has no row ids; received_at plus an occurrence counter is stable across polls.
    const seen = new Map<string, number>()
    return (data?.rows || []).map((row) => {
      const n = seen.get(row.received_at) || 0
      seen.set(row.received_at, n + 1)
      return {
        ...row,
        key: n ? `${row.received_at}~${n}` : row.received_at,
        ms: new Date(row.received_at).getTime(),
        confidence: score(row.score),
      }
    })
  }, [data])

  const needle = q.trim().toLowerCase()
  const searched = needle
    ? rows.filter((row) =>
        `${row.subject} ${row.sender} ${row.why} ${row.decision}`.toLowerCase().includes(needle),
      )
    : rows
  const visible = category ? searched.filter((row) => row.decision === category) : searched
  const count = (value: string) => searched.filter((row) => row.decision === value).length

  const columns: Column<Row>[] = [
    {
      key: 'received',
      header: <Header long="Received" short="Time" />,
      width: 104,
      narrowWidth: 84,
      sortValue: (row) => row.ms,
      sortDescFirst: true,
      className: 'num',
      cell: (row) => received(row.ms, now),
      title: (row) => fullDate(row.ms),
    },
    {
      key: 'sender',
      header: 'Sender',
      width: '20%',
      priority: 2,
      sortValue: (row) => row.sender,
      cell: (row) => <span className="truncate inbox-sender">{row.sender || '—'}</span>,
      title: (row) => row.sender,
    },
    {
      key: 'subject',
      header: 'Subject',
      width: '30%',
      narrowWidth: 'auto',
      cell: (row) => <span className="truncate cell-primary">{row.subject || '(No subject)'}</span>,
      title: (row) => row.subject,
    },
    {
      key: 'decision',
      header: <Header long="Decision" short="Kind" />,
      width: 104,
      narrowWidth: 88,
      sortValue: (row) => row.decision,
      cell: (row) => <StatusBadge status={row.decision} kind="inbox" />,
    },
    {
      key: 'confidence',
      header: <Header long="Conf." short="Conf" />,
      width: 76,
      narrowWidth: 68,
      align: 'right',
      sortValue: (row) => row.confidence,
      sortDescFirst: true,
      cell: (row) => <Confidence value={row.confidence} threshold={threshold} />,
    },
    {
      key: 'reason',
      header: 'Reason',
      priority: 3,
      cell: (row) => (
        <span className={row.unclassified ? 'truncate muted' : 'truncate inbox-reason'}>
          {reason(row, now)}
        </span>
      ),
      title: (row) => reason(row, now),
    },
  ]

  const sort = parseSort(params.get('sort') || DEFAULT_SORT)
  const sortColumn = columns.find((c) => c.key === sort?.key)
  const ordered = sortColumn?.sortValue
    ? sortRows(visible, sortColumn.sortValue, !!sort?.desc)
    : visible
  const selectedKey = params.get('row')
  const selected = selectedKey ? rows.find((row) => row.key === selectedKey) : undefined
  const select = (row: Row | undefined) => row && update({ row: row.key }, { replace: true })

  const missing = query.error instanceof ApiError && query.error.status === 404
  if (missing)
    return (
      <div className="inbox-page">
        <div className="inbox-missing" role="status">
          <span>
            Personal inbox is not configured. Enable it in the handler configuration with a handler
            version that has the decisions API.
          </span>
          <button type="button" className="text-link" onClick={() => void query.refetch()}>
            Check again
          </button>
        </div>
      </div>
    )

  const waiting = data?.awaiting ?? null
  const stuck = data?.stuck ?? null
  const deadLetters = data ? Number(data.dead_letters) || 0 : null
  const filtered = !!(needle || category)

  return (
    <div className="inbox-page">
      <ErrorStrip query={query} label="Inbox" />
      <FactsRow
        className="inbox-facts"
        items={[
          {
            label: 'Mode',
            value: data ? (
              <StatusBadge status={data.dry_run ? 'preview' : 'active'}>
                {data.dry_run ? 'Dry run' : 'Live'}
              </StatusBadge>
            ) : null,
            title: data
              ? data.dry_run
                ? 'Dry run: decisions are recorded but no mailbox changes are made'
                : 'Decisions are applied to the mailbox'
              : undefined,
          },
          { label: 'Threshold', value: threshold?.toFixed(2) ?? null, priority: 2 },
          {
            label: 'Waiting',
            value:
              waiting == null
                ? null
                : waiting && data?.oldest_wait
                  ? `${waiting} (oldest ${data.oldest_wait})`
                  : waiting,
            tone: waiting ? 'pending' : undefined,
          },
          { label: 'Stuck', value: stuck, tone: stuck ? 'warn' : undefined },
          {
            label: 'Dead letters',
            value: deadLetters,
            tone: deadLetters ? 'danger' : undefined,
            priority: deadLetters ? undefined : 2,
          },
          {
            label: 'Last error',
            value: data ? data.last_error ? <ErrorText text={data.last_error} /> : '—' : null,
            priority: 2,
          },
        ]}
      />
      <div className="toolbar inbox-toolbar">
        <FilterChips
          className="chip-strip"
          label="Decision"
          value={category}
          onChange={(value) => update({ category: value === category ? null : value || null })}
          options={[
            { value: '', label: 'All', count: data ? searched.length : null },
            ...CATEGORIES.map((value) => ({
              value,
              label: value[0].toUpperCase() + value.slice(1),
              count: data ? count(value) : null,
              tone:
                value === 'waiting'
                  ? ('pending' as const)
                  : value === 'review'
                    ? ('warn' as const)
                    : undefined,
            })),
          ]}
        />
        <div className="toolbar-end">
          {data && (
            <span className="inbox-count num">
              {visible.length === rows.length
                ? `${rows.length} decisions`
                : `${visible.length} of ${rows.length}`}
            </span>
          )}
          <SearchInput
            className="inbox-search"
            value={q}
            onChange={(value) => setQ(value, { replace: true })}
            placeholder="Search sender, subject, reason…"
          />
        </div>
      </div>
      <Panel className="inbox-table">
        <DataTable
          label="Inbox decisions"
          columns={columns}
          rows={data ? visible : undefined}
          rowKey={(row) => row.key}
          loading={query.isPending}
          defaultSort={DEFAULT_SORT}
          selectedKey={selectedKey}
          onRowClick={select}
          rowLabel={(row) =>
            `${row.subject || 'No subject'}, from ${row.sender || 'unknown sender'}, ${row.decision}`
          }
          rowVersion={(row) => `${row.decision}|${row.score}|${row.why}`}
          skeletonRows={12}
          empty={
            filtered ? (
              <>
                No decisions match ·{' '}
                <button
                  type="button"
                  className="text-link"
                  onClick={() => update({ q: null, category: null })}
                >
                  Clear filters
                </button>
              </>
            ) : (
              'No decisions yet'
            )
          }
        />
      </Panel>
      <DecisionSheet
        row={selected}
        rows={ordered}
        threshold={threshold}
        now={now}
        dryRun={data?.dry_run}
        onSelect={select}
        onClose={() => update({ row: null }, { replace: true })}
        missing={!!selectedKey && !!data && !selected}
      />
    </div>
  )
}

function Confidence({ value, threshold }: { value: number | null; threshold: number | null }) {
  if (value == null) return <span className="muted">—</span>
  const low = threshold != null && value < threshold
  return (
    <span
      className="num inbox-conf"
      data-tone={low ? 'warn' : undefined}
      title={low ? `Below the ${threshold!.toFixed(2)} threshold` : undefined}
    >
      {value.toFixed(2)}
    </span>
  )
}

function DecisionSheet({
  row,
  rows,
  threshold,
  now,
  dryRun,
  onSelect,
  onClose,
  missing,
}: {
  row: Row | undefined
  rows: Row[]
  threshold: number | null
  now: number
  dryRun?: boolean
  onSelect: (row: Row | undefined) => void
  onClose: () => void
  missing: boolean
}) {
  const index = row ? rows.findIndex((r) => r.key === row.key) : -1
  const prev = index > 0 ? rows[index - 1] : undefined
  const next = index >= 0 && index < rows.length - 1 ? rows[index + 1] : undefined
  const link = row ? safeMailLink(row.link) : undefined
  const open = !!row || missing
  return (
    <Sheet open={open} onOpenChange={(value) => !value && onClose()}>
      {open && (
        <SheetContent
          title={row ? row.subject || '(No subject)' : 'Decision'}
          description={
            row ? `${row.sender || 'Unknown sender'} · ${fullDate(row.ms) || row.received}` : ''
          }
          onKeyDown={(event) => {
            if (isEditableTarget(event.target)) return
            if (event.key === 'j' || event.key === 'ArrowDown') onSelect(next)
            else if (event.key === 'k' || event.key === 'ArrowUp') onSelect(prev)
            else return
            event.preventDefault()
          }}
          actions={
            row && (
              <div className="inbox-sheet-nav">
                <button
                  type="button"
                  className="icon-button"
                  aria-label="Previous decision (k)"
                  title="Previous (k)"
                  disabled={!prev}
                  onClick={() => onSelect(prev)}
                >
                  <ChevronUp />
                </button>
                <button
                  type="button"
                  className="icon-button"
                  aria-label="Next decision (j)"
                  title="Next (j)"
                  disabled={!next}
                  onClick={() => onSelect(next)}
                >
                  <ChevronDown />
                </button>
              </div>
            )
          }
          footer={
            row && (
              <>
                {index >= 0 && (
                  <span className="inbox-sheet-pos num">
                    {index + 1} / {rows.length}
                  </span>
                )}
                <Button variant="outline" size="sm" onClick={onClose}>
                  Close
                </Button>
                {link ? (
                  <Button asChild size="sm">
                    <a href={link} target="_blank" rel="noopener noreferrer">
                      Open message
                      <ExternalLink />
                    </a>
                  </Button>
                ) : (
                  <span className="muted inbox-no-link">No message link recorded</span>
                )}
              </>
            )
          }
        >
          {!row ? (
            <InlineEmpty>This decision is no longer in the inbox list</InlineEmpty>
          ) : (
            <div className="inbox-sheet">
              <FactsRow
                items={[
                  {
                    label: 'Decision',
                    value: <StatusBadge status={row.decision} kind="inbox" />,
                  },
                  {
                    label: 'Confidence',
                    value: row.confidence == null ? '—' : row.confidence.toFixed(2),
                    tone:
                      row.confidence != null && threshold != null && row.confidence < threshold
                        ? 'warn'
                        : undefined,
                  },
                  { label: 'Threshold', value: threshold?.toFixed(2) ?? '—' },
                  {
                    label: 'Waiting',
                    value: Number.isFinite(row.ms) ? shortAge(now - row.ms) : '—',
                    tone: 'pending',
                    hidden: !row.unclassified,
                  },
                  { label: 'Mode', value: dryRun ? 'dry-run' : 'live', hidden: dryRun == null },
                ]}
              />
              <section>
                <h3>Reason</h3>
                <p className={row.why ? undefined : 'muted'}>
                  {row.why ||
                    (row.unclassified ? 'Awaiting classification.' : 'No reason recorded.')}
                </p>
              </section>
              <section>
                <h3>Message</h3>
                <dl className="inbox-kv">
                  <dt>From</dt>
                  <dd>{row.sender || '—'}</dd>
                  <dt>Subject</dt>
                  <dd>{row.subject || '(No subject)'}</dd>
                  <dt>Received</dt>
                  <dd className="num">{fullDate(row.ms) || row.received}</dd>
                  <dt>Link</dt>
                  <dd className="mono">{link || row.link || '—'}</dd>
                </dl>
              </section>
            </div>
          )}
        </SheetContent>
      )}
    </Sheet>
  )
}
