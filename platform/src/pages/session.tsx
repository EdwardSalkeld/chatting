import { memo, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Link, useLocation, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  AlertTriangle,
  ArrowDown,
  Check,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Circle,
  Copy,
  MessageSquare,
  Pause,
  Play,
  Send,
  XCircle,
} from 'lucide-react'
import {
  ErrorStrip,
  FactsRow,
  FilterChips,
  InlineEmpty,
  SearchInput,
  SkeletonRows,
  StatusBadge,
  useNow,
  usePageTitle,
  type Fact,
} from '@/components/shared'
import { api, useItems } from '@/lib/api'
import { encode, itemName } from '@/lib/format'
import { isEditableTarget } from '@/lib/keys'
import {
  ENTRY_KINDS,
  entryText,
  formatShow,
  matchesFind,
  parseShow,
  transcript,
  type EntryKind,
  type SessionMeta,
  type TranscriptEntry,
} from '@/lib/transcript'
import type { ActivityEvent, EventsSnapshot, RunHeader } from '@/lib/types'
import { useUrlParams } from '@/lib/url'
import '@/styles/session.css'

const OUTPUT_LINES = 12
// Shed on phones so the facts row stays at about two lines.
const SECONDARY_FACTS = new Set(['Item', 'Backend', 'Messages', 'Finished', 'Task', 'Tier'])
const FOLLOW_SLACK = 80

export function Session() {
  const { id = '' } = useParams()
  return <SessionView key={id} id={id} />
}

const pad = (n: number) => String(n).padStart(2, '0')
function clock(value?: string | null) {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime())
    ? value
    : `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}
function dayKey(value: string) {
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? '' : d.toDateString()
}
function dayLabel(value: string) {
  return new Date(value).toLocaleDateString(undefined, {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
  })
}
function stamp(value?: string | null) {
  if (!value) return '—'
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  const today = d.toDateString() === new Date().toDateString()
  return today
    ? clock(value)
    : `${d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' })} ${clock(value)}`
}
function span(ms?: number | null) {
  if (ms == null || !Number.isFinite(ms)) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  const s = Math.round(ms / 1000)
  if (s < 3600) return `${Math.floor(s / 60)}m ${pad(s % 60)}s`
  return `${Math.floor(s / 3600)}h ${pad(Math.floor((s % 3600) / 60))}m`
}
const compact = new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 })
const full = new Intl.NumberFormat()
const tk = (n: number) => (n < 10_000 ? full.format(n) : compact.format(n))

function highlight(text: string, needle: string): ReactNode {
  if (!needle || !text) return text
  const lower = text.toLowerCase(),
    n = needle.toLowerCase()
  const parts: ReactNode[] = []
  let from = 0,
    at = lower.indexOf(n)
  while (at >= 0) {
    if (at > from) parts.push(text.slice(from, at))
    parts.push(<mark key={at}>{text.slice(at, at + n.length)}</mark>)
    from = at + n.length
    at = lower.indexOf(n, from)
  }
  if (!parts.length) return text
  parts.push(text.slice(from))
  return parts
}

function useEvents(taskID: string | null | undefined, paused: boolean, initialActive: boolean) {
  const [events, setEvents] = useState<ActivityEvent[]>([]),
    [active, setActive] = useState(initialActive),
    [error, setError] = useState<Error | null>(null),
    [loading, setLoading] = useState(true),
    [updatedAt, setUpdatedAt] = useState(0),
    [retry, setRetry] = useState(0)
  const cursor = useRef(0)
  useEffect(() => {
    if (!taskID || paused) return
    let cancelled = false,
      timer: ReturnType<typeof setTimeout>
    const controller = new AbortController()
    async function poll() {
      try {
        const data = await api<EventsSnapshot>(
          `worker/tasks/${encode(taskID!)}/events?after=${cursor.current}`,
          { signal: controller.signal },
        )
        if (cancelled) return
        setError(null)
        setLoading(false)
        setActive(data.active)
        setUpdatedAt(Date.now())
        if (data.events.length) {
          cursor.current = Math.max(
            cursor.current,
            ...data.events.map((event) => event.activity_id),
          )
          setEvents((previous) => {
            const seen = new Set(previous.map((event) => event.activity_id))
            return [...previous, ...data.events.filter((event) => !seen.has(event.activity_id))]
          })
        }
        if (data.active || data.events.length >= 100)
          timer = setTimeout(() => void poll(), data.events.length >= 100 ? 0 : 1000)
      } catch (e) {
        if (cancelled) return
        setLoading(false)
        setError(e instanceof Error ? e : Error('Could not load events.'))
        timer = setTimeout(() => void poll(), 3000)
      }
    }
    void poll()
    return () => {
      cancelled = true
      controller.abort()
      clearTimeout(timer)
    }
  }, [taskID, paused, retry])
  return {
    events,
    active,
    error,
    loading: loading && !!taskID,
    updatedAt,
    retry: () => setRetry((v) => v + 1),
  }
}

function SessionView({ id }: { id: string }) {
  const completed = useLocation().pathname.startsWith('/runs/')
  const [params, update] = useUrlParams()
  const show = useMemo(() => parseShow(params.get('show')), [params])
  const find = params.get('find') || ''
  const [paused, setPaused] = useState(false)
  const runHeader = useQuery({
    queryKey: ['run', id],
    queryFn: () => api<RunHeader>(`worker/runs/${encode(id)}`),
    enabled: completed,
  })
  const taskID = completed ? runHeader.data?.task_id : id
  const stream = useEvents(taskID, paused, !completed)
  const { entries, meta } = useMemo(
    () => transcript(stream.events, !stream.active),
    [stream.events, stream.active],
  )
  const runID = completed ? id : meta.runID
  const lateHeader = useQuery({
    queryKey: ['run', runID],
    queryFn: () => api<RunHeader>(`worker/runs/${encode(runID!)}`),
    enabled: !completed && !!runID,
  })
  const header = completed ? runHeader.data : lateHeader.data
  const items = useItems()
  const workItemID =
    header?.work_item_id ||
    items.data?.active_executors.find((e) => e.task_id === taskID)?.work_item_id ||
    undefined
  const item = items.data?.items.find((i) => i.work_item_id === workItemID)
  usePageTitle(item ? itemName(item) : workItemID || 'Session', runID || taskID || id)

  const searched = useMemo(() => entries.filter((e) => matchesFind(e, find)), [entries, find])
  const counts = useMemo(() => {
    const c: Record<EntryKind, number> = {
      assistant: 0,
      command: 0,
      system: 0,
      delivery: 0,
      error: 0,
    }
    for (const e of searched) c[e.kind]++
    return c
  }, [searched])
  const visible = useMemo(() => searched.filter((e) => show.has(e.kind)), [searched, show])
  const errorCount = useMemo(() => entries.filter((e) => e.kind === 'error').length, [entries])

  const toggleKind = (kind: string) => {
    const next = new Set(show)
    if (next.has(kind as EntryKind)) next.delete(kind as EntryKind)
    else next.add(kind as EntryKind)
    update({ show: formatShow(next) })
  }

  const [following, setFollowing] = useState(!completed)
  const [atBottom, setAtBottom] = useState(true)
  const followRef = useRef(following)
  followRef.current = following
  const measure = useCallback(() => {
    const gap = document.documentElement.scrollHeight - window.scrollY - window.innerHeight
    setAtBottom(gap < FOLLOW_SLACK)
    return gap < FOLLOW_SLACK
  }, [])
  const scrollToEnd = useCallback(() => {
    window.scrollTo({ top: document.documentElement.scrollHeight })
  }, [])
  useEffect(() => {
    if (followRef.current) scrollToEnd()
    measure()
  }, [visible, scrollToEnd, measure])
  useEffect(() => {
    let last = window.scrollY
    function onScroll() {
      const bottom = measure()
      if (window.scrollY < last - 4 && !bottom && followRef.current) setFollowing(false)
      last = window.scrollY
    }
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [measure])
  const [mark, setMark] = useState(0)
  useEffect(() => {
    if (following || atBottom) setMark(visible.length)
  }, [following, atBottom, visible.length])
  const newCount = stream.active && !following ? Math.max(0, visible.length - mark) : 0
  const follow = useCallback(
    (on: boolean) => {
      setFollowing(on)
      if (on) scrollToEnd()
    },
    [scrollToEnd],
  )

  const jumpError = useCallback(
    (step: 1 | -1) => {
      const nodes = Array.from(document.querySelectorAll<HTMLElement>('.t-log [data-kind="error"]'))
      if (!nodes.length) {
        if (errorCount && !show.has('error'))
          update({ show: formatShow(new Set([...show, 'error' as const])) })
        return
      }
      const current = nodes.indexOf(document.activeElement as HTMLElement)
      const edge = document.querySelector('.t-toolbar')?.getBoundingClientRect().bottom ?? 0
      let next: number
      if (current >= 0) next = (current + step + nodes.length) % nodes.length
      else if (step === 1) next = nodes.findIndex((n) => n.getBoundingClientRect().top > edge + 1)
      else next = nodes.findLastIndex((n) => n.getBoundingClientRect().top < edge - 1)
      if (next < 0) next = step === 1 ? 0 : nodes.length - 1
      setFollowing(false)
      nodes[next].scrollIntoView({ block: 'center' })
      nodes[next].focus({ preventScroll: true })
    },
    [errorCount, show, update],
  )

  const [copied, setCopied] = useState<'' | 'ok' | 'fail'>('')
  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(
        visible.map((e) => `${clock(e.at)}  ${entryText(e)}`).join('\n'),
      )
      setCopied('ok')
    } catch {
      setCopied('fail')
    }
    setTimeout(() => setCopied(''), 1500)
  }, [visible])

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      if (event.metaKey || event.ctrlKey || event.altKey || event.defaultPrevented) return
      if (isEditableTarget(event.target)) return
      if (document.querySelector('[role="dialog"][data-state="open"], [role="alertdialog"]')) return
      const key = event.key
      if (key === 'f') follow(!followRef.current)
      else if (key === 'p') setPaused((v) => !v)
      else if (key === 'e' || key === 'E') jumpError(key === 'E' ? -1 : 1)
      else if (key === 'c') void copy()
      else if (key === 'Escape' && followRef.current) setFollowing(false)
      else return
      event.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [follow, jumpError, copy])

  const [expanded, setExpanded] = useState<Record<string, 'open' | 'full' | 'closed'>>({})
  const setEntryState = useCallback(
    (key: string, state: 'open' | 'full' | 'closed') =>
      setExpanded((prev) => ({ ...prev, [key]: state })),
    [],
  )

  if (completed && runHeader.isPending) return <SkeletonRows rows={8} />
  if (completed && runHeader.isError && !runHeader.data)
    return <ErrorStrip query={runHeader} label="Run" />

  const live = stream.active && !paused
  const status = header?.status || (stream.active ? 'live' : meta.finishedStatus || 'finished')
  const request =
    header?.request ||
    (stream.events.find((e) => e.phase === 'task_received')?.detail.request as string | undefined)
  const startedAt = header?.started_at || stream.events[0]?.occurred_at
  const kinds = ENTRY_KINDS.map(({ kind, label }) => ({
    value: kind,
    label,
    count: counts[kind],
    tone: kind === 'error' ? ('danger' as const) : undefined,
  }))
  const everything = show.size === ENTRY_KINDS.length && !find

  return (
    <div className="session">
      <SessionHeader
        header={header}
        meta={meta}
        status={status}
        active={stream.active}
        startedAt={startedAt}
        workItemID={workItemID}
        itemLabel={item ? itemName(item) : workItemID}
        taskID={taskID}
      />
      {request && <RequestLine text={request} />}
      <div className="t-toolbar">
        <FilterChips
          className="t-chips"
          label="Event types"
          options={kinds}
          value={[...show]}
          onChange={toggleKind}
        />
        <SearchInput
          className="t-find"
          value={find}
          onChange={(v) => update({ find: v }, { replace: true })}
          placeholder="Find in transcript"
        />
        <div className="t-actions">
          {errorCount > 0 && (
            <span className="t-errnav" data-tone="danger">
              <button
                type="button"
                onClick={() => jumpError(1)}
                title="Next error (e), previous (shift+e)"
              >
                <XCircle size={14} />
                {errorCount} {errorCount === 1 ? 'error' : 'errors'}
              </button>
              <button type="button" aria-label="Previous error" onClick={() => jumpError(-1)}>
                <ChevronUp size={14} />
              </button>
              <button type="button" aria-label="Next error" onClick={() => jumpError(1)}>
                <ChevronDown size={14} />
              </button>
            </span>
          )}
          {stream.active && (
            <button
              type="button"
              className="t-btn"
              aria-pressed={paused}
              onClick={() => setPaused(!paused)}
              title="Pause polling (p)"
            >
              {paused ? <Play size={14} /> : <Pause size={14} />}
              <span>{paused ? 'Resume' : 'Pause'}</span>
            </button>
          )}
          <button
            type="button"
            className="t-btn"
            aria-pressed={following}
            onClick={() => follow(!following)}
            title="Follow the end of the transcript (f)"
          >
            <ArrowDown size={14} />
            <span>Follow</span>
          </button>
          <button
            type="button"
            className="t-btn"
            onClick={() => void copy()}
            title="Copy visible events (c)"
          >
            {copied === 'ok' ? <Check size={14} /> : <Copy size={14} />}
            <span>{copied === 'ok' ? 'Copied' : copied === 'fail' ? 'Copy failed' : 'Copy'}</span>
          </button>
          <span
            className="t-state"
            data-tone={paused && stream.active ? 'warn' : live ? 'live' : 'neutral'}
          >
            <span
              className="status-dot"
              data-tone={paused && stream.active ? 'warn' : live ? 'live' : 'neutral'}
            />
            {paused && stream.active ? 'Paused' : live ? 'Live' : 'Ended'}
            <span className="num t-count">
              {everything ? entries.length : `${visible.length}/${entries.length}`}
            </span>
          </span>
        </div>
      </div>
      <ErrorStrip
        error={stream.error}
        since={stream.updatedAt || undefined}
        label="Events"
        retry={stream.retry}
      />
      {!taskID ? (
        <InlineEmpty>No task id recorded for this run, so there is no transcript.</InlineEmpty>
      ) : stream.loading ? (
        <SkeletonRows rows={10} />
      ) : !visible.length ? (
        <InlineEmpty
          action={
            !everything && (
              <button
                type="button"
                className="text-link"
                onClick={() => update({ show: null, find: null })}
              >
                Show all
              </button>
            )
          }
        >
          {entries.length
            ? 'No events match'
            : stream.active
              ? 'Waiting for executor output…'
              : 'No events'}
        </InlineEmpty>
      ) : (
        <div className="t-log" role="log" aria-live="polite" aria-label="Session transcript">
          {visible.map((entry, i) => {
            const day = dayKey(entry.at)
            const newDay = i > 0 && day !== dayKey(visible[i - 1].at)
            return (
              <div key={entry.key} className="t-group">
                {newDay && (
                  <div className="t-day" role="separator">
                    <span>{dayLabel(entry.at)}</span>
                  </div>
                )}
                <Entry
                  entry={entry}
                  find={find}
                  state={expanded[entry.key]}
                  setState={setEntryState}
                  showRun={!completed}
                />
              </div>
            )
          })}
        </div>
      )}
      {!stream.active && runID && !completed && (
        <div className="t-end">
          <Link to={`/runs/${encode(runID)}`} className="text-link">
            Open completed run <ChevronRight size={13} />
          </Link>
        </div>
      )}
      {!following && !atBottom && visible.length > 0 && (
        <button type="button" className="t-pill" onClick={() => follow(true)}>
          <ArrowDown size={14} />
          Jump to latest{newCount > 0 && <span className="num"> · {newCount} new</span>}
        </button>
      )}
    </div>
  )
}

function SessionHeader({
  header,
  meta,
  status,
  active,
  startedAt,
  workItemID,
  itemLabel,
  taskID,
}: {
  header?: RunHeader
  meta: SessionMeta
  status: string
  active: boolean
  startedAt?: string
  workItemID?: string
  itemLabel?: string
  taskID?: string | null
}) {
  const now = useNow(1000)
  const elapsed = active && startedAt ? now - Date.parse(startedAt) : header?.duration_ms
  const attempts = header?.attempt_count ?? (meta.attempts || null)
  const decision = header?.auto_model_decision
  const confidence =
    decision?.confidence != null ? Number(decision.confidence).toFixed(2) : undefined
  const tier = header?.model_tier
    ? header.executed_model_tier && header.executed_model_tier !== header.model_tier
      ? `${header.model_tier}→${header.executed_model_tier}${confidence ? ` ${confidence}` : ''}`
      : header.model_tier
    : header?.executed_model_tier
  const t = meta.tokens,
    rr = meta.reroute
  const ctx =
    meta.contextTokens && meta.contextWindow
      ? Math.round((meta.contextTokens / meta.contextWindow) * 100)
      : null
  const facts: Fact[] = [
    { label: 'Status', value: <StatusBadge status={status} kind="run" /> },
    {
      label: 'Item',
      value: workItemID ? (
        <Link className="t-link" to={`/items/${encode(workItemID)}`}>
          {itemLabel || workItemID}
        </Link>
      ) : null,
      hidden: !workItemID,
    },
    { label: 'Source', value: header?.source, hidden: !header?.source },
    { label: active ? 'Elapsed' : 'Duration', value: span(elapsed), hidden: elapsed == null },
    {
      label: 'Attempts',
      value: `${attempts ?? '—'}${header?.max_attempts ? `/${header.max_attempts}` : ''}`,
      tone: attempts && attempts > 1 ? 'warn' : undefined,
      hidden: attempts == null,
    },
    { label: 'Backend', value: header?.executor_backend, hidden: !header?.executor_backend },
    { label: 'Model', mono: true, value: meta.model, hidden: !meta.model },
    {
      label: 'Rerouted',
      mono: true,
      value: rr ? `${rr.from || '?'}→${rr.to || '?'}${rr.count > 1 ? ` ×${rr.count}` : ''}` : null,
      tone: 'warn',
      title: rr?.reason,
      hidden: !rr,
    },
    {
      label: 'Tier',
      value: tier,
      title: decision?.reason ? `Auto decision: ${decision.reason}` : undefined,
      hidden: !tier,
    },
    {
      label: 'Tokens',
      mono: true,
      value: t ? (
        <>
          {tk(t.total)}
          <span className="t-sub">
            {' '}
            in {tk(t.input)} · cached {tk(t.cached)} · out {tk(t.output)}
            {t.reasoning ? ` · reasoning ${tk(t.reasoning)}` : ''}
          </span>
        </>
      ) : null,
      title: t
        ? `input ${full.format(t.input)}, cached ${full.format(t.cached)}, output ${full.format(t.output)}, reasoning ${full.format(t.reasoning)}, total ${full.format(t.total)}`
        : undefined,
      hidden: !t,
    },
    {
      label: 'Context',
      value: ctx != null ? `${ctx}%` : null,
      tone: ctx != null && ctx >= 80 ? 'warn' : undefined,
      title:
        ctx != null
          ? `${full.format(meta.contextTokens!)} of ${full.format(meta.contextWindow!)} tokens`
          : undefined,
      hidden: ctx == null,
    },
    {
      label: 'Messages',
      value: header?.egress_message_count,
      hidden: header?.egress_message_count == null,
    },
    {
      label: 'Escalation',
      value: header?.escalation_reason,
      tone: 'warn',
      hidden: !header?.escalation_reason,
    },
    {
      label: 'Recovery',
      value: 'supervised',
      tone: 'warn',
      hidden: !header?.supervised_recovery_used,
    },
    { label: 'Started', value: stamp(startedAt), title: startedAt, hidden: !startedAt },
    {
      label: 'Finished',
      value: stamp(header?.finished_at),
      title: header?.finished_at,
      hidden: !header?.finished_at,
    },
    {
      label: 'Task',
      mono: true,
      value: taskID,
      hidden: !taskID || !header,
      title: taskID || undefined,
    },
  ]
  const problems = [
    ...(header?.execution_errors || []),
    header?.last_error
      ? `${header.last_error}${header.last_error_stage ? ` · stage ${header.last_error_stage}` : ''}`
      : '',
    header && header.status !== 'success' && header.reason_codes.length
      ? `reason: ${header.reason_codes.join(', ')}`
      : '',
  ].filter(Boolean)
  return (
    <div className="t-head">
      <FactsRow
        className="t-facts"
        items={facts.map((f) => (SECONDARY_FACTS.has(f.label) ? { ...f, priority: 2 } : f))}
      />
      {problems.length > 0 && (
        <div className="t-alert" data-tone="danger" role="alert">
          <AlertTriangle size={14} />
          <span className="mono">{problems.join(' · ')}</span>
        </div>
      )}
    </div>
  )
}

function RequestLine({ text }: { text: string }) {
  const [open, setOpen] = useState(false)
  return (
    <button
      type="button"
      className="t-request"
      aria-expanded={open}
      onClick={() => setOpen(!open)}
      title={open ? 'Collapse' : 'Show the full request'}
    >
      <span className="t-request-label">Request</span>
      <span className="t-request-text" data-open={open || undefined}>
        {text}
      </span>
    </button>
  )
}

const GLYPHS: Record<EntryKind, ReactNode> = {
  assistant: <MessageSquare size={13} />,
  command: <span className="t-dollar">$</span>,
  system: <Circle size={7} />,
  delivery: <Send size={12} />,
  error: <XCircle size={13} />,
}

const Entry = memo(
  function Entry({
    entry,
    find,
    state,
    setState,
    showRun,
  }: {
    entry: TranscriptEntry
    find: string
    state?: 'open' | 'full' | 'closed'
    setState: (key: string, state: 'open' | 'full' | 'closed') => void
    showRun: boolean
  }) {
    const time = clock(entry.at)
    return (
      <div className="t-entry" data-kind={entry.kind} data-tone={entry.tone} tabIndex={-1}>
        <time className="t-time num" dateTime={entry.at} title={new Date(entry.at).toString()}>
          <span className="t-hh">{time.slice(0, 3)}</span>
          {time.slice(3)}
        </time>
        <span className="t-glyph" aria-hidden>
          {GLYPHS[entry.kind]}
        </span>
        <div className="t-body">
          {entry.kind === 'command' && entry.command != null ? (
            <CommandBody entry={entry} find={find} state={state} setState={setState} />
          ) : entry.kind === 'assistant' ? (
            <p className="t-text">{highlight(entry.body, find)}</p>
          ) : (
            <p className="t-line">
              <span className="t-label">{highlight(entry.label, find)}</span>
              {entry.body &&
                (entry.body.includes('\n') || entry.body.length > 160 ? (
                  <span className="t-block mono">{highlight(entry.body, find)}</span>
                ) : (
                  <span className="t-inline">{highlight(entry.body, find)}</span>
                ))}
              {showRun && entry.runID && (
                <Link className="t-link" to={`/runs/${encode(entry.runID)}`}>
                  run
                </Link>
              )}
            </p>
          )}
        </div>
      </div>
    )
  },
  (a, b) =>
    a.find === b.find &&
    a.state === b.state &&
    a.showRun === b.showRun &&
    a.setState === b.setState &&
    a.entry.key === b.entry.key &&
    a.entry.body === b.entry.body &&
    a.entry.running === b.entry.running &&
    a.entry.exit === b.entry.exit,
)

function CommandBody({
  entry,
  find,
  state,
  setState,
}: {
  entry: TranscriptEntry
  find: string
  state?: 'open' | 'full' | 'closed'
  setState: (key: string, state: 'open' | 'full' | 'closed') => void
}) {
  const output = (entry.output || '').replace(/\n+$/, '')
  const outLines = output ? output.split('\n') : []
  const failed = entry.exit != null && entry.exit !== 0
  const matched = !!find && output.toLowerCase().includes(find.toLowerCase())
  const mode = state || (failed || matched ? 'open' : 'closed')
  const shown = mode === 'full' ? outLines : mode === 'open' ? outLines.slice(0, OUTPUT_LINES) : []
  const hidden = outLines.length - shown.length
  return (
    <>
      <div className="t-cmd">
        <code className="t-cmd-text" title={entry.command}>
          {highlight(entry.command || '', find)}
        </code>
        {entry.running ? (
          <span className="t-exit" data-tone="live">
            running
          </span>
        ) : entry.exit != null ? (
          <span className="t-exit" data-tone={failed ? 'danger' : 'neutral'}>
            exit {entry.exit}
          </span>
        ) : null}
        {entry.durationMs != null && entry.durationMs > 0 && (
          <span className="t-dur num">{span(entry.durationMs)}</span>
        )}
        {outLines.length > 0 && (
          <button
            type="button"
            className="t-toggle"
            aria-expanded={mode !== 'closed'}
            onClick={() => setState(entry.key, mode === 'closed' ? 'open' : 'closed')}
          >
            {mode === 'closed' ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
            {outLines.length} {outLines.length === 1 ? 'line' : 'lines'}
          </button>
        )}
      </div>
      {shown.length > 0 && <pre className="t-out">{highlight(shown.join('\n'), find)}</pre>}
      {shown.length > 0 && hidden > 0 && (
        <button type="button" className="t-more" onClick={() => setState(entry.key, 'full')}>
          show {hidden} more
        </button>
      )}
    </>
  )
}
