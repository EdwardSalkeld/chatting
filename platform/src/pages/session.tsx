import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  ArrowDown,
  ArrowLeft,
  Check,
  ChevronRight,
  Circle,
  Copy,
  MessageSquare,
  Pause,
  Play,
  Radio,
  Terminal,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Empty, ErrorNotice, Loading, PageHeading, Panel, StatusBadge } from '@/components/shared'
import { api } from '@/lib/api'
import { dateTime, duration, encode } from '@/lib/format'
import { transcript } from '@/lib/transcript'
import type { ActivityEvent, EventsSnapshot, RunHeader } from '@/lib/types'

export function Session() {
  const { id = '' } = useParams(),
    location = useLocation(),
    completed = location.pathname.startsWith('/runs/')
  const header = useQuery({
    queryKey: ['run', id],
    queryFn: () => api<RunHeader>(`worker/runs/${encode(id)}`),
    enabled: completed,
  })
  const taskID = completed ? header.data?.task_id : id
  const [events, setEvents] = useState<ActivityEvent[]>([]),
    [active, setActive] = useState(!completed)
  const [paused, setPaused] = useState(false),
    [delivery, setDelivery] = useState(false),
    [error, setError] = useState<Error | null>(null),
    [loading, setLoading] = useState(true),
    [retry, setRetry] = useState(0)
  const [copied, setCopied] = useState(false),
    [copyError, setCopyError] = useState('')
  const cursor = useRef(0),
    following = useRef(!completed),
    log = useRef<HTMLDivElement>(null)
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
          timer = setTimeout(
            () => {
              void poll()
            },
            data.events.length >= 100 ? 0 : 1000,
          )
      } catch (e) {
        if (cancelled) return
        setLoading(false)
        setError(e instanceof Error ? e : Error('Could not load events.'))
        timer = setTimeout(() => {
          void poll()
        }, 3000)
      }
    }
    void poll()
    return () => {
      cancelled = true
      controller.abort()
      clearTimeout(timer)
    }
  }, [taskID, paused, retry])
  const entries = useMemo(() => transcript(events, delivery, !active), [events, delivery, active])
  useEffect(() => {
    if (following.current && log.current) log.current.scrollTop = log.current.scrollHeight
  }, [entries])
  const request =
    header.data?.request || events.find((event) => event.phase === 'task_received')?.detail.request
  const runID = completed ? id : events.findLast((event) => event.run_id)?.run_id
  async function copy() {
    try {
      await navigator.clipboard.writeText(
        entries.map((entry) => `${entry.label}\n${entry.body}`).join('\n\n'),
      )
      setCopied(true)
      setCopyError('')
    } catch {
      setCopyError('Clipboard unavailable.')
    }
  }
  if (completed && header.isPending) return <Loading />
  if (completed && header.isError)
    return (
      <ErrorNotice
        error={header.error}
        retry={() => {
          void header.refetch()
        }}
      />
    )
  return (
    <>
      <div className="session-breadcrumbs">
        <Link
          className="back-link"
          to={header.data?.work_item_id ? `/items/${encode(header.data.work_item_id)}` : '/items'}
        >
          <ArrowLeft size={15} />
          {header.data?.work_item_id ? 'Work item' : 'Work items'}
        </Link>
        <ChevronRight size={13} />
        <span>{completed ? 'Completed run' : 'Live session'}</span>
      </div>
      <PageHeading
        title={completed ? 'Session transcript' : active ? 'Live session' : 'Session finished'}
        description={taskID || id}
        action={
          <StatusBadge
            status={completed ? header.data?.status || 'unknown' : active ? 'live' : 'finished'}
          />
        }
      />
      {header.data && (
        <div className="detail-facts">
          <div>
            Source <strong>{header.data.source}</strong>
          </div>
          <div>
            Duration <strong>{duration(header.data.duration_ms)}</strong>
          </div>
          <div>
            Attempts <strong>{header.data.attempt_count ?? '—'}</strong>
          </div>
          <div>
            Started <strong>{dateTime(header.data.started_at)}</strong>
          </div>
        </div>
      )}
      {!!header.data?.reason_codes.length && (
        <div className="subtle-notice">Result reasons: {header.data.reason_codes.join(', ')}</div>
      )}
      {request && typeof request === 'string' && (
        <details className="request-panel" open>
          <summary>
            <MessageSquare size={16} />
            Original request
          </summary>
          <p>{request}</p>
        </details>
      )}
      <Panel className="transcript-panel">
        <div className="transcript-toolbar">
          <div>
            <Terminal size={18} />
            <h2>Transcript</h2>
            <span className="event-count">{entries.length} events</span>
          </div>
          <div className="transcript-actions">
            <label className="delivery-toggle">
              <input
                type="checkbox"
                checked={delivery}
                onChange={(e) => setDelivery(e.target.checked)}
              />
              Delivery events
            </label>
            <Button
              variant="ghost"
              size="sm"
              aria-pressed={paused}
              onClick={() => setPaused(!paused)}
            >
              {paused ? <Play /> : <Pause />}
              {paused ? 'Resume' : 'Pause'}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                following.current = true
                if (log.current) log.current.scrollTop = log.current.scrollHeight
              }}
            >
              <ArrowDown />
              Follow
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                void copy()
              }}
            >
              {copied ? <Check /> : <Copy />}
              {copied ? 'Copied' : 'Copy'}
            </Button>
          </div>
        </div>
        <ErrorNotice error={error} retry={() => setRetry((value) => value + 1)} />
        {copyError && (
          <p className="subtle-notice" role="status">
            {copyError}
          </p>
        )}
        {!taskID ? (
          <Empty title="No transcript available" description="No task ID recorded." />
        ) : (
          <div
            className="transcript-log"
            role="log"
            aria-label="Session transcript"
            tabIndex={0}
            ref={log}
            onScroll={() => {
              if (log.current)
                following.current =
                  log.current.scrollHeight - log.current.scrollTop - log.current.clientHeight < 80
            }}
          >
            {loading ? (
              <Loading />
            ) : !entries.length ? (
              <Empty title="No events" description={active ? 'Waiting for output…' : undefined} />
            ) : (
              entries.map((entry, index) => (
                <article className={`transcript-entry ${entry.kind}`} key={index}>
                  <div className="transcript-marker">
                    {entry.kind === 'assistant' ? (
                      <MessageSquare size={14} />
                    ) : entry.kind === 'command' ? (
                      <Terminal size={14} />
                    ) : (
                      <Circle size={9} />
                    )}
                  </div>
                  <div className="transcript-entry-content">
                    <header>
                      <strong>{entry.label}</strong>
                      <time>{dateTime(entry.at)}</time>
                    </header>
                    {entry.body &&
                      (entry.kind === 'command' || entry.kind === 'delivery' ? (
                        <details open={entry.kind !== 'delivery'}>
                          <summary>
                            {entry.kind === 'delivery' ? 'Show sent message' : 'Command & output'}
                          </summary>
                          <pre>{entry.body}</pre>
                        </details>
                      ) : (
                        <p>{entry.body}</p>
                      ))}
                  </div>
                </article>
              ))
            )}
          </div>
        )}
        <div className="transcript-footer">
          <span>
            {paused ? <Pause size={13} /> : active ? <Radio size={13} /> : <Check size={13} />}{' '}
            {paused ? 'Paused' : active ? 'Live' : 'Session ended'}
          </span>
          {runID && !completed && (
            <Link to={`/runs/${encode(runID)}`} className="text-link">
              View completed run
              <ChevronRight size={13} />
            </Link>
          )}
        </div>
      </Panel>
    </>
  )
}
