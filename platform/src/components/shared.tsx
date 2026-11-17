import { AlertCircle, Search } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useEffect, useState, type ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { statusLabel, statusTone, type StatusKind, type Tone } from '@/lib/status'
import { usePageTitle, TopbarActions, TopbarContext } from '@/components/page-head'

export { usePageTitle, TopbarActions, TopbarContext }
export type { Tone }

/** Sets the top-bar title; `action` renders in the top bar. No in-page heading is drawn. */
export function PageHeading({
  title,
  context,
  action,
}: {
  title: string
  context?: string
  /** @deprecated subtitles are not shown */
  description?: string
  action?: ReactNode
}) {
  usePageTitle(title, context)
  return action ? <TopbarActions>{action}</TopbarActions> : null
}

export function StatusBadge({
  status,
  kind,
  tone,
  children,
  className,
  title,
}: {
  status: string
  kind?: StatusKind
  /** Overrides the tone derived from `status`. */
  tone?: Tone
  children?: ReactNode
  className?: string
  title?: string
}) {
  const resolved = tone || statusTone(status, kind)
  const outline = kind === 'inbox' && resolved === 'neutral'
  return (
    <span
      className={cn('status-badge', outline && 'is-outline', className)}
      data-tone={resolved}
      title={title}
    >
      {!outline && <span className="status-dot" data-tone={resolved} />}
      <span>{children || statusLabel(status)}</span>
    </span>
  )
}

export function StatusDot({
  status,
  kind,
  tone,
  label,
}: {
  status?: string
  kind?: StatusKind
  tone?: Tone
  /** Screen-reader text; defaults to the status label. */
  label?: string
}) {
  const resolved = tone || statusTone(status, kind)
  return (
    <span
      className="status-dot"
      data-tone={resolved}
      role="img"
      aria-label={label || (status ? statusLabel(status) : resolved)}
    />
  )
}

export function CountChip({
  value,
  tone,
}: {
  value: number | string | null | undefined
  tone?: Tone
}) {
  if (value == null || value === 0 || value === '') return null
  return (
    <span className="count-chip" data-tone={tone}>
      {value}
    </span>
  )
}

export function Kbd({ children }: { children: ReactNode }) {
  return <kbd>{children}</kbd>
}

export function useNow(intervalMs = 1000) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

export function shortAge(ms: number) {
  const s = Math.max(0, Math.round(ms / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m`
  const h = Math.floor(m / 60)
  if (h < 48) return `${h}h`
  return `${Math.floor(h / 24)}d`
}

export function ageTone({
  updatedAt,
  interval,
  error,
  now,
}: {
  updatedAt: number
  interval: number
  error?: boolean
  now: number
}): Tone {
  if (error) return 'danger'
  if (!updatedAt) return 'pending'
  return now - updatedAt > 2 * interval ? 'warn' : 'ok'
}

/** "worker 3s" with a status dot; warn after 2× the poll interval, danger on error. Ticks itself. */
export function AgeChip({
  label,
  updatedAt,
  interval,
  error,
  className,
}: {
  label?: string
  /** react-query `dataUpdatedAt` (ms); 0 means still loading. */
  updatedAt: number
  /** Poll interval in ms. */
  interval: number
  error?: boolean
  className?: string
}) {
  const now = useNow(1000)
  const tone = ageTone({ updatedAt, interval, error, now })
  const age = updatedAt ? shortAge(now - updatedAt) : 'loading'
  const description = error
    ? `${label || 'Data'} unavailable${updatedAt ? `, last data ${age} ago` : ''}`
    : updatedAt
      ? `${label || 'Data'} updated ${age} ago`
      : `${label || 'Data'} loading`
  return (
    <span className={cn('age-chip', className)} data-tone={tone} title={description}>
      <span className="status-dot" data-tone={tone} />
      {label && <span className="age-label">{label}</span>}
      <span aria-hidden>{error && !updatedAt ? 'down' : age}</span>
      <span className="sr-only">{description}</span>
    </span>
  )
}

export interface Fact {
  label: string
  value: ReactNode
  mono?: boolean
  tone?: Tone
  title?: string
  hidden?: boolean
  /** 3 hides below 1100px, 2 hides below 760px. */
  priority?: 2 | 3
}
/** Inline key/value strip used under page and sheet headers. Wraps on narrow screens. */
export function FactsRow({ items, className }: { items: Fact[]; className?: string }) {
  return (
    <dl className={cn('facts-row', className)}>
      {items
        .filter((item) => !item.hidden)
        .map((item) => (
          <div key={item.label} data-priority={item.priority}>
            <dt>{item.label}</dt>
            <dd className={item.mono ? 'mono' : undefined} data-tone={item.tone} title={item.title}>
              {item.value ?? '—'}
            </dd>
          </div>
        ))}
    </dl>
  )
}

export interface ChipOption {
  value: string
  label: ReactNode
  count?: number | null
  tone?: Tone
}
/** Toggle chips. `value` may be an array for multi-select; `onChange` receives the clicked value. */
export function FilterChips({
  options,
  value,
  onChange,
  label,
  className,
}: {
  options: ChipOption[]
  value: string | readonly string[]
  onChange: (value: string) => void
  label: string
  className?: string
}) {
  return (
    <div className={cn('filter-chips', className)} role="group" aria-label={label}>
      {options.map((option) => {
        const pressed = Array.isArray(value) ? value.includes(option.value) : value === option.value
        return (
          <button
            key={option.value}
            type="button"
            className="filter-chip"
            aria-pressed={pressed}
            data-tone={option.count ? option.tone : undefined}
            onClick={() => onChange(option.value)}
          >
            {option.label}
            {option.count != null && <span className="chip-count">{option.count}</span>}
          </button>
        )
      })}
    </div>
  )
}

/** Skeleton placeholders at row height. Pass `colSpan` inside a `<tbody>`. */
export function SkeletonRows({ rows = 6, colSpan }: { rows?: number; colSpan?: number }) {
  const widths = ['62%', '48%', '71%', '55%', '66%', '40%']
  if (colSpan)
    return (
      <>
        {Array.from({ length: rows }, (_, i) => (
          <tr key={i} aria-hidden>
            <td className="skeleton-cell" colSpan={colSpan}>
              <span className="skeleton-bar" style={{ width: widths[i % widths.length] }} />
            </td>
          </tr>
        ))}
      </>
    )
  return (
    <div role="status" aria-label="Loading">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="skeleton-row">
          <span className="skeleton-bar" style={{ width: widths[i % widths.length] }} />
        </div>
      ))}
    </div>
  )
}

/** A single 32px line for "nothing here", optionally with an action such as Clear filters. */
export function InlineEmpty({ children, action }: { children: ReactNode; action?: ReactNode }) {
  return (
    <div className="inline-empty">
      <span>{children}</span>
      {action && (
        <>
          <span aria-hidden>·</span>
          {action}
        </>
      )}
    </div>
  )
}

interface QueryLike {
  error: Error | null
  dataUpdatedAt: number
  errorUpdatedAt: number
  refetch: () => unknown
}
function clock(ms: number) {
  return new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })
}
/**
 * 28px danger strip for a failed request that keeps the stale data visible below it:
 * "Worker unavailable since 10:42 (502) · message · Retry".
 */
export function ErrorStrip({
  query,
  error = query?.error ?? null,
  since = query ? query.dataUpdatedAt || query.errorUpdatedAt : undefined,
  label = 'Request',
  retry = query ? () => void query.refetch() : undefined,
  tone = 'danger',
  message,
}: {
  query?: QueryLike
  error?: Error | null
  /** ms timestamp: the last good data, or when the failure started. */
  since?: number
  label?: string
  retry?: () => void
  tone?: 'danger' | 'warn'
  /** Replaces the "<label> unavailable since…" wording, e.g. for a failed save. */
  message?: string
}) {
  if (!error) return null
  const status = 'status' in error && typeof error.status === 'number' ? ` (${error.status})` : ''
  const text = message ?? `${label} unavailable${since ? ` since ${clock(since)}` : ''}${status}`
  return (
    <div className={cn('error-strip', message && 'is-wrap')} role="alert" data-tone={tone}>
      <AlertCircle size={14} />
      <span title={error.message}>
        {text}
        {error.message && !error.message.startsWith('Request failed') ? ` · ${error.message}` : ''}
      </span>
      {retry && (
        <button type="button" onClick={retry}>
          Retry
        </button>
      )}
    </div>
  )
}

export function Empty({
  title,
  description,
  action,
}: {
  title: string
  description?: string
  action?: ReactNode
}) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      {description && <span>{description}</span>}
      {action}
    </div>
  )
}
export function Loading({ rows = 6 }: { rows?: number }) {
  return <SkeletonRows rows={rows} />
}
export function ErrorNotice({
  error,
  retry,
  children,
  title = 'Request failed',
}: {
  error: Error | null
  title?: string
  retry?: () => void
  children?: ReactNode
}) {
  if (!error) return null
  return (
    <div className="error-notice" role="alert">
      <AlertCircle size={14} />
      <div>
        <strong>{title}</strong>
        <p>{error.message}</p>
        {children}
      </div>
      {retry && (
        <Button variant="outline" size="xs" onClick={retry}>
          Retry
        </Button>
      )}
    </div>
  )
}

/** Bordered section with a 32px header. `to` adds a "View all" link. */
export function Panel({
  title,
  meta,
  subtitle,
  count,
  to,
  toLabel = 'View all',
  actions,
  children,
  className,
  id,
}: {
  title?: ReactNode
  meta?: ReactNode
  /** @deprecated use `meta` */
  subtitle?: ReactNode
  count?: number | null
  to?: string
  toLabel?: string
  actions?: ReactNode
  children: ReactNode
  className?: string
  id?: string
}) {
  return (
    <section className={cn('panel', className)} id={id}>
      {title && (
        <div className="panel-heading">
          <h2>{title}</h2>
          {count != null && <span className="count-chip">{count}</span>}
          {(meta || subtitle) && <span className="panel-meta">{meta || subtitle}</span>}
          {(actions || to) && (
            <div className="panel-actions">
              {actions}
              {to && (
                <Link to={to} className="text-link">
                  {toLabel}
                </Link>
              )}
            </div>
          )}
        </div>
      )}
      {children}
    </section>
  )
}

/** 56px stat tile. Pass `to` to make it a link. Render `—` (not 0) when the value is unknown. */
export function Stat({
  label,
  value,
  sub,
  tone,
  to,
  title,
}: {
  label: string
  value: ReactNode
  sub?: ReactNode
  tone?: Tone
  to?: string
  title?: string
}) {
  const body = (
    <>
      <span>{label}</span>
      <div>
        <strong data-tone={tone}>{value ?? '—'}</strong>
        {sub && <small>{sub}</small>}
      </div>
    </>
  )
  return to ? (
    <Link className="stat-tile" to={to} title={title}>
      {body}
    </Link>
  ) : (
    <div className="stat-tile" title={title}>
      {body}
    </div>
  )
}

/** Page search: `/` focuses it, Esc clears then blurs. */
export function SearchInput({
  value,
  onChange,
  placeholder = 'Search…',
  className,
}: {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  className?: string
}) {
  return (
    <div className={cn('search-input', className)}>
      <Search />
      <input
        type="search"
        data-page-search
        aria-label={placeholder}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key !== 'Escape') return
          e.preventDefault()
          if (value) onChange('')
          else e.currentTarget.blur()
        }}
      />
      <kbd>/</kbd>
    </div>
  )
}
