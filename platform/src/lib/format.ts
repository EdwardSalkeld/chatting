import type { WorkItem } from './types'

export const encode = encodeURIComponent
export function itemName(item: WorkItem) {
  if (item.work_item_id === 'item_legacy_general') return 'General conversation'
  const meta = item.reply?.metadata
  return String(
    meta?.chat_title ||
      meta?.subject ||
      meta?.sender ||
      item.origin_conversation_id ||
      item.work_item_id,
  )
}
export function dateTime(value?: string | null) {
  if (!value) return 'No runs yet'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString(undefined, {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
}
export function relative(value?: string | null) {
  if (!value) return 'No runs yet'
  const delta = Math.round((new Date(value).getTime() - Date.now()) / 60000)
  if (!Number.isFinite(delta)) return 'Unknown time'
  if (Math.abs(delta) < 1) return 'Just now'
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
  if (Math.abs(delta) < 60) return formatter.format(delta, 'minute')
  if (Math.abs(delta) < 1440) return formatter.format(Math.round(delta / 60), 'hour')
  return formatter.format(Math.round(delta / 1440), 'day')
}
export function duration(ms: number) {
  return ms >= 60000 ? `${(ms / 60000).toFixed(1)} min` : `${(ms / 1000).toFixed(1)} sec`
}
export function human(value: string) {
  return value.replaceAll('_', ' ').replace(/^./, (c) => c.toUpperCase())
}
export function lines(value: FormDataEntryValue | null): string[] {
  return String(value || '')
    .split('\n')
    .map((s) => s.trim())
    .filter(Boolean)
}
export function localDateTime(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return ''
  return new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16)
}
/** Parses a 24h local "YYYY-MM-DD HH:MM" (or with a T) to a Date; undefined when malformed. */
export function parseLocalDateTime(value: string) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})$/.exec(value.trim())
  if (!m) return undefined
  const [y, mo, d, h, mi] = m.slice(1).map(Number)
  const date = new Date(y, mo - 1, d, h, mi)
  return date.getMonth() === mo - 1 && date.getDate() === d && h < 24 && mi < 60 ? date : undefined
}
export function safeMailLink(value: string) {
  try {
    return new URL(value).protocol === 'https:' ? value : undefined
  } catch {
    return undefined
  }
}

export function fmtTokens(value: number | null | undefined) {
  if (value == null) return '—'
  if (value < 1000) return String(value)
  if (value < 1e6) return `${(value / 1e3).toFixed(value < 1e4 ? 1 : 0)}k`
  return `${(value / 1e6).toFixed(value < 1e7 ? 1 : 0)}M`
}

/** One duration style everywhere: "850ms", "12.3s", "2m 5s", "3h 6m" (no zero padding). */
export function fmtLatency(ms: number | null | undefined) {
  if (ms == null || !Number.isFinite(ms)) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  const s = Math.round(ms / 1000)
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

const pad = (n: number) => String(n).padStart(2, '0')
const DAY_MS = 86400e3

/** HH:MM or HH:MM:SS, always 24h. */
export function fmtClock(value: Date | string | number, seconds = false) {
  const d = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(d.getTime())) return String(value)
  const hm = `${pad(d.getHours())}:${pad(d.getMinutes())}`
  return seconds ? `${hm}:${pad(d.getSeconds())}` : hm
}

/**
 * The one relative date style for past and future times: "09:39", "Yest 18:30", "Tmrw 08:00",
 * "Tue 12:00" within a week, otherwise "Nov 14 11:30".
 */
export function fmtWhen(
  value: string | number | null | undefined,
  { seconds = false, now = Date.now() }: { seconds?: boolean; now?: number } = {},
) {
  if (value == null || value === '') return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  const time = fmtClock(date, seconds)
  const midnight = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime()
  const days = Math.round((midnight(date) - midnight(new Date(now))) / DAY_MS)
  if (days === 0) return time
  if (days === -1) return `Yest ${time}`
  if (days === 1) return `Tmrw ${time}`
  if (Math.abs(days) < 7)
    return `${date.toLocaleDateString(undefined, { weekday: 'short' })} ${time}`
  return `${date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} ${time}`
}

export function fullTime(value: string | null | undefined) {
  if (!value) return undefined
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

export function fmtBytes(bytes: number | null | undefined) {
  if (bytes == null || !Number.isFinite(bytes)) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB']
  let value = bytes,
    unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${unit ? value.toFixed(value < 10 ? 2 : 1) : value} ${units[unit]}`
}

/** "3d 4h", "2h 5m", "7m". */
export function fmtUptime(seconds: number | null | undefined) {
  if (seconds == null || !Number.isFinite(seconds)) return '—'
  const m = Math.floor(Math.max(0, seconds) / 60),
    h = Math.floor(m / 60),
    d = Math.floor(h / 24)
  if (d) return `${d}d ${h % 24}h`
  if (h) return `${h}h ${m % 60}m`
  return `${m}m`
}

/** Per-hour buckets over the last `hours`, oldest first. `value` returns a run's contribution. */
export function hourlyBuckets<T>(
  rows: readonly T[],
  at: (row: T) => number,
  value: (row: T) => number,
  { hours = 24, now = Date.now() }: { hours?: number; now?: number } = {},
) {
  const end = Math.floor(now / 3600e3) * 3600e3 + 3600e3
  const buckets = new Array<number>(hours).fill(0)
  for (const row of rows) {
    const index = hours - 1 - Math.floor((end - 1 - at(row)) / 3600e3)
    if (index >= 0 && index < hours) buckets[index] += value(row)
  }
  return buckets
}

/** Average number of runs in flight per hour, from each run's start and finish. */
export function hourlyConcurrency(
  runs: readonly { created_at: string; latency_ms: number }[],
  { hours = 24, now = Date.now() }: { hours?: number; now?: number } = {},
) {
  const end = Math.floor(now / 3600e3) * 3600e3 + 3600e3
  const start = end - hours * 3600e3
  const buckets = new Array<number>(hours).fill(0)
  for (const run of runs) {
    const finish = timeMs(run.created_at)
    const begin = finish - (run.latency_ms || 0)
    for (let i = Math.max(0, Math.floor((begin - start) / 3600e3)); i < hours; i++) {
      const lo = start + i * 3600e3
      if (lo >= finish) break
      const overlap = Math.min(finish, lo + 3600e3) - Math.max(begin, lo)
      if (overlap > 0) buckets[i] += overlap / 3600e3
    }
  }
  return buckets
}

export const isFailure = (status: string | null | undefined) => !!status && status !== 'success'
export const timeMs = (value: string | null | undefined) =>
  value ? new Date(value).getTime() || 0 : 0
