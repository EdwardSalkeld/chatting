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

export function fmtLatency(ms: number | null | undefined) {
  if (ms == null || !Number.isFinite(ms)) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  const s = ms / 1000
  if (s < 60) return `${s.toFixed(1)}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ${Math.round(s % 60)}s`
  return `${Math.floor(m / 60)}h ${m % 60}m`
}

const sameDay = (a: Date, b: Date) =>
  a.getFullYear() === b.getFullYear() &&
  a.getMonth() === b.getMonth() &&
  a.getDate() === b.getDate()

/** "09:39", "Yest 18:30", "Nov 14 11:30". */
export function fmtWhen(value: string | null | undefined) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const time = date.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
  const now = new Date()
  if (sameDay(date, now)) return time
  const yesterday = new Date(now)
  yesterday.setDate(now.getDate() - 1)
  if (sameDay(date, yesterday)) return `Yest ${time}`
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

export const isFailure = (status: string | null | undefined) => !!status && status !== 'success'
export const timeMs = (value: string | null | undefined) =>
  value ? new Date(value).getTime() || 0 : 0
