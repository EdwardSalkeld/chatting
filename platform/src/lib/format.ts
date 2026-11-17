import type { WorkItem } from './types'

export const encode = encodeURIComponent
export function itemName(item: WorkItem) {
  if (item.work_item_id === 'item_legacy_general') return 'General conversation'
  const meta = item.reply?.metadata
  return String(
    meta?.chat_title || meta?.sender || item.origin_conversation_id || item.work_item_id,
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
