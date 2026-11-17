export type Tone = 'live' | 'ok' | 'pending' | 'warn' | 'danger' | 'neutral'
export type StatusKind = 'run' | 'schedule' | 'reminder' | 'inbox' | 'effect' | 'service'

const TONES: Record<string, Tone> = {
  live: 'live',
  running: 'live',
  success: 'ok',
  fired: 'ok',
  active: 'ok',
  processed: 'ok',
  acked: 'ok',
  ok: 'ok',
  queued: 'pending',
  scheduled: 'pending',
  pending: 'pending',
  attached: 'pending',
  pending_publish: 'pending',
  inflight: 'pending',
  waiting: 'pending',
  loading: 'pending',
  execution_error: 'warn',
  overdue: 'warn',
  uncertain: 'warn',
  stale: 'warn',
  review: 'warn',
  stuck: 'warn',
  preview: 'warn',
  dead_letter: 'danger',
  failed: 'danger',
  failure: 'danger',
  error: 'danger',
  dead: 'danger',
  unavailable: 'danger',
  triage_error: 'danger',
  idle: 'neutral',
  cancelled: 'neutral',
  finished: 'neutral',
  superseded: 'neutral',
  recovered: 'neutral',
  news: 'neutral',
  order: 'neutral',
  action: 'neutral',
}
const INBOX_TONES: Record<string, Tone> = { waiting: 'pending', review: 'warn' }
const reported = new Set<string>()

export function statusTone(value: string | null | undefined, kind?: StatusKind): Tone {
  const key = (value || '').trim().toLowerCase().replaceAll('-', '_').replaceAll(' ', '_')
  if (kind === 'inbox') return INBOX_TONES[key] || 'neutral'
  const tone = TONES[key]
  if (tone) return tone
  if (import.meta.env?.DEV && key && !reported.has(key)) {
    reported.add(key)
    console.warn(`statusTone: unknown status "${value}"`)
  }
  return 'neutral'
}

// One word per outcome: execution_error is "Failed" (warn) everywhere; red "Error" is kept for
// service failures so the two never share a label.
const LABELS: Record<string, string> = {
  execution_error: 'Failed',
  dead_letter: 'Dead letter',
  recovered: 'Recovered',
}

export function statusLabel(value: string) {
  return LABELS[value] || value.replaceAll('_', ' ').replace(/^./, (c) => c.toUpperCase())
}
