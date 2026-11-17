import { expect, it } from 'vitest'
import {
  fmtBytes,
  fmtLatency,
  fmtTokens,
  fmtUptime,
  fmtWhen,
  itemName,
  localDateTime,
  parseLocalDateTime,
  safeMailLink,
} from './format'

it('does not turn mailbox content into executable or relative links', () => {
  expect(safeMailLink('javascript:alert(1)')).toBeUndefined()
  expect(safeMailLink('/relative')).toBeUndefined()
  expect(safeMailLink('https://example.com/mail/123')).toBe('https://example.com/mail/123')
})
it('preserves the instant when converting a reminder to a local form value', () => {
  const original = '2026-11-20T14:30:00Z'
  expect(new Date(localDateTime(original)).toISOString()).toBe(original.replace('00Z', '00.000Z'))
})

it('formats latency, tokens, bytes and uptime compactly', () => {
  expect(fmtLatency(850)).toBe('850ms')
  expect(fmtLatency(12_340)).toBe('12.3s')
  expect(fmtLatency(125_000)).toBe('2m 5s')
  expect(fmtLatency(119_700)).toBe('2m 0s')
  expect(fmtLatency(11_171_000)).toBe('3h 6m')
  expect(fmtLatency(null)).toBe('—')
  expect(fmtTokens(950)).toBe('950')
  expect(fmtTokens(6_300_000)).toBe('6.3M')
  expect(fmtBytes(2_834_592)).toBe('2.70 MiB')
  expect(fmtUptime(275_817)).toBe('3d 4h')
})
it('names email items by subject when there is no chat title', () => {
  const item = {
    work_item_id: 'item_x',
    origin_conversation_id: 'conv',
    reply: { type: 'email', target: 'a@b', metadata: { subject: 'Invoice 42' } },
    run_count: 0,
    queued: 0,
    model_tier: 'auto',
    last_run_at: null,
    explicit_finish: false,
  }
  expect(itemName(item)).toBe('Invoice 42')
})
it('formats past and future times with one relative style', () => {
  const now = new Date(2026, 10, 17, 12, 0).getTime()
  const at = (d: number, h: number, m = 0) => new Date(2026, 10, 17 + d, h, m).getTime()
  expect(fmtWhen(at(0, 9, 5), { now })).toBe('09:05')
  const weekday = (d: number) =>
    new Date(at(d, 0)).toLocaleDateString(undefined, { weekday: 'short' })
  expect(fmtWhen(at(-1, 18, 30), { now })).toBe(`${weekday(-1)} 18:30`)
  expect(fmtWhen(at(1, 8), { now })).toBe(`${weekday(1)} 08:00`)
  expect(fmtWhen(at(0, 9, 5) + 7000, { now, seconds: true })).toBe('09:05:07')
  expect(fmtWhen(at(-3, 12), { now })).toMatch(/^\S+ 12:00$/)
  expect(fmtWhen(at(-30, 12), { now })).toMatch(/ 12:00$/)
  expect(fmtWhen(null)).toBe('—')
})
it('parses 24h local date-times and rejects malformed ones', () => {
  expect(parseLocalDateTime('2026-11-20 14:30')?.getTime()).toBe(
    new Date(2026, 10, 20, 14, 30).getTime(),
  )
  expect(parseLocalDateTime('2026-11-20T09:05')?.getHours()).toBe(9)
  for (const bad of ['2026-02-30 10:00', '2026-11-20 24:00', '11/20/2026 2:30 PM', ''])
    expect(parseLocalDateTime(bad)).toBeUndefined()
})
