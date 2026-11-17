import { expect, it } from 'vitest'
import {
  fmtBytes,
  fmtLatency,
  fmtTokens,
  fmtUptime,
  itemName,
  localDateTime,
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
