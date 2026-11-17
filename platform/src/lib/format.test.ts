import { expect, it } from 'vitest'
import { localDateTime, safeMailLink } from './format'

it('does not turn mailbox content into executable or relative links', () => {
  expect(safeMailLink('javascript:alert(1)')).toBeUndefined()
  expect(safeMailLink('/relative')).toBeUndefined()
  expect(safeMailLink('https://example.com/mail/123')).toBe('https://example.com/mail/123')
})
it('preserves the instant when converting a reminder to a local form value', () => {
  const original = '2026-11-20T14:30:00Z'
  expect(new Date(localDateTime(original)).toISOString()).toBe(original.replace('00Z', '00.000Z'))
})
