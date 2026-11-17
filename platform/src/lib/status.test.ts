import { describe, expect, it } from 'vitest'
import { statusLabel, statusTone } from './status'

describe('statusTone', () => {
  it.each([
    ['live', 'live'],
    ['success', 'ok'],
    ['fired', 'ok'],
    ['active', 'ok'],
    ['processed', 'ok'],
    ['acked', 'ok'],
    ['queued', 'pending'],
    ['scheduled', 'pending'],
    ['pending', 'pending'],
    ['attached', 'pending'],
    ['pending_publish', 'pending'],
    ['execution_error', 'warn'],
    ['overdue', 'warn'],
    ['uncertain', 'warn'],
    ['stale', 'warn'],
    ['stuck', 'warn'],
    ['dead_letter', 'danger'],
    ['failed', 'danger'],
    ['error', 'danger'],
    ['dead', 'danger'],
    ['unavailable', 'danger'],
    ['triage_error', 'danger'],
    ['idle', 'neutral'],
    ['cancelled', 'neutral'],
    ['finished', 'neutral'],
    ['superseded', 'neutral'],
  ] as const)('maps %s to %s', (value, tone) => {
    expect(statusTone(value)).toBe(tone)
  })
  it('tones only waiting and review among inbox categories', () => {
    expect(statusTone('waiting', 'inbox')).toBe('pending')
    expect(statusTone('review', 'inbox')).toBe('warn')
    for (const category of ['news', 'order', 'action', 'failed', 'success'])
      expect(statusTone(category, 'inbox')).toBe('neutral')
  })
  it('normalises case and separators', () => {
    expect(statusTone('Dead-Letter')).toBe('danger')
    expect(statusTone('Execution error')).toBe('warn')
  })
  it('falls back to neutral for unknown or empty values', () => {
    expect(statusTone('something_new')).toBe('neutral')
    expect(statusTone(undefined)).toBe('neutral')
  })
})

describe('statusLabel', () => {
  it('uses one word per run outcome', () => {
    expect(statusLabel('execution_error')).toBe('Failed')
    expect(statusLabel('dead_letter')).toBe('Dead letter')
    expect(statusLabel('recovered')).toBe('Recovered')
    expect(statusLabel('pending_publish')).toBe('Pending publish')
  })
})
