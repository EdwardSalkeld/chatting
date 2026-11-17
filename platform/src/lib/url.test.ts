import { describe, expect, it } from 'vitest'
import { parseSort, patchParams, sortRows, toggleSort } from './url'

describe('patchParams', () => {
  it('sets, replaces and removes keys without touching others', () => {
    const next = patchParams(new URLSearchParams('q=a&tab=history&edit=1'), {
      q: 'b',
      edit: null,
      sort: '-last',
    })
    expect(next.toString()).toBe('q=b&tab=history&sort=-last')
  })
  it('drops empty values and values equal to the default', () => {
    const next = patchParams(
      new URLSearchParams('q=a&tab=history'),
      { q: '', tab: 'upcoming' },
      {
        tab: 'upcoming',
      },
    )
    expect(next.toString()).toBe('')
  })
  it('does not mutate the input', () => {
    const current = new URLSearchParams('q=a')
    patchParams(current, { q: 'b' })
    expect(current.get('q')).toBe('a')
  })
})

describe('sort helpers', () => {
  it('parses direction from a leading minus', () => {
    expect(parseSort('-last_run')).toEqual({ key: 'last_run', desc: true })
    expect(parseSort('name')).toEqual({ key: 'name', desc: false })
    expect(parseSort('')).toBeNull()
  })
  it('toggles ascending and descending on the same column', () => {
    expect(toggleSort('', 'name')).toBe('name')
    expect(toggleSort('name', 'name')).toBe('-name')
    expect(toggleSort('-name', 'name')).toBe('name')
    expect(toggleSort('-name', 'runs', true)).toBe('-runs')
  })
  it('sorts numbers and strings with empty values last in both directions', () => {
    const rows = [{ v: 2 }, { v: null }, { v: 10 }, { v: 1 }]
    expect(sortRows(rows, (r) => r.v, false).map((r) => r.v)).toEqual([1, 2, 10, null])
    expect(sortRows(rows, (r) => r.v, true).map((r) => r.v)).toEqual([10, 2, 1, null])
    const names = ['item 10', 'Item 2', 'item 1'].map((v) => ({ v }))
    expect(sortRows(names, (r) => r.v, false).map((r) => r.v)).toEqual([
      'item 1',
      'Item 2',
      'item 10',
    ])
  })
})
