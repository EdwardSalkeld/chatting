import { expect, it } from 'vitest'
import { GO_TARGETS, isPaletteKey } from './keys'

it('maps every g-sequence letter to a distinct route', () => {
  const routes = Object.values(GO_TARGETS).map((t) => t.to)
  expect(new Set(routes).size).toBe(routes.length)
  expect(GO_TARGETS.m.to).toBe('/reminders')
  expect(GO_TARGETS.y.to).toBe('/system')
})
it('recognises the palette chord on both platforms', () => {
  expect(isPaletteKey({ key: 'k', metaKey: true, ctrlKey: false })).toBe(true)
  expect(isPaletteKey({ key: 'K', metaKey: false, ctrlKey: true })).toBe(true)
  expect(isPaletteKey({ key: 'k', metaKey: false, ctrlKey: false })).toBe(false)
})
