import { useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'

export type ParamPatch = Record<string, string | null | undefined>
export interface SetOptions {
  replace?: boolean
}

export function patchParams(
  current: URLSearchParams,
  patch: ParamPatch,
  defaults: ParamPatch = {},
) {
  const next = new URLSearchParams(current)
  for (const [key, value] of Object.entries(patch)) {
    if (value == null || value === '' || value === defaults[key]) next.delete(key)
    else next.set(key, value)
  }
  return next
}

export function useUrlParams() {
  const [params, setParams] = useSearchParams()
  const update = useCallback(
    (patch: ParamPatch, options: SetOptions = {}) =>
      setParams((current) => patchParams(current, patch), {
        replace: options.replace,
        preventScrollReset: true,
      }),
    [setParams],
  )
  return [params, update] as const
}

export function useUrlState(key: string, defaultValue = '') {
  const [params, update] = useUrlParams()
  const value = params.get(key) ?? defaultValue
  const set = useCallback(
    (next: string, options: SetOptions = {}) =>
      update({ [key]: next === defaultValue ? null : next }, options),
    [update, key, defaultValue],
  )
  return [value, set] as const
}

export interface SortState {
  key: string
  desc: boolean
}
export function parseSort(value: string | null | undefined): SortState | null {
  if (!value) return null
  return value.startsWith('-') ? { key: value.slice(1), desc: true } : { key: value, desc: false }
}
export function toggleSort(current: string, key: string, firstDesc = false) {
  const sort = parseSort(current)
  if (sort?.key !== key) return firstDesc ? `-${key}` : key
  return sort.desc ? key : `-${key}`
}
export function sortRows<T>(
  rows: readonly T[],
  value: (row: T) => string | number | null | undefined,
  desc: boolean,
): T[] {
  return rows
    .map((row, index) => ({ row, index, v: value(row) }))
    .sort((a, b) => {
      const aEmpty = a.v == null || a.v === '',
        bEmpty = b.v == null || b.v === ''
      if (aEmpty || bEmpty) return aEmpty === bEmpty ? a.index - b.index : aEmpty ? 1 : -1
      const order =
        typeof a.v === 'number' && typeof b.v === 'number'
          ? a.v - b.v
          : String(a.v).localeCompare(String(b.v), undefined, {
              numeric: true,
              sensitivity: 'base',
            })
      return (desc ? -order : order) || a.index - b.index
    })
    .map(({ row }) => row)
}
