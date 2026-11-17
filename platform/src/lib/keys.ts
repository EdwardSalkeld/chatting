import { useEffect, useRef } from 'react'

export const GO_TARGETS: Record<string, { to: string; label: string }> = {
  o: { to: '/', label: 'Overview' },
  i: { to: '/items', label: 'Work items' },
  r: { to: '/runs', label: 'Runs' },
  s: { to: '/schedules', label: 'Schedules' },
  m: { to: '/reminders', label: 'Reminders' },
  n: { to: '/inbox', label: 'Inbox' },
  y: { to: '/system', label: 'System' },
}

export const SHORTCUTS: { keys: string[]; label: string }[] = [
  { keys: ['⌘K', 'Ctrl K'], label: 'Command palette' },
  { keys: ['/'], label: 'Focus page search' },
  { keys: ['Esc'], label: 'Clear search / close sheet' },
  { keys: ['j', 'k'], label: 'Next / previous row' },
  { keys: ['Enter', 'o'], label: 'Open focused row' },
  { keys: ['→', '←'], label: 'Expand / collapse dead letter' },
  { keys: ['f', 'p'], label: 'Session: follow / pause' },
  { keys: ['e', 'c'], label: 'Session: next error / copy' },
  ...Object.entries(GO_TARGETS).map(([key, { label }]) => ({ keys: [`g ${key}`], label })),
  { keys: ['['], label: 'Collapse sidebar' },
  { keys: ['r'], label: 'Refetch data' },
  { keys: ['?'], label: 'Keyboard shortcuts' },
]

export function isEditableTarget(target: EventTarget | null) {
  if (!(target instanceof HTMLElement)) return false
  if (target.isContentEditable) return true
  const tag = target.tagName
  if (tag === 'TEXTAREA' || tag === 'SELECT') return true
  if (tag !== 'INPUT') return false
  const type = (target as HTMLInputElement).type
  return !['checkbox', 'radio', 'button', 'submit', 'reset'].includes(type)
}

export function isPaletteKey(event: Pick<KeyboardEvent, 'key' | 'metaKey' | 'ctrlKey'>) {
  return (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k'
}

const ROW = 'tr[data-row]'
/** Rows carry scroll-margin for the sticky headers, so "nearest" keeps them clear of both bars. */
export function focusRow(row: HTMLElement) {
  row.focus({ preventScroll: true })
  row.scrollIntoView({ block: 'nearest' })
}
export function moveRowFocus(step: 1 | -1) {
  const active = document.activeElement as HTMLElement | null
  const current = active?.closest<HTMLElement>(ROW)
  const scope = current?.closest('table') || document.querySelector('main table:has(tr[data-row])')
  const rows = Array.from(scope?.querySelectorAll<HTMLElement>(ROW) || [])
  if (!rows.length) return false
  const index = current ? rows.indexOf(current) : -1
  const next = rows[Math.max(0, Math.min(rows.length - 1, index < 0 ? 0 : index + step))]
  focusRow(next)
  return true
}

export interface ShortcutHandlers {
  palette: () => void
  help: () => void
  refetch: () => void
  toggleSidebar: () => void
  go: (to: string) => void
}

/** Global single keys and `g` sequences; ignored while typing or when a dialog is open. */
export function useShortcuts(handlers: ShortcutHandlers) {
  const ref = useRef(handlers)
  ref.current = handlers
  useEffect(() => {
    let pendingG = 0
    function onKey(event: KeyboardEvent) {
      const h = ref.current
      if (isPaletteKey(event)) {
        event.preventDefault()
        h.palette()
        return
      }
      if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return
      if (isEditableTarget(event.target)) return
      if (document.querySelector('[role="dialog"][data-state="open"], [role="alertdialog"]')) return
      const key = event.key
      if (pendingG && Date.now() - pendingG < 1200) {
        pendingG = 0
        const target = GO_TARGETS[key.toLowerCase()]
        if (target) {
          event.preventDefault()
          h.go(target.to)
        }
        return
      }
      pendingG = 0
      if (key === 'g') pendingG = Date.now()
      else if (key === '?') h.help()
      else if (key === '[') h.toggleSidebar()
      else if (key === 'r') h.refetch()
      else if (key === '/') {
        const search = document.querySelector<HTMLInputElement>('[data-page-search]')
        if (!search) return
        event.preventDefault()
        search.focus()
        search.select()
      } else if (key === 'j' || key === 'k') {
        if (moveRowFocus(key === 'j' ? 1 : -1)) event.preventDefault()
      } else return
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])
}
