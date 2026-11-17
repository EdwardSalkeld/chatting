import { useSyncExternalStore } from 'react'
import { Monitor, Moon, Sun } from 'lucide-react'

export type ThemePref = 'system' | 'light' | 'dark'
const KEY = 'chatting.platform.theme'
const ORDER: ThemePref[] = ['system', 'light', 'dark']

export function readStored(key: string) {
  try {
    return localStorage.getItem(key)
  } catch {
    return null
  }
}
export function writeStored(key: string, value: string) {
  try {
    localStorage.setItem(key, value)
  } catch {
    // storage can be blocked (private mode); the preference just won't persist
  }
}

function storedTheme(): ThemePref {
  const value = readStored(KEY)
  return value === 'light' || value === 'dark' ? value : 'system'
}
export function applyTheme(pref: ThemePref = storedTheme()) {
  const root = document.documentElement
  if (pref === 'system') delete root.dataset.theme
  else root.dataset.theme = pref
}

const listeners = new Set<() => void>()
let current: ThemePref | null = null
function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => listeners.delete(listener)
}
function snapshot() {
  return (current ??= storedTheme())
}
export function setTheme(next: ThemePref) {
  current = next
  writeStored(KEY, next)
  applyTheme(next)
  listeners.forEach((listener) => listener())
}
export function cycleTheme() {
  setTheme(ORDER[(ORDER.indexOf(snapshot()) + 1) % ORDER.length])
}
export function useTheme() {
  return [useSyncExternalStore(subscribe, snapshot), setTheme] as const
}

const ICONS = { system: Monitor, light: Sun, dark: Moon }
export function ThemeToggle() {
  const [theme, setTheme] = useTheme()
  const Icon = ICONS[theme]
  const next = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length]
  return (
    <button
      type="button"
      className="icon-button"
      aria-label={`Theme: ${theme}. Switch to ${next}`}
      title={`Theme: ${theme} (click for ${next})`}
      onClick={() => setTheme(next)}
    >
      <Icon />
    </button>
  )
}
