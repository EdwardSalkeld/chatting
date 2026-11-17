import { createContext, useCallback, useContext, useRef, useState, type ReactNode } from 'react'
import { AlertCircle, Check, Info } from 'lucide-react'
import type { Tone } from '@/lib/status'

export interface ToastOptions {
  tone?: Extract<Tone, 'ok' | 'danger' | 'warn' | 'neutral'>
  /** ms; failures stay longer so they can be read. */
  duration?: number
}
interface ToastItem extends Required<ToastOptions> {
  id: number
  message: string
}
type Show = (message: string, options?: ToastOptions) => void

const ToastContext = createContext<Show>(() => {})

export function useToast() {
  return useContext(ToastContext)
}

/** One polite live region for write confirmations, write failures and refetches. */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([])
  const next = useRef(0)
  const show = useCallback<Show>((message, { tone = 'ok', duration } = {}) => {
    const id = ++next.current
    const ms = duration ?? (tone === 'danger' ? 8000 : 4000)
    setItems((list) =>
      [...list.filter((t) => t.message !== message), { id, message, tone, duration: ms }].slice(-3),
    )
    setTimeout(() => setItems((list) => list.filter((t) => t.id !== id)), ms)
  }, [])
  return (
    <ToastContext.Provider value={show}>
      {children}
      <div className="toast-region" role="status" aria-live="polite" aria-atomic="false">
        {items.map((item) => {
          const Icon = item.tone === 'ok' ? Check : item.tone === 'neutral' ? Info : AlertCircle
          return (
            <div key={item.id} className="toast" data-tone={item.tone}>
              <Icon size={14} aria-hidden />
              <span>{item.message}</span>
            </div>
          )
        })}
      </div>
    </ToastContext.Provider>
  )
}
