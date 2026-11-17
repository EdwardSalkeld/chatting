import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { useLocation } from 'react-router-dom'

export interface PageHead {
  path: string
  title: string
  context?: string
}
interface PageHeadApi {
  setHead: (head: PageHead) => void
  actionsSlot: HTMLElement | null
  contextSlot: HTMLElement | null
}
export const PageHeadContext = createContext<PageHeadApi | null>(null)

export function usePageHeadState() {
  const [head, setHead] = useState<PageHead | null>(null)
  const [actionsSlot, setActionsSlot] = useState<HTMLElement | null>(null)
  const [contextSlot, setContextSlot] = useState<HTMLElement | null>(null)
  return { head, setHead, actionsSlot, setActionsSlot, contextSlot, setContextSlot }
}

/** Sets the top-bar title (and document title) for the current route. */
export function usePageTitle(title: string, context?: string) {
  const api = useContext(PageHeadContext)
  const { pathname } = useLocation()
  const setHead = api?.setHead
  useEffect(() => {
    setHead?.({ path: pathname, title, context })
  }, [setHead, pathname, title, context])
}

/** Renders children into the top bar, right after the title. */
export function TopbarActions({ children }: { children: ReactNode }) {
  const slot = useContext(PageHeadContext)?.actionsSlot
  return slot ? createPortal(children, slot) : null
}
/** Renders rich context (badges, ids) next to the top-bar title. */
export function TopbarContext({ children }: { children: ReactNode }) {
  const slot = useContext(PageHeadContext)?.contextSlot
  return slot ? createPortal(children, slot) : null
}
