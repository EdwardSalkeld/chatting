import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, NavLink, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'
import { Command, MessageSquare, MoreHorizontal, PanelLeftClose, PanelLeftOpen } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import { Tip, TooltipProvider } from '@/components/ui/tooltip'
import { useDayRuns, useInbox, useItems, useReminders, useSchedules } from '@/lib/api'
import { useShortcuts } from '@/lib/keys'
import { Overview } from '@/pages/overview'
import { WorkItems, WorkItemDetail } from '@/pages/work'
import { Automations } from '@/pages/automations'
import { InboxPage } from '@/pages/inbox'
import { Session } from '@/pages/session'
import { Runs } from '@/pages/runs'
import { System } from '@/pages/system'
import { AgeChip, CountChip, Empty, type Tone } from '@/components/shared'
import { PageHeadContext, usePageHeadState } from '@/components/page-head'
import { NAV, sectionFor } from '@/components/nav'
import { ThemeToggle, cycleTheme, readStored, writeStored } from '@/components/theme'
import { CommandPalette } from '@/components/command-palette'
import { ShortcutHelp } from '@/components/shortcut-help'

const RAIL_KEY = 'chatting.platform.rail'
const WORKER_INTERVAL = 4000,
  HANDLER_INTERVAL = 10000

export default function App() {
  const location = useLocation()
  const navigate = useNavigate()
  const client = useQueryClient()
  const head = usePageHeadState()
  const [rail, setRail] = useState(() => readStored(RAIL_KEY) === '1')
  const [palette, setPalette] = useState(false)
  const [help, setHelp] = useState(false)
  const [more, setMore] = useState(false)

  const fallback = sectionFor(location.pathname)
  const current = head.head?.path === location.pathname ? head.head : null
  const title = current?.title || fallback

  const toggleRail = useCallback(() => {
    setRail((value) => {
      writeStored(RAIL_KEY, value ? '0' : '1')
      return !value
    })
  }, [])
  const refetch = useCallback(() => void client.refetchQueries({ type: 'active' }), [client])
  useShortcuts({
    palette: () => setPalette((open) => !open),
    help: () => setHelp(true),
    refetch,
    toggleSidebar: toggleRail,
    go: navigate,
  })
  useEffect(() => setMore(false), [location.pathname])

  const headApi = useMemo(
    () => ({
      setHead: head.setHead,
      actionsSlot: head.actionsSlot,
      contextSlot: head.contextSlot,
    }),
    [head.setHead, head.actionsSlot, head.contextSlot],
  )

  return (
    <PageHeadContext.Provider value={headApi}>
      <TooltipProvider>
        <DocumentTitle title={title} />
        <div className="app-shell">
          <a className="skip-link" href="#main">
            Skip to content
          </a>
          <aside className={`sidebar ${rail ? 'is-rail' : ''}`} aria-label="Platform navigation">
            <div className="sidebar-head">
              <Link className="brand" to="/">
                <MessageSquare size={16} />
                <span>chatting</span>
              </Link>
              <Tip label={rail ? 'Expand sidebar  [' : 'Collapse sidebar  ['} side="right">
                <button
                  type="button"
                  className="icon-button"
                  aria-label={rail ? 'Expand sidebar' : 'Collapse sidebar'}
                  aria-pressed={rail}
                  onClick={toggleRail}
                >
                  {rail ? <PanelLeftOpen /> : <PanelLeftClose />}
                </button>
              </Tip>
            </div>
            <nav aria-label="Main navigation">
              {NAV.map((entry) => (
                <Tip key={entry.to} label={rail ? entry.label : null} side="right">
                  <NavLink
                    to={entry.to}
                    end={entry.to === '/'}
                    className="nav-item"
                    aria-label={rail ? entry.label : undefined}
                  >
                    <entry.icon size={16} />
                    <span>{entry.label}</span>
                    <NavCount to={entry.to} />
                  </NavLink>
                </Tip>
              ))}
            </nav>
            <ServiceRows />
          </aside>
          <div className="main-shell">
            <header className="topbar">
              <div className="topbar-title">
                <h1>{title}</h1>
                {current?.context && <span className="topbar-context mono">{current.context}</span>}
                <span className="topbar-context" ref={head.setContextSlot} />
              </div>
              <div className="topbar-actions" ref={head.setActionsSlot} />
              <div className="topbar-spacer" />
              <div className="topbar-tools">
                <ServiceAges />
                <ThemeToggle />
                <button
                  type="button"
                  className="palette-button"
                  aria-label="Open command palette"
                  onClick={() => setPalette(true)}
                >
                  <Command />
                  <span>Jump to…</span>
                  <kbd>⌘K</kbd>
                </button>
              </div>
            </header>
            <main id="main" className="main-content">
              <Routes>
                <Route path="/" element={<Overview />} />
                <Route path="/overview" element={<Overview />} />
                <Route path="/items" element={<WorkItems />} />
                <Route path="/items/:id" element={<WorkItemDetail />} />
                <Route path="/runs" element={<Runs />} />
                <Route path="/runs/:id" element={<Session key={location.pathname} />} />
                <Route path="/tasks/:id" element={<Session key={location.pathname} />} />
                <Route
                  path="/schedules"
                  element={<Automations key="schedules" kind="schedules" />}
                />
                <Route
                  path="/reminders"
                  element={<Automations key="reminders" kind="reminders" />}
                />
                <Route path="/inbox" element={<InboxPage />} />
                <Route path="/system" element={<System />} />
                <Route
                  path="*"
                  element={
                    <Empty
                      title="Page not found"
                      action={
                        <Button asChild size="sm">
                          <Link to="/">Overview</Link>
                        </Button>
                      }
                    />
                  }
                />
              </Routes>
            </main>
          </div>
          <nav className="tabbar" aria-label="Main navigation">
            {NAV.filter((n) => ['/', '/items', '/runs', '/inbox'].includes(n.to)).map((entry) => (
              <NavLink
                key={entry.to}
                to={entry.to}
                end={entry.to === '/'}
                className={({ isActive }) => (isActive ? 'active' : '')}
              >
                <entry.icon />
                <span>{entry.short}</span>
                <NavCount to={entry.to} />
              </NavLink>
            ))}
            <button
              type="button"
              className={
                ['/schedules', '/reminders', '/system'].some((p) => location.pathname.startsWith(p))
                  ? 'active'
                  : ''
              }
              aria-haspopup="dialog"
              onClick={() => setMore(true)}
            >
              <MoreHorizontal />
              <span>More</span>
            </button>
          </nav>
        </div>
        <Sheet open={more} onOpenChange={setMore}>
          <SheetContent title="More" aria-describedby={undefined}>
            <div className="more-list">
              {NAV.filter((n) => ['/schedules', '/reminders', '/system'].includes(n.to)).map(
                (entry) => (
                  <NavLink
                    key={entry.to}
                    to={entry.to}
                    className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
                  >
                    <entry.icon size={16} />
                    <span>{entry.label}</span>
                    <NavCount to={entry.to} />
                  </NavLink>
                ),
              )}
            </div>
            <ServiceRows />
          </SheetContent>
        </Sheet>
        <CommandPalette
          open={palette}
          onOpenChange={setPalette}
          onRefetch={refetch}
          onHelp={() => setHelp(true)}
          onToggleTheme={cycleTheme}
        />
        <ShortcutHelp open={help} onOpenChange={setHelp} />
      </TooltipProvider>
    </PageHeadContext.Provider>
  )
}

function DocumentTitle({ title }: { title: string }) {
  const live = !!useItems().data?.active_executors.length
  const attention =
    (useFailedRuns().count || 0) + (useOverdue() || 0) + (useInbox().data?.stuck || 0)
  useEffect(() => {
    document.title = `${live ? '● ' : ''}${attention ? `(${attention}) ` : ''}${title} · Chatting`
  }, [live, attention, title])
  return null
}

function useFailedRuns() {
  const runs = useDayRuns().data?.runs
  const failed = runs?.filter((r) => r.result_status !== 'success')
  return {
    count: failed?.length,
    tone: (failed?.some((r) => r.result_status === 'dead_letter') ? 'danger' : 'warn') as Tone,
  }
}
function useOverdue() {
  const reminders = useReminders().data?.reminders
  const now = Date.now()
  return reminders?.filter((r) => r.status === 'scheduled' && new Date(r.run_at).getTime() < now)
    .length
}

function NavCount({ to }: { to: string }) {
  if (to === '/items') return <ItemsCount />
  if (to === '/runs') return <FailedCount />
  if (to === '/reminders') return <OverdueCount />
  if (to === '/inbox') return <InboxCount />
  return null
}
function Counted({ value, tone }: { value: number | undefined; tone: Tone }) {
  if (!value) return null
  return (
    <>
      <CountChip value={value} tone={tone} />
      <span className="rail-dot" data-tone={tone} />
    </>
  )
}
function ItemsCount() {
  return <Counted value={useItems().data?.active_executors.length} tone="live" />
}
function OverdueCount() {
  return <Counted value={useOverdue()} tone="warn" />
}
function FailedCount() {
  const { count, tone } = useFailedRuns()
  return <Counted value={count} tone={tone} />
}
function InboxCount() {
  return <Counted value={useInbox().data?.awaiting} tone="pending" />
}

function useServices() {
  const worker = useItems(),
    handler = useSchedules()
  return [
    {
      name: 'handler',
      interval: HANDLER_INTERVAL,
      updatedAt: handler.dataUpdatedAt,
      error: handler.isError,
    },
    {
      name: 'worker',
      interval: WORKER_INTERVAL,
      updatedAt: worker.dataUpdatedAt,
      error: worker.isError,
    },
  ]
}
function ServiceAges() {
  return (
    <div className="service-ages" aria-label="Data age">
      {useServices().map((s) => (
        <AgeChip
          key={s.name}
          label={s.name}
          updatedAt={s.updatedAt}
          interval={s.interval}
          error={s.error}
        />
      ))}
    </div>
  )
}
function ServiceRows() {
  return (
    <div className="sidebar-services">
      {useServices().map((s) => (
        <div className="service-row" key={s.name}>
          <span>{s.name === 'handler' ? 'Handler' : 'Worker'}</span>
          <AgeChip updatedAt={s.updatedAt} interval={s.interval} error={s.error} />
        </div>
      ))}
    </div>
  )
}
