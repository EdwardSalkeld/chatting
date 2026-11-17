import { useEffect, useRef, useState } from 'react'
import { Link, NavLink, Route, Routes, useLocation } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'
import {
  Activity,
  CalendarClock,
  ChevronRight,
  LayoutDashboard,
  ListTodo,
  Mail,
  Menu,
  MessageSquare,
  RefreshCw,
  Server,
  X,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useItems, useSchedules } from '@/lib/api'
import { Overview } from '@/pages/overview'
import { WorkItems, WorkItemDetail } from '@/pages/work'
import { Automations } from '@/pages/automations'
import { InboxPage } from '@/pages/inbox'
import { Session } from '@/pages/session'
import { Empty } from '@/components/shared'

const nav = [
  { to: '/', label: 'Overview', icon: LayoutDashboard },
  { to: '/items', label: 'Work items', icon: ListTodo },
  { to: '/schedules', label: 'Schedules', icon: CalendarClock },
  { to: '/reminders', label: 'Reminders', icon: Activity },
  { to: '/inbox', label: 'Personal inbox', icon: Mail },
]
export default function App() {
  const location = useLocation()
  const [menu, setMenu] = useState(false),
    [refreshing, setRefreshing] = useState(false)
  const sidebar = useRef<HTMLElement>(null),
    menuButton = useRef<HTMLButtonElement>(null)
  const client = useQueryClient(),
    items = useItems(),
    schedules = useSchedules()
  const section =
    nav.find((n) => n.to !== '/' && location.pathname.startsWith(n.to))?.label ||
    (location.pathname.startsWith('/tasks/') || location.pathname.startsWith('/runs/')
      ? 'Session'
      : 'Overview')
  useEffect(() => {
    document.title = `${section} · Chatting`
    setMenu(false)
  }, [section, location.pathname])
  useEffect(() => {
    if (!menu) return
    sidebar.current?.querySelector<HTMLAnchorElement>('a')?.focus()
    const media = matchMedia('(min-width: 761px)')
    const closeOnDesktop = () => {
      if (media.matches) setMenu(false)
    }
    media.addEventListener('change', closeOnDesktop)
    return () => {
      media.removeEventListener('change', closeOnDesktop)
      menuButton.current?.focus()
    }
  }, [menu])
  async function refresh() {
    setRefreshing(true)
    await client.invalidateQueries()
    setRefreshing(false)
  }
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      {menu && (
        <button
          className="mobile-backdrop"
          aria-label="Close navigation"
          onClick={() => setMenu(false)}
        />
      )}
      <aside
        ref={sidebar}
        id="platform-navigation"
        className={`sidebar ${menu ? 'is-open' : ''}`}
        role={menu ? 'dialog' : undefined}
        aria-modal={menu || undefined}
        aria-label="Platform navigation"
        onKeyDown={(event) => {
          if (!menu) return
          if (event.key === 'Escape') {
            setMenu(false)
            return
          }
          if (event.key !== 'Tab') return
          const links = sidebar.current?.querySelectorAll<HTMLElement>('a, button')
          if (!links?.length) return
          const first = links[0],
            last = links[links.length - 1]
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault()
            last.focus()
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault()
            first.focus()
          }
        }}
      >
        <button
          className="sidebar-close"
          aria-label="Close navigation"
          onClick={() => setMenu(false)}
        >
          <X size={18} />
        </button>
        <Link className="brand" to="/">
          <MessageSquare size={22} />
          <strong>chatting</strong>
          <span>Platform</span>
        </Link>
        <nav aria-label="Main navigation">
          {nav.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
            >
              <Icon size={17} />
              <span>{label}</span>
              {to === '/items' && !!items.data?.active_executors.length && (
                <span className="nav-count">{items.data.active_executors.length}</span>
              )}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="nav-label">Services</div>
          {[
            { name: 'Handler', icon: Server, query: schedules },
            { name: 'Worker', icon: Activity, query: items },
          ].map(({ name, icon: Icon, query }) => (
            <div className="connection" key={name}>
              <Icon size={14} />
              <span>{name}</span>
              <span
                className={`connection-dot ${query.isError ? 'offline' : query.isPending ? 'pending' : ''}`}
              />
              <span className="sr-only">
                {query.isError ? 'Unavailable' : query.isPending ? 'Connecting' : 'Connected'}
              </span>
            </div>
          ))}
        </div>
      </aside>
      <div className="main-shell" inert={menu}>
        <header className="topbar">
          <div className="breadcrumbs">
            <Button
              ref={menuButton}
              variant="ghost"
              size="icon"
              className="mobile-menu"
              aria-label="Open navigation"
              aria-expanded={menu}
              aria-controls="platform-navigation"
              onClick={() => setMenu(true)}
            >
              <Menu />
            </Button>
            <span>Chatting</span>
            <ChevronRight size={13} />
            <strong>{section}</strong>
          </div>
          <Button variant="ghost" size="sm" disabled={refreshing} onClick={refresh}>
            <RefreshCw className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </header>
        <main id="main" className="main-content">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/overview" element={<Overview />} />
            <Route path="/items" element={<WorkItems />} />
            <Route path="/items/:id" element={<WorkItemDetail />} />
            <Route path="/runs/:id" element={<Session key={location.pathname} />} />
            <Route path="/tasks/:id" element={<Session key={location.pathname} />} />
            <Route path="/schedules" element={<Automations key="schedules" kind="schedules" />} />
            <Route path="/reminders" element={<Automations key="reminders" kind="reminders" />} />
            <Route path="/inbox" element={<InboxPage />} />
            <Route
              path="*"
              element={
                <Empty
                  title="Page not found"
                  action={
                    <Button asChild>
                      <Link to="/">Overview</Link>
                    </Button>
                  }
                />
              }
            />
          </Routes>
        </main>
      </div>
    </div>
  )
}
