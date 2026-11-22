import {
  Bell,
  CalendarClock,
  History,
  LayoutDashboard,
  ListTodo,
  Mail,
  MessageSquare,
  Server,
  type LucideIcon,
} from 'lucide-react'

export interface NavEntry {
  to: string
  label: string
  short: string
  icon: LucideIcon
  key: string
}
export const NAV: NavEntry[] = [
  { to: '/', label: 'Overview', short: 'Overview', icon: LayoutDashboard, key: 'o' },
  { to: '/conversations', label: 'Conversations', short: 'Chats', icon: MessageSquare, key: 'c' },
  { to: '/items', label: 'Work items', short: 'Items', icon: ListTodo, key: 'i' },
  { to: '/runs', label: 'Runs', short: 'Runs', icon: History, key: 'r' },
  { to: '/schedules', label: 'Schedules', short: 'Schedules', icon: CalendarClock, key: 's' },
  { to: '/reminders', label: 'Reminders', short: 'Reminders', icon: Bell, key: 'm' },
  { to: '/inbox', label: 'Inbox', short: 'Inbox', icon: Mail, key: 'n' },
  { to: '/system', label: 'System', short: 'System', icon: Server, key: 'y' },
]
export function sectionFor(pathname: string) {
  if (pathname.startsWith('/tasks/') || /^\/runs\/.+/.test(pathname)) return 'Session'
  return NAV.find((n) => n.to !== '/' && pathname.startsWith(n.to))?.label || 'Overview'
}
