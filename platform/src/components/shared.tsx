import { AlertCircle, ArrowRight, LoaderCircle, Search } from 'lucide-react'
import { Link } from 'react-router-dom'
import type { ReactNode } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'
import { human } from '@/lib/format'

export function PageHeading({
  title,
  description,
  action,
}: {
  title: string
  description?: string
  action?: ReactNode
}) {
  return (
    <div className="page-heading">
      <div>
        <h1>{title}</h1>
        {description && <p>{description}</p>}
      </div>
      {action}
    </div>
  )
}
export function StatusBadge({ status, children }: { status: string; children?: ReactNode }) {
  const tone = ['success', 'active', 'processed', 'order', 'scheduled'].includes(status)
    ? 'good'
    : ['error', 'failed', 'failure', 'action', 'review'].includes(status)
      ? 'warn'
      : status === 'live'
        ? 'live'
        : 'neutral'
  return (
    <span className={cn('status-badge', tone)}>
      <span className="status-dot" />
      {children || human(status)}
    </span>
  )
}
export function Empty({
  title,
  description,
  action,
}: {
  title: string
  description?: string
  action?: ReactNode
}) {
  return (
    <div className="empty-state">
      <h3>{title}</h3>
      {description && <p>{description}</p>}
      {action}
    </div>
  )
}
export function Loading() {
  return (
    <div className="loading-state" role="status">
      <LoaderCircle className="animate-spin" size={22} />
      <span>Loading…</span>
    </div>
  )
}
export function ErrorNotice({
  error,
  retry,
  children,
  title = 'Request failed',
}: {
  error: Error | null
  title?: string
  retry?: () => void
  children?: ReactNode
}) {
  if (!error) return null
  return (
    <div className="error-notice" role="alert">
      <AlertCircle size={18} />
      <div>
        <strong>{title}</strong>
        <p>{error.message}</p>
        {children}
      </div>
      {retry && (
        <Button variant="outline" size="sm" onClick={retry}>
          Retry
        </Button>
      )}
    </div>
  )
}
export function Panel({
  title,
  subtitle,
  to,
  children,
  className,
}: {
  title?: string
  subtitle?: string
  to?: string
  children: ReactNode
  className?: string
}) {
  return (
    <section className={cn('panel', className)}>
      {title && (
        <div className="panel-heading">
          <div>
            <h2>{title}</h2>
            {subtitle && <p>{subtitle}</p>}
          </div>
          {to && (
            <Link to={to} className="text-link">
              View all <ArrowRight size={14} />
            </Link>
          )}
        </div>
      )}
      {children}
    </section>
  )
}
export function SearchInput({
  value,
  onChange,
  placeholder = 'Search…',
}: {
  value: string
  onChange: (value: string) => void
  placeholder?: string
}) {
  return (
    <div className="search-input">
      <Search size={17} />
      <Input
        aria-label={placeholder}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </div>
  )
}
