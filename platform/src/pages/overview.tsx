import { Link } from 'react-router-dom'
import { ArrowRight, Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Empty, ErrorNotice, Loading, PageHeading, Panel, StatusBadge } from '@/components/shared'
import { ApiError, useInbox, useItems, useReminders, useSchedules } from '@/lib/api'
import { dateTime, encode, itemName, relative } from '@/lib/format'

export function Overview() {
  const items = useItems(),
    schedules = useSchedules(),
    reminders = useReminders(),
    inbox = useInbox()
  const active = items.data?.active_executors || []
  const queued = items.data?.items.reduce((sum, item) => sum + item.queued, 0)
  const upcoming = (reminders.data?.reminders || [])
    .filter((r) => r.status === 'scheduled')
    .sort((a, b) => a.run_at.localeCompare(b.run_at))
  const metrics = [
    {
      label: 'Running',
      value: items.isError ? '—' : items.data ? active.length : '…',
      to: '/items?filter=live',
    },
    { label: 'Queued', value: items.isError ? '—' : (queued ?? '…'), to: '/items?filter=queued' },
    {
      label: 'Schedules',
      value: schedules.isError ? '—' : (schedules.data?.schedules.length ?? '…'),
      to: '/schedules',
    },
    {
      label: 'Unclassified mail',
      value: inbox.isError ? '—' : (inbox.data?.awaiting ?? '…'),
      to: '/inbox?category=waiting',
    },
  ]
  return (
    <>
      <PageHeading title="Overview" />
      <ErrorNotice
        error={items.error || schedules.error}
        retry={() => {
          void items.refetch()
          void schedules.refetch()
        }}
      />
      <div className="stats-grid">
        {metrics.map((metric) => (
          <Link className="stat-card" key={metric.label} to={metric.to}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
            <ArrowRight size={15} />
          </Link>
        ))}
      </div>
      <div className="overview-columns">
        <div className="overview-primary">
          <Panel title="Running tasks" to="/items?filter=live">
            {items.isPending ? (
              <Loading />
            ) : active.length ? (
              <div className="live-list">
                {active.map((run) => (
                  <Link
                    className="live-card"
                    key={run.task_id}
                    to={`/tasks/${encode(run.task_id)}`}
                  >
                    <div className="live-copy">
                      <div>
                        <StatusBadge status="live" />
                        <span>{run.source}</span>
                      </div>
                      <h3>{run.preview || run.task_id}</h3>
                      <p>
                        {relative(run.started_at)} · Attempt {run.attempt}
                      </p>
                    </div>
                    <ArrowRight size={17} />
                  </Link>
                ))}
              </div>
            ) : (
              <Empty title={items.isError ? 'Worker unavailable' : 'No running tasks'} />
            )}
          </Panel>
          <Panel title="Recent work items" to="/items">
            {items.isPending ? (
              <Loading />
            ) : items.data?.items.length ? (
              <div className="work-list">
                {items.data.items.slice(0, 8).map((item) => (
                  <Link
                    className="work-row"
                    key={item.work_item_id}
                    to={`/items/${encode(item.work_item_id)}`}
                  >
                    <div className="work-row-copy">
                      <h3>{itemName(item)}</h3>
                      <p>
                        {item.reply?.type || 'Internal'} · {item.run_count}{' '}
                        {item.run_count === 1 ? 'run' : 'runs'}
                      </p>
                    </div>
                    <span className="row-time">{relative(item.last_run_at)}</span>
                    <ArrowRight size={16} />
                  </Link>
                ))}
              </div>
            ) : (
              <Empty title={items.isError ? 'Worker unavailable' : 'No work items'} />
            )}
          </Panel>
        </div>
        <div className="overview-secondary">
          <Panel title="Upcoming reminders" to="/reminders">
            <ErrorNotice
              error={reminders.error}
              retry={() => {
                void reminders.refetch()
              }}
            />
            {reminders.isPending ? (
              <Loading />
            ) : upcoming.length ? (
              <div className="upcoming-list">
                {upcoming.slice(0, 5).map((reminder) => (
                  <Link
                    className="upcoming-row"
                    key={reminder.reminder_id}
                    to={`/reminders?edit=${encode(reminder.reminder_id)}`}
                  >
                    <div>
                      <h3>{reminder.prompt}</h3>
                      <p>{dateTime(reminder.run_at)}</p>
                    </div>
                    <ArrowRight size={15} />
                  </Link>
                ))}
              </div>
            ) : (
              <Empty
                title={reminders.isError ? 'Reminders unavailable' : 'No upcoming reminders'}
              />
            )}
            <Link to="/reminders?new=1" className="panel-footer-link">
              <Plus size={14} />
              New reminder
            </Link>
          </Panel>
          <Panel title="Inbox" to="/inbox">
            {inbox.isPending ? (
              <Loading />
            ) : inbox.isError ? (
              <div className="small-empty">
                {inbox.error instanceof ApiError && inbox.error.status === 404
                  ? 'Not configured'
                  : 'Inbox unavailable'}
              </div>
            ) : (
              inbox.data && (
                <>
                  <div className="inbox-overview-status">
                    <StatusBadge status={inbox.data.dry_run ? 'preview' : 'active'}>
                      {inbox.data.dry_run ? 'Preview' : 'Active'}
                    </StatusBadge>
                    <span>{inbox.data.rows.length} recent decisions</span>
                  </div>
                  {inbox.data.rows.length ? (
                    <div className="inbox-overview-list">
                      {['action', 'review', 'order', 'news', 'waiting'].map((category) => (
                        <Link key={category} to={`/inbox?category=${category}`}>
                          <StatusBadge status={category} />
                          <strong>
                            {inbox.data!.rows.filter((row) => row.decision === category).length}
                          </strong>
                          <ArrowRight size={13} />
                        </Link>
                      ))}
                    </div>
                  ) : (
                    <Empty title="No decisions" />
                  )}
                  {!!inbox.data.last_error && (
                    <ErrorNotice error={new Error(inbox.data.last_error)} />
                  )}
                </>
              )
            )}
          </Panel>
          <Button variant="outline" asChild>
            <Link to="/schedules?new=1">
              <Plus />
              New schedule
            </Link>
          </Button>
        </div>
      </div>
    </>
  )
}
