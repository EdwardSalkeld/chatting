import { useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent, ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { LoaderCircle, Mail, MessageSquare, Plus, Trash2 } from 'lucide-react'
import { useToast } from '@/components/toast'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import {
  ErrorStrip,
  FactsRow,
  FilterChips,
  Panel,
  SearchInput,
  SkeletonRows,
  StatusBadge,
  StatusDot,
  TopbarActions,
  useNow,
  usePageTitle,
  type Fact,
} from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { api, useReminders, useRuns, useSchedules } from '@/lib/api'
import { encode, fmtLatency, fmtWhen, lines, localDateTime, parseLocalDateTime } from '@/lib/format'
import { useUrlParams } from '@/lib/url'
import {
  cronError,
  describeCron,
  fullWhen,
  nextRuns,
  parseCronTask,
  parseReminderTask,
  relTime,
  shortCron,
  timezoneError,
  zonedWhen,
} from '@/lib/cron'
import type { Reminder, RunRow, Schedule } from '@/lib/types'
import '@/styles/automations.css'

type Kind = 'schedules' | 'reminders'
type RecordType = Schedule | Reminder
const isSchedule = (row: RecordType): row is Schedule => 'schedule_id' in row
const recordID = (row: RecordType) => (isSchedule(row) ? row.schedule_id : row.reminder_id)
const localZone = Intl.DateTimeFormat().resolvedOptions().timeZone
const ms = (value?: string | null) => (value ? new Date(value).getTime() : NaN)

const localWhen = (value?: string | null) => fmtWhen(value)
const latency = fmtLatency
function matches(query: string, ...values: (string | null | undefined)[]) {
  if (!query) return true
  const haystack = values.join(' ').toLowerCase()
  return query
    .toLowerCase()
    .split(/\s+/)
    .every((term) => haystack.includes(term))
}

export function Automations({ kind }: { kind: Kind }) {
  return kind === 'schedules' ? <SchedulesPage /> : <RemindersPage />
}

/** Write confirmations go to the app-wide toast region, which announces them. */
function useSavedNotice() {
  const toast = useToast()
  return [null, (message: string) => toast(message)] as const
}

/** Channel icon and the tail of the target id; the full id is in the tooltip. */
function TargetCell({ type, target }: { type?: string; target?: string }) {
  if (!type && !target) return <span className="muted">—</span>
  const Icon = type === 'email' ? Mail : type ? MessageSquare : null
  const short = target && target.length > 8 ? `…${target.slice(-6)}` : target
  return (
    <span className="source-label" title={`${type || ''} ${target || ''}`.trim()}>
      {Icon && <Icon aria-hidden />}
      <span className="mono">{short || type}</span>
    </span>
  )
}

function useSheetParams() {
  const [params, update] = useUrlParams()
  const edit = params.get('edit'),
    isNew = params.has('new')
  return {
    params,
    update,
    edit,
    isNew,
    open: (id: string) => update({ edit: id, new: null }),
    create: () => update({ new: '1', edit: null }),
    close: () => update({ edit: null, new: null }, { replace: true }),
  }
}

interface ScheduleRow {
  schedule: Schedule
  next?: Date
  fires: RunRow[]
}

const RECENT_FIRES = 20

/** The latest cron runs across every schedule, newest first. */
function RecentFires({
  runs,
  loading,
  now,
  onSchedule,
}: {
  runs: RunRow[] | undefined
  loading: boolean
  now: number
  onSchedule: (job: string) => void
}) {
  const rows = useMemo(
    () =>
      runs && [...runs].sort((a, b) => ms(b.created_at) - ms(a.created_at)).slice(0, RECENT_FIRES),
    [runs],
  )
  const columns: Column<RunRow>[] = [
    {
      key: 'status',
      header: 'Status',
      width: 112,
      cell: (r) => <StatusBadge status={r.result_status} kind="run" />,
    },
    {
      key: 'schedule',
      header: 'Schedule',
      cell: (r) => {
        const job = parseCronTask(r.task_id)?.job
        return job ? (
          <button type="button" className="am-link truncate" onClick={() => onSchedule(job)}>
            {job}
          </button>
        ) : (
          <span className="muted mono truncate">{r.task_id}</span>
        )
      },
      title: (r) => r.preview || undefined,
    },
    {
      key: 'when',
      header: 'When',
      width: 120,
      cell: (r) => <span className="num">{relTime(r.created_at, now)}</span>,
      title: (r) => fullWhen(new Date(r.created_at), localZone),
    },
    {
      key: 'latency',
      header: 'Latency',
      width: 80,
      align: 'right',
      priority: 2,
      cell: (r) => <span className="num">{latency(r.latency_ms)}</span>,
    },
  ]
  return (
    <Panel
      title="Recent fires"
      meta={`latest ${RECENT_FIRES} cron runs`}
      to="/runs?source=cron"
      toLabel="All cron runs"
    >
      <DataTable
        label="Recent fires"
        columns={columns}
        rows={rows}
        rowKey={(r) => r.run_id}
        rowHref={(r) => `/runs/${encode(r.run_id)}`}
        loading={loading}
        sortParam={false}
        skeletonRows={5}
        empty="No cron runs in the recent run history"
      />
    </Panel>
  )
}

function SchedulesPage() {
  usePageTitle('Schedules')
  const schedules = useSchedules()
  const cronRuns = useRuns({ source: 'cron', limit: 500 })
  const sheet = useSheetParams()
  const q = sheet.params.get('q') || '',
    filter = sheet.params.get('filter') || 'all'
  const now = useNow(15000)
  const [notice, setNotice] = useSavedNotice()

  const firesByJob = useMemo(() => {
    const map = new Map<string, RunRow[]>()
    for (const run of cronRuns.data?.runs || []) {
      const job = parseCronTask(run.task_id)?.job
      if (!job) continue
      map.set(job, [...(map.get(job) || []), run])
    }
    return map
  }, [cronRuns.data])

  const all: ScheduleRow[] | undefined = useMemo(
    () =>
      schedules.data?.schedules.map((schedule) => ({
        schedule,
        next: nextRuns(schedule.cron, schedule.timezone, 1, new Date(now))[0],
        fires: firesByJob.get(schedule.job_name) || [],
      })),
    [schedules.data, firesByJob, now],
  )
  const failing = (row: ScheduleRow) => !!row.fires[0] && row.fires[0].result_status !== 'success'
  const dueSoon = (row: ScheduleRow) => !!row.next && row.next.getTime() - now < 3600000
  const filters: Record<string, (row: ScheduleRow) => boolean> = {
    all: () => true,
    failing,
    due: dueSoon,
    never: (row) => !row.fires.length,
  }
  const rows = all?.filter(
    (row) =>
      (filters[filter] || filters.all)(row) &&
      matches(
        q,
        row.schedule.job_name,
        row.schedule.content,
        row.schedule.cron,
        row.schedule.timezone,
        row.schedule.reply_channel_target,
      ),
  )
  const count = (key: string) => all?.filter(filters[key]).length ?? null

  const columns: Column<ScheduleRow>[] = [
    {
      key: 'name',
      header: 'Name',
      className: 'am-col-name',
      sortValue: (r) => r.schedule.job_name,
      title: (r) => `${r.schedule.job_name} — ${r.schedule.content}`,
      cell: (r) => (
        <>
          <span className="cell-primary">{r.schedule.job_name}</span>
          <span className="cell-secondary">{r.schedule.content}</span>
        </>
      ),
    },
    {
      key: 'cron',
      header: 'Schedule',
      className: 'am-col-cron',
      priority: 2,
      sortValue: (r) => r.schedule.cron,
      title: (r) =>
        `${describeCron(r.schedule.cron) || 'Invalid cron'} (${r.schedule.timezone || 'UTC'})`,
      cell: (r) => (
        <>
          <span className="am-human">{shortCron(r.schedule.cron)}</span>
          <span className="am-raw mono">{r.schedule.cron}</span>
        </>
      ),
    },
    {
      key: 'tz',
      header: 'Zone',
      className: 'am-col-tz',
      priority: 3,
      sortValue: (r) => r.schedule.timezone || 'UTC',
      cell: (r) => <span className="muted">{r.schedule.timezone || 'UTC'}</span>,
    },
    {
      key: 'next',
      header: 'Next run',
      className: 'am-col-next',
      sortValue: (r) => r.next?.getTime(),
      title: (r) =>
        r.next
          ? `${fullWhen(r.next, r.schedule.timezone)}\n${fullWhen(r.next, localZone)}`
          : undefined,
      cell: (r) =>
        r.next ? (
          <span className="num">
            <span className={dueSoon(r) ? 'am-soon' : 'am-fg'}>{relTime(r.next, now)}</span>
            <span className="am-sub"> · {zonedWhen(r.next, r.schedule.timezone, now)}</span>
          </span>
        ) : (
          <span className="am-warn">invalid</span>
        ),
    },
    {
      key: 'last',
      header: 'Last run',
      className: 'am-col-last',
      sortDescFirst: true,
      sortValue: (r) => (r.fires[0] ? ms(r.fires[0].created_at) : undefined),
      title: (r) =>
        r.fires[0]
          ? `${r.fires[0].result_status} · ${fullWhen(new Date(r.fires[0].created_at), localZone)}`
          : 'No runs in the recent run history',
      cell: (r) => {
        const run = r.fires[0]
        if (!run) return <span className="muted">{cronRuns.isPending ? '…' : '—'}</span>
        return (
          <span className="cell-flex num">
            <StatusDot status={run.result_status} kind="run" />
            <Link className="am-link" to={`/runs/${encode(run.run_id)}`}>
              {relTime(run.created_at, now)}
            </Link>
            <span className="am-sub" data-priority="3">
              {latency(run.latency_ms)}
            </span>
          </span>
        )
      },
    },
    {
      key: 'target',
      header: 'Target',
      className: 'am-col-target',
      priority: 3,
      sortValue: (r) => r.schedule.reply_channel_target,
      cell: (r) => (
        <TargetCell type={r.schedule.reply_channel_type} target={r.schedule.reply_channel_target} />
      ),
    },
  ]

  return (
    <div className="am-page">
      <TopbarActions>
        <Button variant="outline" size="sm" onClick={sheet.create}>
          <Plus />
          New schedule
        </Button>
      </TopbarActions>
      <div className="toolbar am-toolbar">
        <FilterChips
          label="Filter schedules"
          value={filter}
          onChange={(value) => sheet.update({ filter: value === 'all' ? null : value })}
          options={[
            { value: 'all', label: 'Active', count: count('all') },
            { value: 'failing', label: 'Last run failed', count: count('failing'), tone: 'danger' },
            { value: 'due', label: 'Due < 1h', count: count('due'), tone: 'pending' },
            { value: 'never', label: 'No recent runs', count: count('never') },
          ]}
        />
        <SearchInput
          value={q}
          onChange={(value) => sheet.update({ q: value }, { replace: true })}
          placeholder="Search schedules…"
        />
        <div className="toolbar-end">{notice}</div>
      </div>
      <Panel>
        <ErrorStrip query={schedules} label="Handler" />
        <ErrorStrip query={cronRuns} label="Run history" tone="warn" />
        <DataTable
          label="Schedules"
          columns={columns}
          rows={rows}
          rowKey={(r) => r.schedule.schedule_id}
          loading={schedules.isPending}
          onRowClick={(r) => sheet.open(r.schedule.schedule_id)}
          selectedKey={sheet.edit}
          defaultSort="next"
          rowVersion={(r) => `${r.schedule.version}:${r.fires[0]?.run_id || ''}`}
          className="am-table"
          empty={
            q || filter !== 'all' ? (
              <>
                No schedules match ·{' '}
                <button
                  type="button"
                  className="am-link"
                  onClick={() => sheet.update({ q: null, filter: null })}
                >
                  Clear filters
                </button>
              </>
            ) : (
              <>
                No schedules ·{' '}
                <button type="button" className="am-link" onClick={sheet.create}>
                  New schedule
                </button>
              </>
            )
          }
        />
      </Panel>
      <RecentFires
        runs={cronRuns.data?.runs}
        loading={cronRuns.isPending}
        now={now}
        onSchedule={(job) => {
          const match = schedules.data?.schedules.find((s) => s.job_name === job)
          if (match) sheet.open(match.schedule_id)
        }}
      />
      <AutomationSheet
        kind="schedules"
        sheet={sheet}
        fires={(record) => (isSchedule(record) ? firesByJob.get(record.job_name) || [] : [])}
        onSaved={setNotice}
      />
    </div>
  )
}

interface ReminderRow {
  reminder: Reminder
  overdue: boolean
  run?: RunRow
}

function RemindersPage() {
  usePageTitle('Reminders')
  const reminders = useReminders()
  const reminderRuns = useRuns({ source: 'reminder', limit: 500 })
  const sheet = useSheetParams()
  const q = sheet.params.get('q') || '',
    tab = sheet.params.get('tab') === 'history' ? 'history' : 'upcoming'
  const now = useNow(15000)
  const [notice, setNotice] = useSavedNotice()

  const runsByReminder = useMemo(() => {
    const map = new Map<string, RunRow[]>()
    for (const run of reminderRuns.data?.runs || []) {
      const parsed = parseReminderTask(run.task_id)
      if (!parsed) continue
      map.set(parsed.reminderId, [...(map.get(parsed.reminderId) || []), run])
    }
    return map
  }, [reminderRuns.data])
  const all: ReminderRow[] | undefined = useMemo(() => {
    const runFor = (reminder: Reminder) => {
      const runs = runsByReminder.get(reminder.reminder_id) || []
      return (
        runs.find((run) => parseReminderTask(run.task_id)?.revision === reminder.revision) ||
        runs[0]
      )
    }
    return reminders.data?.reminders.map((reminder) => ({
      reminder,
      overdue: reminder.status === 'scheduled' && ms(reminder.run_at) < now,
      run: reminder.status === 'fired' ? runFor(reminder) : undefined,
    }))
  }, [reminders.data, runsByReminder, now])
  const upcoming = all?.filter((row) => row.reminder.status === 'scheduled')
  const history = all?.filter((row) => row.reminder.status !== 'scheduled')
  const overdueCount = upcoming?.filter((row) => row.overdue).length ?? 0
  const rows = (tab === 'history' ? history : upcoming)?.filter((row) =>
    matches(
      q,
      row.reminder.prompt,
      row.reminder.reminder_id,
      row.reminder.reply_channel.target,
      row.reminder.status,
    ),
  )

  const prompt: Column<ReminderRow> = {
    key: 'prompt',
    header: 'Prompt',
    className: 'am-col-prompt',
    sortValue: (r) => r.reminder.prompt,
    title: (r) => r.reminder.prompt,
    cell: (r) => <span className="cell-primary">{r.reminder.prompt}</span>,
  }
  const when: Column<ReminderRow> = {
    key: 'when',
    header: 'When',
    className: 'am-col-when',
    sortValue: (r) => ms(r.reminder.run_at),
    title: (r) => fullWhen(new Date(r.reminder.run_at), localZone),
    cell: (r) => <span className="num am-fg">{localWhen(r.reminder.run_at)}</span>,
  }
  const target: Column<ReminderRow> = {
    key: 'target',
    header: 'Target',
    className: 'am-col-target',
    priority: 3,
    sortValue: (r) => r.reminder.reply_channel.target,
    cell: (r) => (
      <TargetCell type={r.reminder.reply_channel.type} target={r.reminder.reply_channel.target} />
    ),
  }
  const revision: Column<ReminderRow> = {
    key: 'rev',
    header: 'Rev',
    className: 'am-col-ver',
    align: 'right',
    priority: 3,
    sortValue: (r) => r.reminder.revision,
    cell: (r) => <span className="num muted">{r.reminder.revision}</span>,
  }
  const upcomingColumns: Column<ReminderRow>[] = [
    when,
    {
      key: 'in',
      header: 'In',
      className: 'am-col-in',
      sortValue: (r) => ms(r.reminder.run_at),
      cell: (r) =>
        r.overdue ? (
          <span className="num am-warn">
            overdue {relTime(r.reminder.run_at, now).replace(' ago', '')}
          </span>
        ) : (
          <span className="num">{relTime(r.reminder.run_at, now)}</span>
        ),
    },
    prompt,
    target,
    revision,
    {
      key: 'status',
      header: 'Status',
      className: 'am-col-status',
      priority: 2,
      sortValue: (r) => (r.overdue ? 0 : 1),
      cell: (r) => <StatusBadge status={r.overdue ? 'overdue' : 'scheduled'} kind="reminder" />,
    },
  ]
  const historyColumns: Column<ReminderRow>[] = [
    when,
    {
      key: 'outcome',
      header: 'Outcome',
      className: 'am-col-status',
      sortValue: (r) => r.reminder.status,
      cell: (r) => <StatusBadge status={r.reminder.status} kind="reminder" />,
    },
    {
      key: 'at',
      header: 'At',
      className: 'am-col-in',
      priority: 2,
      sortDescFirst: true,
      sortValue: (r) => ms(r.reminder.fired_at || r.reminder.cancelled_at || r.reminder.updated_at),
      title: (r) =>
        localWhen(r.reminder.fired_at || r.reminder.cancelled_at || r.reminder.updated_at),
      cell: (r) => (
        <span className="num">
          {relTime(r.reminder.fired_at || r.reminder.cancelled_at || r.reminder.updated_at, now)}
        </span>
      ),
    },
    prompt,
    {
      key: 'run',
      header: 'Run',
      className: 'am-col-last',
      priority: 2,
      sortValue: (r) => r.run?.result_status,
      cell: (r) =>
        r.run ? (
          <span className="cell-flex">
            <StatusDot status={r.run.result_status} kind="run" />
            <Link className="am-link num" to={`/runs/${encode(r.run.run_id)}`}>
              {latency(r.run.latency_ms)}
            </Link>
            <span className="am-sub">
              {r.run.result_status === 'success' ? 'ok' : r.run.result_status.replaceAll('_', ' ')}
            </span>
          </span>
        ) : (
          <span className="muted">—</span>
        ),
    },
    target,
    revision,
  ]

  return (
    <div className="am-page">
      <TopbarActions>
        <Button variant="outline" size="sm" onClick={sheet.create}>
          <Plus />
          New reminder
        </Button>
      </TopbarActions>
      <div className="toolbar am-toolbar">
        <FilterChips
          label="Reminder tab"
          value={tab}
          onChange={(value) =>
            sheet.update({ tab: value === 'upcoming' ? null : value, sort: null })
          }
          options={[
            {
              value: 'upcoming',
              label: overdueCount ? (
                <>
                  Upcoming <span className="am-warn">· {overdueCount} overdue</span>
                </>
              ) : (
                'Upcoming'
              ),
              count: upcoming?.length ?? null,
              tone: overdueCount ? 'warn' : undefined,
            },
            { value: 'history', label: 'History', count: history?.length ?? null },
          ]}
        />
        <SearchInput
          value={q}
          onChange={(value) => sheet.update({ q: value }, { replace: true })}
          placeholder="Search reminders…"
        />
        <div className="toolbar-end">{notice}</div>
      </div>
      <Panel>
        <ErrorStrip query={reminders} label="Handler" />
        <DataTable
          key={tab}
          label={tab === 'history' ? 'Reminder history' : 'Upcoming reminders'}
          columns={tab === 'history' ? historyColumns : upcomingColumns}
          rows={rows}
          rowKey={(r) => r.reminder.reminder_id}
          loading={reminders.isPending}
          onRowClick={(r) => sheet.open(r.reminder.reminder_id)}
          selectedKey={sheet.edit}
          defaultSort={tab === 'history' ? '-when' : 'when'}
          rowVersion={(r) => `${r.reminder.revision}:${r.reminder.status}`}
          className="am-table"
          empty={
            q ? (
              <>
                No reminders match ·{' '}
                <button type="button" className="am-link" onClick={() => sheet.update({ q: null })}>
                  Clear search
                </button>
              </>
            ) : tab === 'history' ? (
              'No past reminders'
            ) : (
              <>
                No upcoming reminders ·{' '}
                <button type="button" className="am-link" onClick={sheet.create}>
                  New reminder
                </button>
              </>
            )
          }
        />
      </Panel>
      <AutomationSheet
        kind="reminders"
        sheet={sheet}
        fires={(record) => (isSchedule(record) ? [] : runsByReminder.get(record.reminder_id) || [])}
        onSaved={setNotice}
      />
    </div>
  )
}

function AutomationSheet({
  kind,
  sheet,
  fires,
  onSaved,
}: {
  kind: Kind
  sheet: ReturnType<typeof useSheetParams>
  fires: (record: RecordType) => RunRow[]
  onSaved: (message: string) => void
}) {
  const { edit: selectedID, isNew } = sheet
  const recurring = kind === 'schedules'
  const detail = useQuery({
    queryKey: [kind, selectedID],
    queryFn: () =>
      api<{ schedule?: Schedule; reminder?: Reminder; history: RecordType[] }>(
        `handler/${kind}/${encode(selectedID!)}?history=1`,
      ),
    enabled: !!selectedID,
  })
  const selected = detail.data?.schedule || detail.data?.reminder
  const open = isNew || !!selectedID
  const noun = recurring ? 'schedule' : 'reminder'
  return (
    <Sheet
      open={open}
      onOpenChange={(value) => {
        if (!value) sheet.close()
      }}
    >
      {open &&
        (!isNew && !selected ? (
          <SheetContent
            title={recurring ? 'Schedule' : 'Reminder'}
            description={<span className="mono">{selectedID}</span>}
          >
            {detail.error ? (
              <ErrorStrip query={detail} label="Handler" />
            ) : (
              <SkeletonRows rows={8} />
            )}
          </SheetContent>
        ) : (
          <AutomationForm
            key={selectedID || 'new'}
            kind={kind}
            record={isNew ? undefined : selected}
            history={detail.data?.history || []}
            fires={selected ? fires(selected) : []}
            title={
              isNew
                ? `New ${noun}`
                : selected && isSchedule(selected)
                  ? selected.job_name
                  : selected?.prompt || 'Reminder'
            }
            onClose={sheet.close}
            onSaved={(message) => {
              sheet.close()
              onSaved(message)
            }}
          />
        ))}
    </Sheet>
  )
}

function Section({
  title,
  meta,
  children,
}: {
  title: string
  meta?: ReactNode
  children: ReactNode
}) {
  return (
    <section className="am-section">
      <h3>
        {title}
        {meta != null && <span className="am-sub">{meta}</span>}
      </h3>
      {children}
    </section>
  )
}

function Field({
  id,
  label,
  hint,
  error,
  wide,
  children,
}: {
  id: string
  label: ReactNode
  hint?: ReactNode
  error?: string | null
  wide?: boolean
  children: ReactNode
}) {
  return (
    <div className="am-field" data-wide={wide || undefined}>
      <Label htmlFor={id}>{label}</Label>
      {children}
      {error ? (
        <p className="am-hint am-warn" id={`${id}-error`}>
          {error}
        </p>
      ) : (
        hint && <p className="am-hint">{hint}</p>
      )}
    </div>
  )
}

function changedFields(row: RecordType, previous?: RecordType) {
  if (!previous) return 'created'
  const keys: [string, (r: RecordType) => unknown][] = isSchedule(row)
    ? [
        ['name', (r) => (r as Schedule).job_name],
        ['prompt', (r) => (r as Schedule).content],
        ['cron', (r) => (r as Schedule).cron],
        ['zone', (r) => (r as Schedule).timezone],
        [
          'target',
          (r) => `${(r as Schedule).reply_channel_type}:${(r as Schedule).reply_channel_target}`,
        ],
        ['context', (r) => JSON.stringify([r.context_refs, r.prompt_context])],
      ]
    : [
        ['time', (r) => (r as Reminder).run_at],
        ['prompt', (r) => (r as Reminder).prompt],
        ['target', (r) => JSON.stringify((r as Reminder).reply_channel)],
        ['context', (r) => JSON.stringify([r.context_refs, r.prompt_context])],
      ]
  const changed = keys.filter(([, get]) => get(row) !== get(previous)).map(([name]) => name)
  return changed.length ? changed.join(', ') : 'no field changes'
}

function formSnapshot(form: HTMLFormElement) {
  return JSON.stringify([...new FormData(form)].map(([key, value]) => [key, String(value).trim()]))
}

function AutomationForm({
  kind,
  record,
  history,
  fires,
  title,
  onClose,
  onSaved,
}: {
  kind: Kind
  record?: RecordType
  history: RecordType[]
  fires: RunRow[]
  title: string
  onClose: () => void
  onSaved: (message: string) => void
}) {
  const recurring = kind === 'schedules',
    schedule = record && isSchedule(record) ? record : undefined,
    reminder = record && !isSchedule(record) ? record : undefined
  const readOnly = !!reminder && reminder.status !== 'scheduled'
  const [busy, setBusy] = useState(false),
    [error, setError] = useState<Error | null>(null),
    [confirm, setConfirm] = useState(false)
  const [cron, setCron] = useState(schedule?.cron || ''),
    [timezone, setTimezone] = useState(schedule?.timezone || (record ? '' : localZone))
  const client = useQueryClient()
  const toast = useToast()
  const submission = useRef({ fingerprint: '', key: '' })
  const now = useNow(30000)
  const formID = `${kind}-form`
  const cronProblem = cron.trim() ? cronError(cron) : null,
    zoneProblem = timezoneError(timezone)
  const upcoming = recurring ? nextRuns(cron, timezone, 5, new Date(now)) : []
  const overdue = !!reminder && reminder.status === 'scheduled' && ms(reminder.run_at) < now
  const [when, setWhen] = useState(() =>
    reminder ? localDateTime(reminder.run_at).replace('T', ' ') : '',
  )
  const whenDate = parseLocalDateTime(when)
  const whenProblem = when.trim() && !whenDate ? 'Use YYYY-MM-DD HH:MM (24h)' : null

  const formRef = useRef<HTMLFormElement>(null)
  const initial = useRef<string | null>(null)
  const [dirty, setDirty] = useState(false)
  useEffect(() => {
    if (formRef.current) initial.current = formSnapshot(formRef.current)
  }, [])
  const checkDirty = () => {
    if (formRef.current && initial.current != null)
      setDirty(formSnapshot(formRef.current) !== initial.current)
  }
  const unchanged = !!record && !dirty

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    if (unchanged) return
    const data = new FormData(event.currentTarget),
      value = (key: string) => String(data.get(key) || '').trim()
    let payload: Record<string, unknown>
    try {
      const common = {
        context_refs: lines(data.get('context_refs')),
        prompt_context: lines(data.get('prompt_context')),
        created_by: record?.created_by || 'platform',
      }
      if (recurring) {
        const problem = cronError(value('cron')) || timezoneError(value('timezone'))
        if (problem) throw Error(problem)
        payload = {
          ...common,
          job_name: value('job_name'),
          content: value('content'),
          cron: value('cron'),
          timezone: value('timezone') || 'UTC',
          reply_channel_type: value('channel_type'),
          reply_channel_target: value('channel_target'),
        }
      } else {
        const metadata: unknown = JSON.parse(value('metadata') || '{}')
        if (!metadata || Array.isArray(metadata) || typeof metadata !== 'object')
          throw Error('Reply metadata must be a JSON object.')
        payload = {
          ...common,
          run_at: (() => {
            const at = parseLocalDateTime(value('run_at'))
            if (!at) throw Error('When must be YYYY-MM-DD HH:MM (24h).')
            return at.toISOString()
          })(),
          prompt: value('prompt'),
          reply_channel: { type: value('channel_type'), target: value('channel_target'), metadata },
          created_from_task_id: reminder?.created_from_task_id || 'platform',
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e : Error('Check the form values.'))
      return
    }
    if (!recurring) {
      const fingerprint = JSON.stringify(payload)
      if (submission.current.fingerprint !== fingerprint)
        submission.current = {
          fingerprint,
          key:
            'platform:' +
            Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) =>
              b.toString(16).padStart(2, '0'),
            ).join(''),
        }
      payload.idempotency_key = submission.current.key
    }
    setBusy(true)
    try {
      await api(`handler/${kind}${record ? '/' + encode(recordID(record)) : ''}`, {
        method: record ? 'PUT' : 'POST',
        body: JSON.stringify(payload),
      })
      await client.invalidateQueries({ queryKey: [kind] })
      onSaved(`${recurring ? 'Schedule' : 'Reminder'} ${record ? 'updated' : 'created'}`)
    } catch (e) {
      setError(e instanceof Error ? e : Error('Could not save.'))
      toast(`Save failed${e instanceof Error ? `: ${e.message}` : ''}`, { tone: 'danger' })
    } finally {
      setBusy(false)
    }
  }
  async function remove() {
    if (!record) return
    setBusy(true)
    setError(null)
    try {
      await api(`handler/${kind}/${encode(recordID(record))}`, { method: 'DELETE' })
      await client.invalidateQueries({ queryKey: [kind] })
      onSaved(recurring ? 'Schedule deleted' : 'Reminder cancelled')
    } catch (e) {
      setError(e instanceof Error ? e : Error('Could not remove.'))
      toast(
        `${recurring ? 'Delete' : 'Cancel'} failed${e instanceof Error ? `: ${e.message}` : ''}`,
        {
          tone: 'danger',
        },
      )
      setConfirm(false)
    } finally {
      setBusy(false)
    }
  }

  const facts: Fact[] = schedule
    ? [
        { label: 'Status', value: <StatusBadge status={schedule.status} kind="schedule" /> },
        { label: 'Version', value: schedule.version },
        {
          label: 'Next',
          value: upcoming[0] ? relTime(upcoming[0], now) : '—',
          title: upcoming[0] ? fullWhen(upcoming[0], schedule.timezone) : undefined,
        },
        {
          label: 'Last',
          value: fires[0] ? relTime(fires[0].created_at, now) : '—',
          tone: fires[0] && fires[0].result_status !== 'success' ? 'danger' : undefined,
        },
        { label: 'Updated', value: localWhen(schedule.created_at) },
        { label: 'By', value: schedule.created_by },
      ]
    : reminder
      ? [
          {
            label: 'Status',
            value: overdue ? (
              <StatusBadge status="overdue" kind="reminder" />
            ) : (
              <StatusBadge status={reminder.status} kind="reminder" />
            ),
          },
          { label: 'Revision', value: reminder.revision },
          {
            label: reminder.status === 'scheduled' ? 'Due' : 'Was due',
            value: relTime(reminder.run_at, now),
            tone: overdue ? 'warn' : undefined,
          },
          ...(reminder.fired_at
            ? [{ label: 'Fired', value: localWhen(reminder.fired_at) }]
            : reminder.cancelled_at
              ? [{ label: 'Cancelled', value: localWhen(reminder.cancelled_at) }]
              : []),
          { label: 'By', value: reminder.created_by },
          {
            label: 'From',
            value: reminder.created_from_task_id,
            mono: true,
            title: reminder.created_from_task_id,
          },
        ]
      : []

  const footer = (
    <>
      {record && !readOnly && (
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="am-danger"
          disabled={busy}
          onClick={() => setConfirm(true)}
        >
          <Trash2 />
          {recurring ? 'Delete' : 'Cancel reminder'}
        </Button>
      )}
      <span className="am-spacer" />
      {!readOnly && unchanged && <span className="am-clean">No changes</span>}
      <Button type="button" variant="outline" size="sm" disabled={busy} onClick={onClose}>
        {readOnly || unchanged ? 'Close' : 'Discard'}
      </Button>
      {!readOnly && (
        <Button type="submit" size="sm" form={formID} disabled={busy || unchanged}>
          {busy && <LoaderCircle className="animate-spin" />}
          {record ? 'Save changes' : recurring ? 'Create schedule' : 'Create reminder'}
        </Button>
      )}
    </>
  )

  return (
    <SheetContent
      title={title}
      description={
        record ? (
          <span className="mono">{recordID(record)}</span>
        ) : recurring ? (
          'Recurring task'
        ) : (
          `Times in ${localZone}`
        )
      }
      footer={footer}
      className="am-sheet"
    >
      <div className="am-sheet-body">
        {record && <FactsRow items={facts} />}
        <ErrorStrip error={error} message="Couldn’t save" />
        <form
          id={formID}
          ref={formRef}
          onSubmit={save}
          onInput={checkDirty}
          onChange={checkDirty}
          className="am-form"
        >
          <fieldset disabled={busy || readOnly}>
            {recurring ? (
              <>
                <Field id="job-name" label="Name" wide>
                  <Input
                    id="job-name"
                    name="job_name"
                    required
                    defaultValue={schedule?.job_name}
                    placeholder="Morning briefing"
                  />
                </Field>
                <Field id="content" label="Prompt" wide>
                  <Textarea
                    id="content"
                    name="content"
                    required
                    defaultValue={schedule?.content}
                    rows={3}
                  />
                </Field>
                <Field
                  id="cron"
                  label="Cron"
                  error={cronProblem}
                  hint={describeCron(cron) || 'minute hour day month weekday'}
                >
                  <Input
                    className="font-mono"
                    id="cron"
                    name="cron"
                    required
                    value={cron}
                    onChange={(e) => setCron(e.target.value)}
                    aria-invalid={!!cronProblem || undefined}
                    aria-describedby={cronProblem ? 'cron-error' : undefined}
                    placeholder="0 9 * * 1-5"
                  />
                </Field>
                <Field
                  id="timezone"
                  label="Timezone"
                  error={zoneProblem}
                  hint="IANA name, e.g. Europe/London"
                >
                  <Input
                    id="timezone"
                    name="timezone"
                    value={timezone}
                    onChange={(e) => setTimezone(e.target.value)}
                    aria-invalid={!!zoneProblem || undefined}
                    aria-describedby={zoneProblem ? 'timezone-error' : undefined}
                    placeholder="UTC"
                    list="am-timezones"
                  />
                  <datalist id="am-timezones">
                    {[
                      localZone,
                      'UTC',
                      'Europe/London',
                      'Europe/Berlin',
                      'America/New_York',
                      'America/Los_Angeles',
                      'Asia/Tokyo',
                    ]
                      .filter((zone, i, list) => list.indexOf(zone) === i)
                      .map((zone) => (
                        <option key={zone} value={zone} />
                      ))}
                  </datalist>
                </Field>
              </>
            ) : (
              <>
                <Field id="prompt" label="Prompt" wide>
                  <Textarea
                    id="prompt"
                    name="prompt"
                    required
                    defaultValue={reminder?.prompt}
                    rows={3}
                  />
                </Field>
                <Field
                  id="run-at"
                  label="When"
                  error={whenProblem}
                  hint={
                    whenDate
                      ? `${relTime(whenDate, now)} · ${localZone}`
                      : `YYYY-MM-DD HH:MM, 24h ${localZone}`
                  }
                  wide
                >
                  <Input
                    id="run-at"
                    name="run_at"
                    className="font-mono"
                    inputMode="numeric"
                    autoComplete="off"
                    required
                    value={when}
                    onChange={(e) => setWhen(e.target.value)}
                    aria-invalid={!!whenProblem || undefined}
                    aria-describedby={whenProblem ? 'run-at-error' : undefined}
                    placeholder="2026-11-20 09:00"
                  />
                </Field>
              </>
            )}
            <Field id="channel-type" label={`Reply channel${recurring ? '' : ' *'}`}>
              <Input
                id="channel-type"
                name="channel_type"
                required={!recurring}
                defaultValue={schedule?.reply_channel_type || reminder?.reply_channel.type || ''}
                placeholder="telegram"
              />
            </Field>
            <Field id="channel-target" label={`Reply target${recurring ? '' : ' *'}`}>
              <Input
                id="channel-target"
                name="channel_target"
                className="font-mono"
                required={!recurring}
                defaultValue={
                  schedule?.reply_channel_target || reminder?.reply_channel.target || ''
                }
                placeholder="Chat or channel ID"
              />
            </Field>
            <Field
              id="context-refs"
              label="Context references"
              hint="One per line, repo:/absolute/path"
              wide
            >
              <Textarea
                id="context-refs"
                name="context_refs"
                className="font-mono"
                defaultValue={record?.context_refs.join('\n')}
                placeholder="repo:/absolute/path"
                rows={2}
              />
            </Field>
            <Field id="prompt-context" label="Prompt context" hint="One entry per line" wide>
              <Textarea
                id="prompt-context"
                name="prompt_context"
                defaultValue={record?.prompt_context.join('\n')}
                rows={2}
              />
            </Field>
            {!recurring && (
              <Field id="metadata" label="Reply metadata (JSON)" wide>
                <Textarea
                  className="font-mono"
                  id="metadata"
                  name="metadata"
                  defaultValue={JSON.stringify(reminder?.reply_channel.metadata || {}, null, 2)}
                  rows={3}
                />
              </Field>
            )}
          </fieldset>
        </form>

        {recurring && (
          <Section title="Next 5 runs" meta={timezone || 'UTC'}>
            {upcoming.length ? (
              <ol className="am-list">
                {upcoming.map((date) => (
                  <li key={date.getTime()} title={fullWhen(date, localZone)}>
                    <span className="num am-fg">{fullWhen(date, timezone || 'UTC')}</span>
                    <span className="am-end num muted">{relTime(date, now)}</span>
                  </li>
                ))}
              </ol>
            ) : (
              <p className="am-empty">
                {cron.trim()
                  ? 'Fix the cron expression to preview runs'
                  : 'Enter a cron expression'}
              </p>
            )}
          </Section>
        )}

        {record && (
          <Section title={recurring ? 'Recent fires' : 'Runs'} meta={fires.length || null}>
            {fires.length ? (
              <ol className="am-list">
                {fires.slice(0, 10).map((run) => {
                  const scheduled = parseCronTask(run.task_id)?.scheduledFor
                  return (
                    <li key={run.run_id}>
                      <StatusDot status={run.result_status} kind="run" />
                      <Link className="am-link num" to={`/runs/${encode(run.run_id)}`}>
                        {localWhen(scheduled?.toISOString() || run.created_at)}
                      </Link>
                      <span className="am-sub">
                        {run.result_status === 'success'
                          ? 'success'
                          : run.result_status.replaceAll('_', ' ')}
                      </span>
                      <span className="am-end num muted">{latency(run.latency_ms)}</span>
                    </li>
                  )
                })}
              </ol>
            ) : (
              <p className="am-empty">No runs in recent history</p>
            )}
          </Section>
        )}

        {record && (
          <Section title="Revisions" meta={history.length || null}>
            <ol className="am-list">
              {history.map((row, i) => {
                const version = isSchedule(row) ? row.version : row.revision
                return (
                  <li
                    key={version}
                    data-current={
                      version === (schedule?.version ?? reminder?.revision) || undefined
                    }
                  >
                    <span className="num am-ver">
                      {isSchedule(row) ? `v${version}` : `r${version}`}
                    </span>
                    <StatusDot
                      status={row.status}
                      kind={isSchedule(row) ? 'schedule' : 'reminder'}
                      tone={i > 0 ? 'neutral' : undefined}
                    />
                    <span className="am-rev-status">{row.status}</span>
                    <span
                      className="am-rev-what truncate"
                      title={isSchedule(row) ? `${row.cron} · ${row.content}` : row.prompt}
                    >
                      {changedFields(row, history[i + 1])}
                    </span>
                    <span className="am-end num muted">
                      {localWhen(isSchedule(row) ? row.created_at : row.updated_at)}
                    </span>
                  </li>
                )
              })}
            </ol>
          </Section>
        )}
      </div>
      <AlertDialog open={confirm} onOpenChange={setConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {recurring ? 'Delete this schedule?' : 'Cancel this reminder?'}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {recurring
                ? 'Future runs of this schedule will stop. Its revision history will be retained.'
                : 'This reminder will not fire. It will remain in your history.'}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busy}>Keep it</AlertDialogCancel>
            <Button
              type="button"
              variant="destructive"
              disabled={busy}
              onClick={() => {
                void remove()
              }}
            >
              {busy ? 'Working…' : recurring ? 'Delete schedule' : 'Cancel reminder'}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </SheetContent>
  )
}
