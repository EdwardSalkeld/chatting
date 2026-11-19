import { useMemo, type ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  ErrorStrip,
  ErrorText,
  FactsRow,
  Panel,
  Sparkline,
  Stat,
  StatusBadge,
  TopbarActions,
  shortAge,
  useNow,
  usePageTitle,
  type Fact,
  type Tone,
} from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import {
  ApiError,
  useActivity,
  useDayRuns,
  useDeadLetters,
  useHandlerMetrics,
  useInbox,
  useItems,
  useReminders,
  useRuns,
  useSchedules,
  useWorkerMetrics,
} from '@/lib/api'
import type { Executor, RunRow, WorkItem } from '@/lib/types'
import {
  encode,
  fmtBytes,
  fmtLatency,
  fmtTokens,
  fmtUptime,
  fmtWhen,
  fullTime,
  hourlyBuckets,
  hourlyConcurrency,
  itemName,
  timeMs,
} from '@/lib/format'
import { statusLabel, statusTone } from '@/lib/status'
import { useUrlState } from '@/lib/url'
import { RunStatus, SourceLabel, runError, tokenTitle } from '@/components/runs'
import { nextRuns } from '@/lib/cron'
import { parseProm, promSum, promValue } from '@/lib/prom'
import { handlerHealth } from '@/lib/health'
import '@/styles/overview.css'

const DAY = 86400e3
// Caps keep all five panels starting above the fold at 1440x900.
const ATTENTION_ROWS = 6,
  RECENT_ROWS = 5,
  UPCOMING_ROWS = 6
const time = timeMs

export function fmtNext(at: number, now: number) {
  return fmtWhen(at, { now })
}

export function nextCronFire(cron: string, timezone: string, now: number) {
  return nextRuns(cron, timezone, 1, new Date(now))[0]?.getTime()
}

const loopTone = (age: number | undefined): Tone =>
  age == null ? 'neutral' : age > 300e3 ? 'danger' : age > 60e3 ? 'warn' : 'ok'

interface Attention {
  key: string
  status: string
  tone: Tone
  label: string
  title: string
  secondary?: string
  source?: string | null
  detail?: string
  /** error: red mono; code: mono (ids, targets); text: sans prose. */
  detailKind?: 'error' | 'code' | 'text'
  /** Row tooltip, e.g. the request prompt behind a failed run. */
  tooltip?: string
  /** 0 for rows describing current state, shown as "now" unless `ageText` says more. */
  at: number
  ageText?: string
  href: string
}
const TONE_RANK: Record<Tone, number> = {
  danger: 0,
  warn: 1,
  live: 2,
  pending: 3,
  ok: 4,
  neutral: 5,
}

interface Upcoming {
  key: string
  kind: 'cron' | 'reminder'
  at: number
  title: string
  detail: string
  timezone?: string
  href: string
}

type Activity =
  | { kind: 'live'; key: string; executor: Executor }
  | { kind: 'queued'; key: string; item: WorkItem; position: number }

interface InboxCategory {
  category: string
  total: number
  day: number
  latest: number
}

const age = (now: number, at: number) => (at ? shortAge(Math.abs(now - at)) : '—')

export function Overview() {
  usePageTitle('Overview')
  const now = useNow(1000)
  const minute = Math.floor(now / 60000) * 60000
  const [attentionView, setAttentionView] = useUrlState('attention')

  const items = useItems()
  const activity = useActivity()
  const day = useDayRuns()
  const recent = useRuns({ limit: RECENT_ROWS })
  const deadLetters = useDeadLetters()
  const reminders = useReminders()
  const schedules = useSchedules()
  const inbox = useInbox()
  const handlerMetrics = useHandlerMetrics()
  const workerMetrics = useWorkerMetrics()

  const itemMap = useMemo(
    () => new Map((items.data?.items || []).map((item) => [item.work_item_id, item])),
    [items.data],
  )
  const nameOf = (id: string | null | undefined, fallback = '—') => {
    const item = id ? itemMap.get(id) : undefined
    return item ? itemName(item) : id || fallback
  }

  const live = items.data?.active_executors ?? activity.data?.active_executors
  // Oldest first, which is the order the worker claims them in.
  const queuedItems = (items.data?.items || [])
    .filter((item) => item.queued > 0)
    .sort((a, b) => time(a.oldest_queued_at) - time(b.oldest_queued_at))
  const oldestQueued = activity.data?.queue.oldest_queued_at
  const dayRuns = day.data?.runs
  const dayErrors = useMemo(
    () => dayRuns?.filter((run) => run.result_status !== 'success'),
    [dayRuns],
  )
  const pendingDead = useMemo(
    () => deadLetters.data?.dead_letters.filter((d) => !d.replayed_run_id),
    [deadLetters.data],
  )
  const hm = useMemo(() => parseProm(handlerMetrics.data || ''), [handlerMetrics.data])
  const wm = useMemo(() => parseProm(workerMetrics.data || ''), [workerMetrics.data])
  const health = handlerHealth(hm, now)
  const lastLoop = promValue(hm, 'chatting_message_handler_last_loop_completed_timestamp_seconds')
  const loopAge = lastLoop ? Math.max(0, now - lastLoop * 1000) : undefined
  const inboxMissing = inbox.error instanceof ApiError && inbox.error.status === 404
  const outbox = activity.data?.outbox

  const attention = (() => {
    const rows: Attention[] = health.issues.map((issue) => ({
      key: `health:${issue.key}`,
      status: 'error',
      tone: 'danger',
      label: 'Error',
      title: issue.title,
      detail: issue.detail,
      detailKind: 'text',
      source: issue.key === 'telegram' ? 'telegram' : 'internal',
      at: 0,
      href: '/system',
    }))
    for (const dl of pendingDead || [])
      rows.push({
        key: `dl:${dl.dead_letter_id}`,
        status: 'dead_letter',
        tone: 'danger',
        label: 'Dead letter',
        title: nameOf(dl.work_item_id, dl.task_id || dl.run_id),
        secondary: `${dl.attempt_count} attempts`,
        source: dl.source,
        detail:
          [dl.last_error, dl.reason_codes.join(', ')].filter(Boolean).join(' · ') ||
          'No error recorded',
        detailKind: dl.last_error || dl.reason_codes.length ? 'error' : 'text',
        at: time(dl.created_at),
        href: `/runs/${encode(dl.run_id)}`,
      })
    for (const run of dayErrors || []) {
      if (run.result_status === 'dead_letter') continue
      const recovered = dayRuns?.some(
        (other) =>
          other.work_item_id === run.work_item_id &&
          other.result_status === 'success' &&
          time(other.created_at) > time(run.created_at),
      )
      rows.push({
        key: `run:${run.run_id}`,
        status: recovered ? 'recovered' : run.result_status,
        tone: recovered ? 'neutral' : statusTone(run.result_status),
        label: statusLabel(recovered ? 'recovered' : run.result_status),
        title: nameOf(run.work_item_id, run.task_id),
        secondary: recovered ? 'later run ok' : undefined,
        source: run.source,
        detail: runError(run) || 'No error recorded',
        detailKind: runError(run) ? 'error' : 'text',
        tooltip: run.preview ? `Request: ${run.preview}` : undefined,
        at: time(run.created_at),
        href: `/runs/${encode(run.run_id)}`,
      })
    }
    for (const reminder of reminders.data?.reminders || []) {
      const due = time(reminder.run_at)
      if (reminder.status !== 'scheduled' || !due || due > minute) continue
      rows.push({
        key: `rem:${reminder.reminder_id}`,
        status: 'overdue',
        tone: 'warn',
        label: 'Overdue',
        title: reminder.prompt,
        secondary: `due ${fmtWhen(reminder.run_at)}`,
        source: 'reminder',
        detail: `${reminder.reply_channel.type} ${reminder.reply_channel.target}`,
        detailKind: 'code',
        at: due,
        href: `/reminders?edit=${encode(reminder.reminder_id)}`,
      })
    }
    const mail = inbox.data
    if (mail) {
      const oldest = mail.oldest_wait ? `oldest ${mail.oldest_wait}` : undefined
      if (mail.stuck > 0 || mail.awaiting > 0)
        rows.push({
          key: 'inbox:waiting',
          status: mail.stuck ? 'stuck' : 'waiting',
          tone: mail.stuck ? 'warn' : 'pending',
          label: mail.stuck ? 'Stuck' : 'Waiting',
          title: mail.stuck
            ? `${mail.stuck} inbox messages stuck in triage`
            : `${mail.awaiting} inbox messages waiting`,
          detail:
            [mail.stuck ? `${mail.awaiting} waiting` : '', oldest].filter(Boolean).join(' · ') ||
            undefined,
          detailKind: 'text',
          source: 'email',
          at: 0,
          ageText: mail.oldest_wait || undefined,
          href: '/inbox?category=waiting',
        })
      if (Number(mail.dead_letters) > 0)
        rows.push({
          key: 'inbox:dead',
          status: 'dead_letter',
          tone: 'danger',
          label: 'Dead letter',
          title: `${mail.dead_letters} inbox dead letters`,
          detail: 'Messages the handler stopped retrying',
          detailKind: 'text',
          source: 'email',
          at: 0,
          href: '/inbox',
        })
      if (mail.last_error)
        rows.push({
          key: 'inbox:error',
          status: 'error',
          tone: 'danger',
          label: 'Error',
          title: 'Inbox last error',
          source: 'email',
          detail: mail.last_error,
          detailKind: 'error',
          at: 0,
          href: '/inbox',
        })
    }
    if (outbox?.pending && outbox.oldest_created_at && now - time(outbox.oldest_created_at) > 300e3)
      rows.push({
        key: 'outbox',
        status: 'pending_publish',
        tone: 'warn',
        label: 'Outbox',
        title: `${outbox.pending} replies waiting to publish`,
        detail: `Oldest queued ${fmtWhen(outbox.oldest_created_at)}`,
        detailKind: 'text',
        source: 'internal',
        at: time(outbox.oldest_created_at),
        href: '/system',
      })
    if (loopTone(loopAge) === 'warn' || loopTone(loopAge) === 'danger')
      rows.push({
        key: 'handler:loop',
        status: 'stale',
        tone: loopTone(loopAge),
        label: 'Stale',
        title: 'Handler loop has not completed recently',
        detail: `Last loop finished ${fmtWhen(now - loopAge!)}`,
        detailKind: 'text',
        source: 'internal',
        at: now - loopAge!,
        href: '/system',
      })
    // Newest first so today's failures are not buried under week-old dead letters; rows
    // describing current state (at 0) count as now.
    return rows.sort(
      (a, b) => (b.at || now) - (a.at || now) || TONE_RANK[a.tone] - TONE_RANK[b.tone],
    )
  })()

  const upcoming = useMemo(() => {
    const rows: Upcoming[] = []
    for (const schedule of schedules.data?.schedules || []) {
      if (schedule.status !== 'active') continue
      const at = nextCronFire(schedule.cron, schedule.timezone, minute)
      if (at)
        rows.push({
          key: `s:${schedule.schedule_id}`,
          kind: 'cron',
          at,
          title: schedule.job_name,
          detail: schedule.cron,
          timezone: schedule.timezone,
          href: `/schedules?edit=${encode(schedule.schedule_id)}`,
        })
    }
    for (const reminder of reminders.data?.reminders || []) {
      const at = time(reminder.run_at)
      if (reminder.status !== 'scheduled' || at <= minute) continue
      rows.push({
        key: `r:${reminder.reminder_id}`,
        kind: 'reminder',
        at,
        title: reminder.prompt,
        detail: `${reminder.reply_channel.type} ${reminder.reply_channel.target}`,
        href: `/reminders?edit=${encode(reminder.reminder_id)}`,
      })
    }
    return rows.sort((a, b) => a.at - b.at)
  }, [schedules.data, reminders.data, minute])

  const activityRows: Activity[] | undefined = live && [
    ...live.map((executor) => ({ kind: 'live' as const, key: executor.task_id, executor })),
    ...queuedItems.map((item, i) => ({
      kind: 'queued' as const,
      key: item.work_item_id,
      item,
      position: i + 1,
    })),
  ]

  const inboxRows = useMemo(() => {
    const data = inbox.data
    if (!data) return undefined
    const byCategory = new Map<string, InboxCategory>()
    for (const category of ['waiting', 'review', 'action', 'order', 'news'])
      byCategory.set(category, { category, total: 0, day: 0, latest: 0 })
    for (const row of data.rows) {
      const entry = byCategory.get(row.decision)
      if (!entry) continue
      const at = time(row.received_at)
      if (at > minute - DAY) entry.day++
      entry.latest = Math.max(entry.latest, at)
    }
    for (const entry of byCategory.values())
      entry.total =
        entry.category === 'waiting' ? data.awaiting : (data.counts[entry.category] ?? 0)
    return [...byCategory.values()]
  }, [inbox.data, minute])

  const showAll = attentionView === 'all'
  const longest = live?.reduce((max, e) => Math.max(max, now - time(e.started_at)), 0)
  const latestDead = pendingDead?.reduce((max, d) => Math.max(max, time(d.created_at)), 0)
  const dayTokens = dayRuns?.reduce((sum, r) => sum + (r.tokens?.total_tokens || 0), 0)
  const dayModels = useMemo(() => {
    const byModel = new Map<string, number>()
    for (const r of dayRuns || [])
      if (r.model && r.tokens?.total_tokens)
        byModel.set(r.model, (byModel.get(r.model) || 0) + r.tokens.total_tokens)
    return [...byModel]
      .sort((a, b) => b[1] - a[1])
      .map(([model, n]) => `${model} ${fmtTokens(n)}`)
      .join(' · ')
  }, [dayRuns])
  const trends = useMemo(() => {
    if (!dayRuns) return undefined
    const at = (r: RunRow) => time(r.created_at)
    return {
      concurrency: hourlyConcurrency(dayRuns, { now: minute }),
      failed: hourlyBuckets(dayRuns, at, (r) => (r.result_status === 'success' ? 0 : 1), {
        now: minute,
      }),
      tokens: hourlyBuckets(dayRuns, at, (r) => r.tokens?.total_tokens || 0, { now: minute }),
    }
  }, [dayRuns, minute])
  const errorTone: Tone | undefined = !dayErrors?.length
    ? undefined
    : dayErrors.some((r) => r.result_status === 'dead_letter')
      ? 'danger'
      : 'warn'

  const systemFacts: Fact[] = [
    {
      label: 'Telegram',
      value: health.telegram,
      tone: health.telegram === 'Failing' || health.telegram === 'Stale' ? 'danger' : undefined,
      title: 'Telegram polling health; process reachability alone does not confirm connectivity',
    },
    {
      label: 'Handler up',
      value: fmtUptime(promValue(hm, 'chatting_message_handler_uptime_seconds')),
    },
    {
      label: 'Loop',
      value: loopAge == null ? undefined : `${shortAge(loopAge)} ago`,
      tone: loopTone(loopAge) === 'ok' ? undefined : loopTone(loopAge),
    },
    {
      label: 'Ingress',
      value: num(promValue(hm, 'chatting_message_handler_ingress_published_total')),
    },
    {
      label: 'Egress',
      value:
        promValue(hm, 'chatting_message_handler_egress_dispatched_total') != null &&
        `${num(promValue(hm, 'chatting_message_handler_egress_dispatched_total'))} / ${num(promValue(hm, 'chatting_message_handler_egress_received_total'))}`,
      title: 'dispatched / received',
    },
    {
      label: 'Dropped',
      value: num(promValue(hm, 'chatting_message_handler_egress_dropped_total')),
      tone: promValue(hm, 'chatting_message_handler_egress_dropped_total') ? 'warn' : undefined,
    },
    {
      label: 'Dedupe',
      value: pct(promValue(hm, 'chatting_message_handler_egress_dedupe_hit_rate_pct')),
    },
    {
      label: 'Outbox',
      value:
        outbox &&
        (outbox.pending
          ? `${outbox.pending} · ${shortAge(now - time(outbox.oldest_created_at))}`
          : '0'),
      title: 'Pending replies · age of the oldest',
    },
    {
      label: 'Handler DB',
      value: fmtBytes(promSum(hm, 'chatting_sqlite_file_size_bytes', { component: 'handler' })),
      title: 'database + WAL + shm',
    },
    {
      label: 'Worker DB',
      value: fmtBytes(promSum(wm, 'chatting_sqlite_file_size_bytes', { component: 'worker' })),
      title: 'database + WAL + shm',
    },
  ]

  return (
    <div className="overview">
      <TopbarActions>
        <Button variant="outline" size="sm" asChild>
          <Link to="/reminders?new=1">
            <Plus />
            Reminder
          </Link>
        </Button>
        <Button variant="outline" size="sm" asChild>
          <Link to="/schedules?new=1">
            <Plus />
            Schedule
          </Link>
        </Button>
      </TopbarActions>

      <div className="stat-tiles ov-tiles">
        <Stat
          label="Running"
          value={items.isError && !live ? null : (live?.length ?? '…')}
          tone={live?.length ? 'live' : undefined}
          sub={longest ? `longest ${shortAge(longest)}` : undefined}
          to="/items?status=live"
          spark={
            trends && (
              <Sparkline
                values={trends.concurrency}
                tone="live"
                label="Average runs in flight per hour, last 24h"
              />
            )
          }
        />
        <Stat
          label="Queued"
          value={activity.isError && !activity.data ? null : (activity.data?.queue.queued ?? '…')}
          tone={
            oldestQueued && now - time(oldestQueued) > 600e3
              ? 'warn'
              : activity.data?.queue.queued
                ? 'pending'
                : undefined
          }
          sub={
            [
              activity.data?.queue.queued && oldestQueued
                ? `oldest ${shortAge(now - time(oldestQueued))}`
                : '',
              outbox?.pending ? `${outbox.pending} to publish` : '',
            ]
              .filter(Boolean)
              .join(' · ') || undefined
          }
          title={oldestQueued ? `Oldest queued since ${fullTime(oldestQueued)}` : undefined}
          to="/items?status=queued"
        />
        <Stat
          label="Failed 24h"
          value={day.isError && !dayRuns ? null : (dayErrors?.length ?? '…')}
          tone={errorTone}
          sub={dayRuns && `of ${dayRuns.length}${day.data?.truncated ? '+' : ''} runs`}
          to="/runs?status=failed&since=24h"
          title="Failed (amber) and dead-letter (red) runs in the last 24 hours"
          spark={
            trends && (
              <Sparkline
                values={trends.failed}
                tone={errorTone || 'neutral'}
                label="Failed runs per hour, last 24h"
              />
            )
          }
        />
        <Stat
          label="Dead letters"
          value={deadLetters.isError && !pendingDead ? null : (pendingDead?.length ?? '…')}
          tone={pendingDead?.length ? 'danger' : undefined}
          sub={latestDead ? `latest ${shortAge(now - latestDead)}` : undefined}
          to="/runs?status=dead_letter"
        />
        <Stat
          label="Inbox waiting"
          value={
            inboxMissing
              ? 'off'
              : inbox.isError && !inbox.data
                ? null
                : (inbox.data?.awaiting ?? '…')
          }
          tone={inbox.data?.stuck ? 'warn' : inbox.data?.awaiting ? 'pending' : undefined}
          sub={
            inbox.data &&
            [
              inbox.data.awaiting && inbox.data.oldest_wait
                ? `oldest ${inbox.data.oldest_wait}`
                : '',
              inbox.data.stuck ? `${inbox.data.stuck} stuck` : '',
            ]
              .filter(Boolean)
              .join(' · ')
          }
          title={inboxMissing ? 'Personal inbox is not configured' : undefined}
          to="/inbox?category=waiting"
        />
        <Stat
          label="Tokens 24h"
          value={day.isError && !dayRuns ? null : dayTokens == null ? '…' : fmtTokens(dayTokens)}
          sub={
            dayRuns?.length
              ? `${fmtTokens(Math.round((dayTokens || 0) / dayRuns.length))} per run`
              : undefined
          }
          title={dayModels ? `${dayModels} · open tokens by model` : undefined}
          to="/system?window=24h#tokens"
          spark={trends && <Sparkline values={trends.tokens} label="Tokens per hour, last 24h" />}
        />
      </div>

      <section className="ov-system" aria-label="System">
        <Link to="/system" className="ov-system-label">
          System
        </Link>
        {handlerMetrics.isError && !handlerMetrics.data ? (
          <span className="ov-system-error" data-tone="warn">
            Handler metrics unavailable
          </span>
        ) : null}
        <FactsRow
          items={systemFacts.filter((fact) => fact.value != null && fact.value !== false)}
        />
      </section>

      <Panel
        title="Needs attention"
        count={attention.length || null}
        className="ov-attention"
        actions={
          attention.length > ATTENTION_ROWS && (
            <button
              type="button"
              className="text-link"
              onClick={() => setAttentionView(showAll ? '' : 'all', { replace: true })}
            >
              {showAll ? 'Show fewer' : `Show all ${attention.length}`}
            </button>
          )
        }
        to="/runs?status=failed"
        toLabel="Failed runs"
      >
        <ErrorStrip query={deadLetters} label="Dead letters" />
        <ErrorStrip query={day} label="Runs" />
        <DataTable
          label="Needs attention"
          columns={attentionColumns(now)}
          rows={
            !dayRuns && !pendingDead && !attention.length
              ? undefined
              : showAll
                ? attention
                : attention.slice(0, ATTENTION_ROWS)
          }
          rowKey={(row) => row.key}
          rowHref={(row) => row.href}
          loading={day.isPending || deadLetters.isPending}
          sortParam={false}
          skeletonRows={4}
          empty="Nothing needs attention"
        />
      </Panel>

      <div className="ov-columns">
        <div className="ov-column">
          <Panel
            title="Running now"
            count={live?.length || null}
            meta={queuedItems.length ? `${queuedItems.length} items queued` : undefined}
            to="/items?status=live"
          >
            <ErrorStrip query={items} label="Worker" />
            <DataTable
              label="Running now"
              columns={activityColumns(now, nameOf, queuedItems.length)}
              rows={activityRows}
              rowKey={(row) => `${row.kind}:${row.key}`}
              rowHref={(row) =>
                row.kind === 'live'
                  ? `/tasks/${encode(row.executor.task_id)}`
                  : `/items/${encode(row.item.work_item_id)}`
              }
              loading={items.isPending}
              sortParam={false}
              skeletonRows={3}
              empty="Nothing running"
            />
          </Panel>
          <Panel title="Recent runs" to="/runs" className="ov-capped">
            <ErrorStrip query={recent} label="Runs" />
            <DataTable
              label="Recent runs"
              columns={recentColumns(nameOf)}
              rows={recent.data?.runs.slice(0, RECENT_ROWS)}
              rowKey={(row) => row.run_id}
              rowHref={(row) => `/runs/${encode(row.run_id)}`}
              rowVersion={(row) => row.result_status}
              loading={recent.isPending}
              sortParam={false}
              empty="No runs yet"
            />
          </Panel>
        </div>
        <div className="ov-column">
          <Panel
            title="Upcoming"
            meta="cron and reminders"
            count={upcoming.length || null}
            to="/schedules"
            toLabel="Schedules"
            actions={
              <Link to="/reminders" className="text-link">
                Reminders
              </Link>
            }
            className="ov-capped"
          >
            <ErrorStrip query={schedules} label="Schedules" />
            <ErrorStrip query={reminders} label="Reminders" />
            <DataTable
              label="Upcoming"
              columns={upcomingColumns(now)}
              rows={schedules.data || reminders.data ? upcoming.slice(0, UPCOMING_ROWS) : undefined}
              rowKey={(row) => row.key}
              rowHref={(row) => row.href}
              loading={schedules.isPending || reminders.isPending}
              sortParam={false}
              empty="Nothing scheduled"
            />
          </Panel>
          <Panel
            title="Inbox"
            meta={
              inbox.data &&
              `${inbox.data.dry_run ? 'dry run' : 'live'} · threshold ${inbox.data.threshold}`
            }
            to="/inbox"
          >
            {inboxMissing ? (
              <div className="ov-note">Personal inbox is not configured</div>
            ) : (
              <>
                <ErrorStrip query={inbox} label="Inbox" />
                <DataTable
                  label="Inbox categories"
                  columns={inboxColumns(now)}
                  rows={inboxRows}
                  rowKey={(row) => row.category}
                  rowHref={(row) => `/inbox?category=${row.category}`}
                  loading={inbox.isPending}
                  sortParam={false}
                  skeletonRows={5}
                />
              </>
            )}
          </Panel>
        </div>
      </div>
    </div>
  )
}

function num(value: number | undefined) {
  return value == null ? undefined : value.toLocaleString()
}
function pct(value: number | undefined) {
  return value == null ? undefined : `${value.toFixed(1)}%`
}

function Primary({ title, secondary }: { title: ReactNode; secondary?: ReactNode }) {
  return (
    <span className="ov-primary">
      <span className="cell-primary truncate">{title}</span>
      {secondary && <span className="cell-secondary truncate">{secondary}</span>}
    </span>
  )
}

function attentionColumns(now: number): Column<Attention>[] {
  return [
    {
      key: 'status',
      header: 'Status',
      width: 112,
      cell: (row) => (
        <StatusBadge status={row.status} tone={row.tone}>
          {row.label}
        </StatusBadge>
      ),
    },
    {
      key: 'what',
      header: 'What',
      cell: (row) => <Primary title={row.title} secondary={row.secondary} />,
      title: (row) => [row.title, row.tooltip].filter(Boolean).join('\n'),
    },
    {
      key: 'source',
      header: 'Source',
      width: 104,
      priority: 2,
      cell: (row) => <SourceLabel source={row.source} />,
    },
    {
      key: 'detail',
      header: 'Detail',
      width: '38%',
      priority: 3,
      cell: (row) =>
        !row.detail ? (
          '—'
        ) : row.detailKind === 'error' ? (
          <ErrorText
            text={row.detail}
            className={row.tone === 'neutral' ? 'is-muted' : undefined}
          />
        ) : (
          <span className={row.detailKind === 'text' ? 'truncate' : 'mono truncate ov-detail'}>
            {row.detail}
          </span>
        ),
      title: (row) => [row.detail, row.tooltip].filter(Boolean).join('\n') || undefined,
    },
    {
      key: 'age',
      header: 'Age',
      width: 56,
      align: 'right',
      cell: (row) => row.ageText || (row.at ? age(now, row.at) : 'now'),
      title: (row) =>
        row.at
          ? new Date(row.at).toLocaleString()
          : row.ageText
            ? `Oldest has waited ${row.ageText}`
            : 'Current state',
    },
  ]
}

function activityColumns(
  now: number,
  nameOf: (id: string | null | undefined, fallback?: string) => string,
  queueLength: number,
): Column<Activity>[] {
  return [
    {
      key: 'status',
      header: 'Status',
      width: 96,
      cell: (row) =>
        row.kind === 'live' ? (
          <StatusBadge status="live" tone="live">
            Live
          </StatusBadge>
        ) : (
          <StatusBadge status="queued" kind="run" />
        ),
    },
    {
      key: 'what',
      header: 'Work item',
      cell: (row) =>
        row.kind === 'live' ? (
          <Primary
            title={nameOf(row.executor.work_item_id, row.executor.task_id)}
            secondary={row.executor.preview}
          />
        ) : (
          <Primary
            title={itemName(row.item)}
            secondary={`${row.item.queued} queued · #${row.position} of ${queueLength}`}
          />
        ),
      title: (row) => (row.kind === 'live' ? row.executor.preview : undefined),
    },
    {
      key: 'source',
      header: 'Source',
      width: 92,
      priority: 2,
      cell: (row) => (
        <SourceLabel source={row.kind === 'live' ? row.executor.source : row.item.reply?.type} />
      ),
    },
    {
      key: 'attempt',
      header: 'Att',
      width: 44,
      align: 'right',
      priority: 3,
      cell: (row) => (row.kind === 'live' ? row.executor.attempt : `#${row.position}`),
      title: (row) => (row.kind === 'queued' ? `Position ${row.position} in the queue` : undefined),
    },
    {
      key: 'elapsed',
      header: 'For',
      width: 56,
      align: 'right',
      cell: (row) => {
        const since = row.kind === 'live' ? row.executor.started_at : row.item.oldest_queued_at
        if (!since) return '—'
        const waited = now - time(since)
        return (
          <span
            className="num"
            data-tone={row.kind === 'queued' && waited > 600e3 ? 'warn' : undefined}
          >
            {age(now, time(since))}
          </span>
        )
      },
      title: (row) =>
        row.kind === 'live'
          ? fullTime(row.executor.started_at)
          : row.item.oldest_queued_at
            ? `Queued since ${fullTime(row.item.oldest_queued_at)}`
            : undefined,
    },
  ]
}

function recentColumns(
  nameOf: (id: string | null | undefined, fallback?: string) => string,
): Column<RunRow>[] {
  return [
    {
      key: 'status',
      header: 'Status',
      width: 96,
      cell: (row) => <RunStatus status={row.result_status} />,
    },
    {
      key: 'when',
      header: 'When',
      width: 88,
      cell: (row) => <span className="num">{fmtWhen(row.created_at)}</span>,
      title: (row) => fullTime(row.created_at),
    },
    {
      key: 'what',
      header: 'Work item',
      cell: (row) => (
        <Primary title={nameOf(row.work_item_id, row.task_id)} secondary={row.preview} />
      ),
      title: (row) => row.preview,
    },
    {
      key: 'source',
      header: 'Source',
      width: 92,
      priority: 3,
      cell: (row) => <SourceLabel source={row.source} />,
    },
    {
      key: 'latency',
      header: 'Took',
      width: 68,
      align: 'right',
      priority: 2,
      cell: (row) => fmtLatency(row.latency_ms),
    },
    {
      key: 'tokens',
      header: 'Tokens',
      width: 72,
      align: 'right',
      priority: 2,
      mono: true,
      cell: (row) => fmtTokens(row.tokens?.total_tokens),
      title: (row) => tokenTitle(row.tokens, row.model),
    },
  ]
}

function upcomingColumns(now: number): Column<Upcoming>[] {
  return [
    {
      key: 'when',
      header: 'When',
      width: 92,
      cell: (row) => <span className="num">{fmtNext(row.at, now)}</span>,
      title: (row) => new Date(row.at).toLocaleString(),
    },
    {
      key: 'in',
      header: 'In',
      width: 48,
      align: 'right',
      cell: (row) => shortAge(row.at - now),
    },
    {
      key: 'kind',
      header: 'Kind',
      width: 96,
      priority: 2,
      cell: (row) => <SourceLabel source={row.kind} />,
    },
    {
      key: 'what',
      header: 'What',
      cell: (row) => (
        <span className="ov-primary">
          <span className="cell-primary truncate">{row.title}</span>
          {row.kind === 'cron' && (
            <span className="cell-secondary mono truncate" data-priority="3">
              {row.detail}
              {row.timezone && row.timezone !== 'UTC' ? ` ${row.timezone}` : ''}
            </span>
          )}
        </span>
      ),
      title: (row) => `${row.title} · ${row.detail}${row.timezone ? ` (${row.timezone})` : ''}`,
    },
  ]
}

function inboxColumns(now: number): Column<InboxCategory>[] {
  return [
    {
      key: 'category',
      header: 'Category',
      cell: (row) => <StatusBadge status={row.category} kind="inbox" />,
    },
    {
      key: 'total',
      header: 'Total',
      width: 64,
      align: 'right',
      cell: (row) => row.total.toLocaleString(),
    },
    {
      key: 'day',
      header: '24h',
      width: 56,
      align: 'right',
      cell: (row) => row.day || '—',
    },
    {
      key: 'latest',
      header: 'Latest',
      width: 64,
      align: 'right',
      cell: (row) => age(now, row.latest),
      title: (row) => (row.latest ? new Date(row.latest).toLocaleString() : undefined),
    },
  ]
}
