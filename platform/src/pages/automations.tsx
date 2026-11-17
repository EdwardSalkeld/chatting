import { useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowRight, Clock3, History, LoaderCircle, Plus, Send, Trash2, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
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
  Empty,
  ErrorNotice,
  Loading,
  PageHeading,
  Panel,
  SearchInput,
  StatusBadge,
} from '@/components/shared'
import { api, useReminders, useSchedules } from '@/lib/api'
import { dateTime, encode, lines, localDateTime, relative } from '@/lib/format'
import type { Reminder, Schedule } from '@/lib/types'

type Kind = 'schedules' | 'reminders'
type RecordType = Schedule | Reminder
const isSchedule = (row: RecordType): row is Schedule => 'schedule_id' in row
const recordID = (row: RecordType) => (isSchedule(row) ? row.schedule_id : row.reminder_id)

export function Automations({ kind }: { kind: Kind }) {
  const schedules = useSchedules(),
    reminders = useReminders()
  const query = kind === 'schedules' ? schedules : reminders
  const [params, setParams] = useSearchParams()
  const [search, setSearch] = useState(''),
    [filter, setFilter] = useState('scheduled'),
    [notice, setNotice] = useState('')
  const recurring = kind === 'schedules'
  const rows: RecordType[] = recurring
    ? schedules.data?.schedules || []
    : reminders.data?.reminders || []
  const visible = rows.filter(
    (row) =>
      (recurring ||
        (filter === 'scheduled' ? row.status === 'scheduled' : row.status !== 'scheduled')) &&
      (isSchedule(row)
        ? `${row.job_name} ${row.content} ${row.cron}`
        : `${row.prompt} ${row.reply_channel.target}`
      )
        .toLowerCase()
        .includes(search.toLowerCase()),
  )
  const selectedID = params.get('edit'),
    isNew = params.has('new'),
    open = isNew || !!selectedID
  const detail = useQuery({
    queryKey: [kind, selectedID],
    queryFn: () =>
      api<{ schedule?: Schedule; reminder?: Reminder; history: RecordType[] }>(
        `handler/${kind}/${encode(selectedID!)}?history=1`,
      ),
    enabled: !!selectedID,
  })
  const selected = detail.data?.schedule || detail.data?.reminder
  function close() {
    setParams({}, { replace: true })
  }
  return (
    <>
      <PageHeading
        title={recurring ? 'Schedules' : 'Reminders'}
        action={
          <Button onClick={() => setParams({ new: '1' })}>
            <Plus />
            {recurring ? 'New schedule' : 'New reminder'}
          </Button>
        }
      />
      {notice && (
        <div className="success-notice" role="status">
          {notice}
          <button aria-label="Dismiss notification" onClick={() => setNotice('')}>
            <X size={16} />
          </button>
        </div>
      )}
      <ErrorNotice
        error={query.error}
        retry={() => {
          void query.refetch()
        }}
      />
      <div className="list-toolbar">
        {recurring ? (
          <div className="list-count">
            <span className="status-dot" />
            {rows.length} active {rows.length === 1 ? 'schedule' : 'schedules'}
          </div>
        ) : (
          <div className="filter-tabs">
            {[
              ['scheduled', 'Upcoming'],
              ['history', 'History'],
            ].map(([value, label]) => (
              <button
                key={value}
                aria-pressed={filter === value}
                className={filter === value ? 'selected' : ''}
                onClick={() => setFilter(value)}
              >
                {label}
              </button>
            ))}
          </div>
        )}
        <SearchInput value={search} onChange={setSearch} placeholder={`Search ${kind}…`} />
      </div>
      {query.isPending ? (
        <Panel>
          <Loading />
        </Panel>
      ) : visible.length && recurring ? (
        <Panel>
          <div className="table-scroll">
            <table className="schedule-table">
              <thead>
                <tr>
                  <th>SCHEDULE</th>
                  <th>CRON</th>
                  <th>TIMEZONE</th>
                  <th>REPLY TARGET</th>
                  <th>
                    <span className="sr-only">Edit schedule</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {visible.filter(isSchedule).map((row) => (
                  <tr key={row.schedule_id}>
                    <td>
                      <button
                        className="schedule-name"
                        onClick={() => setParams({ edit: row.schedule_id })}
                      >
                        <strong>{row.job_name}</strong>
                        <span>{row.content}</span>
                      </button>
                    </td>
                    <td>
                      <code>{row.cron}</code>
                    </td>
                    <td>{row.timezone || 'UTC'}</td>
                    <td>
                      {row.reply_channel_type
                        ? `${row.reply_channel_type} · ${row.reply_channel_target}`
                        : '—'}
                    </td>
                    <td>
                      <Button
                        variant="ghost"
                        size="icon-sm"
                        aria-label={`Edit ${row.job_name}`}
                        onClick={() => setParams({ edit: row.schedule_id })}
                      >
                        <ArrowRight />
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      ) : visible.length ? (
        <div className="reminder-list">
          {visible.map((row) =>
            isSchedule(row) ? null : (
              <button
                className="reminder-card"
                key={row.reminder_id}
                onClick={() => setParams({ edit: row.reminder_id })}
              >
                <div className="reminder-date">
                  <span>{new Date(row.run_at).toLocaleString(undefined, { month: 'short' })}</span>
                  <strong>{new Date(row.run_at).getDate()}</strong>
                </div>
                <div className="reminder-copy">
                  <h2>{row.prompt}</h2>
                  <p>
                    <Clock3 size={13} />
                    {dateTime(row.run_at)}
                    <span>·</span>
                    {row.reply_channel.type} · {row.reply_channel.target}
                  </p>
                </div>
                <div className="reminder-right">
                  <StatusBadge status={row.status} />
                  <span>
                    {relative(
                      row.status === 'scheduled'
                        ? row.run_at
                        : row.cancelled_at || row.fired_at || row.updated_at,
                    )}
                  </span>
                </div>
                <ArrowRight size={18} />
              </button>
            ),
          )}
        </div>
      ) : (
        <Panel>
          <Empty
            title={
              search
                ? 'No matching results'
                : recurring
                  ? 'No schedules'
                  : filter === 'history'
                    ? 'No past reminders'
                    : 'No upcoming reminders'
            }
            action={
              search ? (
                <Button variant="outline" onClick={() => setSearch('')}>
                  Clear search
                </Button>
              ) : (
                (recurring || filter === 'scheduled') && (
                  <Button onClick={() => setParams({ new: '1' })}>
                    <Plus />
                    {recurring ? 'New schedule' : 'New reminder'}
                  </Button>
                )
              )
            }
          />
        </Panel>
      )}
      {!recurring && (
        <p className="list-caption">{Intl.DateTimeFormat().resolvedOptions().timeZone}</p>
      )}
      <Dialog
        open={open}
        onOpenChange={(value) => {
          if (!value) close()
        }}
      >
        <DialogContent className="automation-dialog">
          <DialogHeader>
            <DialogTitle>
              {isNew
                ? recurring
                  ? 'New schedule'
                  : 'New reminder'
                : recurring
                  ? 'Schedule details'
                  : 'Reminder details'}
            </DialogTitle>
            <DialogDescription className={recurring ? 'sr-only' : undefined}>
              {recurring
                ? 'Recurring task'
                : `Times in ${Intl.DateTimeFormat().resolvedOptions().timeZone}`}
            </DialogDescription>
          </DialogHeader>
          {!isNew && detail.isPending ? (
            <Loading />
          ) : !isNew && !selected ? (
            <ErrorNotice
              error={detail.error}
              retry={() => {
                void detail.refetch()
              }}
            />
          ) : (
            <AutomationForm
              key={selectedID || 'new'}
              kind={kind}
              record={isNew ? undefined : selected}
              history={detail.data?.history || []}
              onClose={close}
              onSaved={(message) => {
                close()
                setNotice(message)
              }}
            />
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}

function AutomationForm({
  kind,
  record,
  history,
  onClose,
  onSaved,
}: {
  kind: Kind
  record?: RecordType
  history: RecordType[]
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
  const client = useQueryClient()
  const submission = useRef({ fingerprint: '', key: '' })
  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    const data = new FormData(event.currentTarget),
      value = (key: string) => String(data.get(key) || '').trim()
    let payload: Record<string, unknown>
    try {
      const common = {
        context_refs: lines(data.get('context_refs')),
        prompt_context: lines(data.get('prompt_context')),
        created_by: record?.created_by || 'platform',
      }
      if (recurring)
        payload = {
          ...common,
          job_name: value('job_name'),
          content: value('content'),
          cron: value('cron'),
          timezone: value('timezone') || 'UTC',
          reply_channel_type: value('channel_type'),
          reply_channel_target: value('channel_target'),
        }
      else {
        const metadata: unknown = JSON.parse(value('metadata') || '{}')
        if (!metadata || Array.isArray(metadata) || typeof metadata !== 'object')
          throw Error('Reply metadata must be a JSON object.')
        payload = {
          ...common,
          run_at: new Date(value('run_at')).toISOString(),
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
      onSaved(`${recurring ? 'Schedule' : 'Reminder'} ${record ? 'updated' : 'created'}.`)
    } catch (e) {
      setError(e instanceof Error ? e : Error('Could not save.'))
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
      onSaved(recurring ? 'Schedule deleted.' : 'Reminder cancelled.')
    } catch (e) {
      setError(e instanceof Error ? e : Error('Could not remove.'))
      setConfirm(false)
    } finally {
      setBusy(false)
    }
  }
  return (
    <form onSubmit={save} className="automation-form">
      <fieldset disabled={busy || readOnly}>
        {recurring ? (
          <>
            <div className="form-field">
              <Label htmlFor="job-name">Schedule name</Label>
              <Input
                id="job-name"
                name="job_name"
                required
                defaultValue={schedule?.job_name}
                placeholder="Morning briefing"
              />
            </div>
            <div className="form-field">
              <Label htmlFor="content">Prompt</Label>
              <Textarea
                id="content"
                name="content"
                required
                defaultValue={schedule?.content}

                rows={3}
              />
            </div>
            <div className="form-columns">
              <div className="form-field">
                <Label htmlFor="cron">Cron expression</Label>
                <Input
                  className="font-mono"
                  id="cron"
                  name="cron"
                  required
                  defaultValue={schedule?.cron}
                  placeholder="0 9 * * 1-5"
                />
                <p className="field-hint">Five fields: minute, hour, day, month, weekday.</p>
              </div>
              <div className="form-field">
                <Label htmlFor="timezone">Timezone</Label>
                <Input
                  id="timezone"
                  name="timezone"
                  defaultValue={
                    schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone
                  }
                  placeholder="Europe/London"
                />
                <p className="field-hint">An IANA timezone, such as Europe/London.</p>
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="form-field">
              <Label htmlFor="prompt">Prompt</Label>
              <Textarea
                id="prompt"
                name="prompt"
                required
                defaultValue={reminder?.prompt}

                rows={3}
              />
            </div>
            <div className="form-field">
              <Label htmlFor="run-at">When</Label>
              <Input
                id="run-at"
                name="run_at"
                type="datetime-local"
                required
                defaultValue={reminder ? localDateTime(reminder.run_at) : ''}
              />
              <p className="field-hint">Local time</p>
            </div>
          </>
        )}
        <div className="form-section-title">
          <Send size={14} />
          Delivery
        </div>
        <div className="form-columns">
          <div className="form-field">
            <Label htmlFor="channel-type">Reply channel{!recurring && ' *'}</Label>
            <Input
              id="channel-type"
              name="channel_type"
              required={!recurring}
              defaultValue={schedule?.reply_channel_type || reminder?.reply_channel.type || ''}
              placeholder="telegram"
            />
          </div>
          <div className="form-field">
            <Label htmlFor="channel-target">Reply target{!recurring && ' *'}</Label>
            <Input
              id="channel-target"
              name="channel_target"
              required={!recurring}
              defaultValue={schedule?.reply_channel_target || reminder?.reply_channel.target || ''}
              placeholder="Chat or channel ID"
            />
          </div>
        </div>
        <details className="advanced-fields">
          <summary>Context</summary>
          <div className="form-field">
            <Label htmlFor="context-refs">Context references</Label>
            <Textarea
              id="context-refs"
              name="context_refs"
              defaultValue={record?.context_refs.join('\n')}
              placeholder="repo:/absolute/path"
              rows={2}
            />
            <p className="field-hint">One per line, using repo:/absolute/path.</p>
          </div>
          <div className="form-field">
            <Label htmlFor="prompt-context">Prompt context</Label>
            <Textarea
              id="prompt-context"
              name="prompt_context"
              defaultValue={record?.prompt_context.join('\n')}
              placeholder="One context entry per line"
              rows={2}
            />
          </div>
          {!recurring && (
            <div className="form-field">
              <Label htmlFor="metadata">Reply metadata (JSON)</Label>
              <Textarea
                className="font-mono"
                id="metadata"
                name="metadata"
                defaultValue={JSON.stringify(reminder?.reply_channel.metadata || {}, null, 2)}
                rows={3}
              />
            </div>
          )}
        </details>
      </fieldset>
      {readOnly && (
        <div className="subtle-notice">
          {reminder?.status === 'cancelled' ? 'Cancelled' : 'Fired'}
        </div>
      )}
      {record && (
        <details className="revision-history">
          <summary>
            <History size={14} />
            Revision history ({history.length})
          </summary>
          {history.map((row) => (
            <div key={isSchedule(row) ? row.version : row.revision}>
              <strong>Revision {isSchedule(row) ? row.version : row.revision}</strong>
              <StatusBadge status={row.status} />
              <p>
                {dateTime(isSchedule(row) ? row.created_at : row.updated_at)} ·{' '}
                {isSchedule(row) ? row.content : row.prompt}
              </p>
            </div>
          ))}
        </details>
      )}
      <ErrorNotice error={error} title="Couldn’t save changes" />
      <div className="form-footer">
        {record && !readOnly && (
          <Button
            type="button"
            variant="ghost"
            className="danger-button"
            disabled={busy}
            onClick={() => setConfirm(true)}
          >
            <Trash2 />
            {recurring ? 'Delete schedule' : 'Cancel reminder'}
          </Button>
        )}
        <div className="form-footer-right">
          <Button type="button" variant="outline" disabled={busy} onClick={onClose}>
            {readOnly ? 'Close' : 'Discard'}
          </Button>
          {!readOnly && (
            <Button type="submit" disabled={busy}>
              {busy && <LoaderCircle className="animate-spin" />}
              {record ? 'Save changes' : recurring ? 'Create schedule' : 'Create reminder'}
            </Button>
          )}
        </div>
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
    </form>
  )
}
