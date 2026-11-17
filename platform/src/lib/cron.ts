import cronstrue from 'cronstrue'
import { CronExpressionParser } from 'cron-parser'

// The handler parses with robfig/cron using exactly these five fields and no extensions.
const FIELDS = ['minute', 'hour', 'day of month', 'month', 'day of week'] as const
const FIELD_RE = /^[\d*?,/\-A-Za-z]+$/

export function cronError(expression: string): string | null {
  const parts = expression.trim().split(/\s+/).filter(Boolean)
  if (!parts.length) return 'Enter a cron expression.'
  if (parts[0].startsWith('@')) return 'Use five fields; @ shortcuts are not supported.'
  if (parts.length !== 5)
    return `Expected 5 fields (minute hour day month weekday), got ${parts.length}.`
  for (const [i, part] of parts.entries()) {
    if (!FIELD_RE.test(part) || /[LH#]/.test(part.replace(/[A-Za-z]{3}/g, '')))
      return `Unsupported ${FIELDS[i]} field “${part}”.`
  }
  try {
    CronExpressionParser.parse(parts.join(' '), { strict: false })
  } catch (e) {
    return e instanceof Error ? e.message.replace(/^Validation error, /, '') : 'Invalid cron.'
  }
  return null
}

export function timezoneError(tz: string): string | null {
  if (!tz.trim()) return null
  try {
    new Intl.DateTimeFormat('en-GB', { timeZone: tz.trim() })
    return null
  } catch {
    return `Unknown timezone “${tz}”.`
  }
}

export function describeCron(expression: string): string | null {
  if (cronError(expression)) return null
  try {
    return cronstrue.toString(expression.trim(), {
      use24HourTimeFormat: true,
      verbose: false,
      throwExceptionOnParseError: true,
    })
  } catch {
    return null
  }
}

const DAY_ABBR: Record<string, string> = {
  Monday: 'Mon',
  Tuesday: 'Tue',
  Wednesday: 'Wed',
  Thursday: 'Thu',
  Friday: 'Fri',
  Saturday: 'Sat',
  Sunday: 'Sun',
}
const MONTH_ABBR: Record<string, string> = {
  January: 'Jan',
  February: 'Feb',
  March: 'Mar',
  April: 'Apr',
  May: 'May',
  June: 'Jun',
  July: 'Jul',
  August: 'Aug',
  September: 'Sep',
  October: 'Oct',
  November: 'Nov',
  December: 'Dec',
}

/** Compact cronstrue text for table cells: "08:00 weekdays", "03:15 daily", "every 6h at :30". */
export function shortCron(expression: string): string {
  const full = describeCron(expression)
  if (!full) return expression.trim()
  let text = full
    .replace(/Monday through Friday/g, 'weekdays')
    .replace(/Saturday and Sunday/g, 'weekends')
    .replace(/ through /g, '–')
    .replace(/, only on /g, ' ')
    .replace(/, only in /g, ' in ')
    .replace(/\bon day (\d+) of the month\b/g, 'day $1')
    .replace(/\bon the last day of the month\b/g, 'last day')
    .replace(/\bevery (\d+) hours\b/g, 'every $1h')
    .replace(/\bevery (\d+) minutes\b/g, 'every $1m')
    .replace(/^On the hour, /, ':00 ')
    .replace(/^At (\d+) minutes past the hour, /, ':$1 ')
    .replace(/^At /, '')
    .replace(/, /g, ' ')
  for (const [long, short] of Object.entries({ ...DAY_ABBR, ...MONTH_ABBR }))
    text = text.replace(new RegExp(`\\b${long}\\b`, 'g'), short)
  if (/^\d\d:\d\d( and \d\d:\d\d)*$/.test(text)) text += ' daily'
  return text.replace(/^Every /, 'every ')
}

export function nextRuns(expression: string, timezone: string, count = 5, from = new Date()) {
  if (cronError(expression) || timezoneError(timezone)) return []
  try {
    return CronExpressionParser.parse(expression.trim(), {
      currentDate: from,
      tz: timezone.trim() || 'UTC',
    })
      .take(count)
      .map((date) => date.toDate())
  } catch {
    return []
  }
}

export function previousRun(expression: string, timezone: string, from = new Date()) {
  if (cronError(expression) || timezoneError(timezone)) return null
  try {
    return CronExpressionParser.parse(expression.trim(), {
      currentDate: from,
      tz: timezone.trim() || 'UTC',
    })
      .prev()
      .toDate()
  } catch {
    return null
  }
}

/** "in 2h 14m", "in 14h", "in 3d", "12m ago". */
export function relTime(target: Date | number | string, now: number = Date.now()) {
  const ms = new Date(target).getTime() - now
  if (!Number.isFinite(ms)) return '—'
  const future = ms >= 0,
    m = Math.floor(Math.abs(ms) / 60000)
  let text: string
  if (m < 1) text = '<1m'
  else if (m < 60) text = `${m}m`
  else if (m < 600) text = m % 60 ? `${Math.floor(m / 60)}h ${m % 60}m` : `${m / 60}h`
  else if (m < 2880) text = `${Math.round(m / 60)}h`
  else text = `${Math.round(m / 1440)}d`
  return future ? `in ${text}` : `${text} ago`
}

/** "Tue 08:00" in the given zone, or "1 Jan 00:00" when it is not within the coming week. */
export function zonedWhen(date: Date, timezone?: string, now: number = Date.now()) {
  const zone = timezone?.trim() || undefined
  const soon = Math.abs(date.getTime() - now) < 6 * 86400000
  try {
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: zone,
      ...(soon ? { weekday: 'short' } : { month: 'short', day: 'numeric' }),
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23',
    })
      .format(date)
      .replace(',', '')
  } catch {
    return date.toISOString().slice(0, 16).replace('T', ' ')
  }
}

export function fullWhen(date: Date, timezone?: string) {
  try {
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: timezone?.trim() || undefined,
      weekday: 'short',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hourCycle: 'h23',
      timeZoneName: 'short',
    }).format(date)
  } catch {
    return date.toISOString()
  }
}

/** `task:cron:<job name>:<scheduled ISO time>` as the handler builds cron envelopes. */
export function parseCronTask(taskId: string) {
  const match = /^task:cron:(.+):(\d{4}-\d\d-\d\dT[\d:.]+(?:Z|[+-]\d\d:\d\d)?)$/.exec(taskId)
  if (!match) return null
  const scheduled = new Date(match[2])
  return { job: match[1], scheduledFor: Number.isNaN(scheduled.getTime()) ? null : scheduled }
}

/** `task:reminder:<reminder id>:<revision>`. */
export function parseReminderTask(taskId: string) {
  const match = /^task:reminder:(.+):(\d+)$/.exec(taskId)
  return match ? { reminderId: match[1], revision: Number(match[2]) } : null
}
