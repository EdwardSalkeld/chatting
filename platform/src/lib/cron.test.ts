import { describe, expect, it } from 'vitest'
import {
  cronError,
  describeCron,
  nextRuns,
  parseCronTask,
  parseReminderTask,
  previousRun,
  relTime,
  shortCron,
  timezoneError,
  zonedWhen,
} from './cron'

describe('cronError', () => {
  it('accepts standard five-field expressions', () => {
    for (const expr of ['0 8 * * 1-5', '*/5 * * * *', '0 9 1 1,4,7,10 *', '0 9 * * MON'])
      expect(cronError(expr)).toBeNull()
  })
  it('rejects what the handler parser rejects', () => {
    expect(cronError('')).toMatch(/Enter/)
    expect(cronError('0 8 * *')).toMatch(/Expected 5 fields/)
    expect(cronError('0 0 8 * * 1')).toMatch(/got 6/)
    expect(cronError('@daily')).toMatch(/@ shortcuts/)
    expect(cronError('0 0 L * *')).toMatch(/day of month/)
    expect(cronError('0 25 * * *')).not.toBeNull()
  })
})

describe('describeCron / shortCron', () => {
  it('gives the full cronstrue sentence', () => {
    expect(describeCron('0 8 * * 1-5')).toBe('At 08:00, Monday through Friday')
    expect(describeCron('nope')).toBeNull()
  })
  it('compacts common shapes for table cells', () => {
    expect(shortCron('0 8 * * 1-5')).toBe('08:00 weekdays')
    expect(shortCron('15 3 * * *')).toBe('03:15 daily')
    expect(shortCron('0 10,14 * * 1-5')).toBe('10:00 and 14:00 weekdays')
    expect(shortCron('45 19 * * 0-4')).toBe('19:45 Sun–Thu')
    expect(shortCron('0 6 * * 5')).toBe('06:00 Fri')
    expect(shortCron('0 7 1 * *')).toBe('07:00 day 1')
    expect(shortCron('30 */6 * * *')).toBe(':30 every 6h')
    expect(shortCron('*/5 * * * *')).toBe('every 5 minutes')
    expect(shortCron('0 9 1 1,4,7,10 *')).toBe('09:00 day 1 in Jan Apr Jul and Oct')
  })
  it('falls back to the raw expression when invalid', () => {
    expect(shortCron('0 8 * *')).toBe('0 8 * *')
  })
})

describe('nextRuns', () => {
  const from = new Date('2026-11-17T09:00:00Z')
  it('computes runs in the schedule timezone', () => {
    const runs = nextRuns('0 8 * * 1-5', 'America/New_York', 2, from)
    expect(runs.map((d) => d.toISOString())).toEqual([
      '2026-11-17T13:00:00.000Z',
      '2026-11-18T13:00:00.000Z',
    ])
  })
  it('treats an empty timezone as UTC', () => {
    expect(nextRuns('0 8 * * *', '', 1, from)[0].toISOString()).toBe('2026-11-18T08:00:00.000Z')
  })
  it('returns nothing for invalid input', () => {
    expect(nextRuns('0 8 * *', 'UTC', 3, from)).toEqual([])
    expect(nextRuns('0 8 * * *', 'Mars/Base', 3, from)).toEqual([])
  })
  it('finds the previous run', () => {
    expect(previousRun('0 8 * * *', 'UTC', from)?.toISOString()).toBe('2026-11-17T08:00:00.000Z')
  })
})

describe('timezoneError', () => {
  it('validates IANA names', () => {
    expect(timezoneError('Europe/London')).toBeNull()
    expect(timezoneError('')).toBeNull()
    expect(timezoneError('Mars/Base')).toMatch(/Unknown/)
  })
})

describe('relTime', () => {
  const now = Date.parse('2026-11-17T10:00:00Z')
  it('formats future and past offsets compactly', () => {
    expect(relTime(now + 20_000, now)).toBe('in <1m')
    expect(relTime(now + 12 * 60000, now)).toBe('in 12m')
    expect(relTime(now + 134 * 60000, now)).toBe('in 2h 14m')
    expect(relTime(now + 120 * 60000, now)).toBe('in 2h')
    expect(relTime(now + 14 * 3600000, now)).toBe('in 14h')
    expect(relTime(now + 3 * 86400000, now)).toBe('in 3d')
    expect(relTime(now - 12 * 60000, now)).toBe('12m ago')
    expect(relTime('garbage', now)).toBe('—')
  })
})

describe('zonedWhen', () => {
  const now = Date.parse('2026-11-17T10:00:00Z')
  it('shows weekday and time in the zone, or the date when far away', () => {
    expect(zonedWhen(new Date('2026-11-18T08:00:00Z'), 'Europe/London', now)).toBe('Wed 08:00')
    expect(zonedWhen(new Date('2026-11-18T08:00:00Z'), 'Asia/Tokyo', now)).toBe('Wed 17:00')
    expect(zonedWhen(new Date('2027-01-01T00:00:00Z'), 'UTC', now)).toBe('1 Jan 00:00')
  })
})

describe('task id parsing', () => {
  it('parses cron task ids with colons in the time', () => {
    const parsed = parseCronTask('task:cron:Sentry triage:2026-11-14T11:30:00+00:00')
    expect(parsed?.job).toBe('Sentry triage')
    expect(parsed?.scheduledFor?.toISOString()).toBe('2026-11-14T11:30:00.000Z')
    expect(parseCronTask('task:telegram:item:0')).toBeNull()
  })
  it('parses reminder task ids', () => {
    expect(parseReminderTask('task:reminder:rem_rich23:1')).toEqual({
      reminderId: 'rem_rich23',
      revision: 1,
    })
    expect(parseReminderTask('task:cron:x:1')).toBeNull()
  })
})
