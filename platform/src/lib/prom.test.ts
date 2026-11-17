import { describe, expect, it } from 'vitest'
import { parseProm, promSamples, promSum, promValue, sampleKey } from './prom'

const TEXT = `# HELP chatting_message_handler_uptime_seconds Seconds since the message handler process started.
# TYPE chatting_message_handler_uptime_seconds gauge
chatting_message_handler_uptime_seconds 275269.654315
# TYPE chatting_message_handler_loops_total counter
chatting_message_handler_loops_total 52334 1794914389000

chatting_sqlite_file_size_bytes{component="handler",file="database"} 4096
chatting_sqlite_file_size_bytes{component="handler",file="wal"} 2834592
weird{path="a\\"b\\\\c\\nd", x="y,z}"} 1
gauge_nan NaN
gauge_inf +Inf
not a sample
broken{a="1" 3
`

describe('parseProm', () => {
  const metrics = parseProm(TEXT)

  it('reads HELP and TYPE', () => {
    expect(metrics.help.chatting_message_handler_uptime_seconds).toBe(
      'Seconds since the message handler process started.',
    )
    expect(metrics.type.chatting_message_handler_loops_total).toBe('counter')
  })

  it('parses plain and labelled samples, ignoring timestamps', () => {
    expect(promValue(metrics, 'chatting_message_handler_uptime_seconds')).toBeCloseTo(275269.654)
    expect(promValue(metrics, 'chatting_message_handler_loops_total')).toBe(52334)
    expect(promValue(metrics, 'chatting_sqlite_file_size_bytes', { file: 'wal' })).toBe(2834592)
    expect(promSamples(metrics, 'chatting_sqlite_file_size_bytes')).toHaveLength(2)
  })

  it('unescapes label values and keeps commas and braces inside quotes', () => {
    expect(promSamples(metrics, 'weird')[0].labels).toEqual({ path: 'a"b\\c\nd', x: 'y,z}' })
  })

  it('handles special float values', () => {
    expect(promValue(metrics, 'gauge_nan')).toBeNaN()
    expect(promValue(metrics, 'gauge_inf')).toBe(Infinity)
  })

  it('skips malformed lines', () => {
    expect(promValue(metrics, 'not')).toBeUndefined()
    expect(promValue(metrics, 'broken')).toBeUndefined()
  })

  it('returns undefined for missing metrics and empty input', () => {
    expect(promValue(undefined, 'x')).toBeUndefined()
    expect(parseProm('').samples).toEqual([])
  })

  it('builds a stable sample key', () => {
    expect(sampleKey(promSamples(metrics, 'chatting_sqlite_file_size_bytes')[0])).toBe(
      'chatting_sqlite_file_size_bytes{component="handler",file="database"}',
    )
  })
})

it('sums labelled samples', () => {
  const m = parseProm('x{c="a",f="db"} 2\nx{c="a",f="wal"} 3\nx{c="b",f="db"} 7\n')
  expect(promSum(m, 'x', { c: 'a' })).toBe(5)
  expect(promSum(m, 'missing')).toBeUndefined()
})
