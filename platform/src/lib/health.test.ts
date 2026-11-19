import { describe, expect, it } from 'vitest'
import { handlerHealth } from './health'
import { parseProm } from './prom'

const now = 1_800_000_000_000
const metrics = (values: Record<string, number>) =>
  parseProm(
    Object.entries(values)
      .map(([key, value]) => `chatting_message_handler_${key} ${value}`)
      .join('\n'),
  )

describe('handler connection health', () => {
  it('surfaces Telegram failure while the process is alive and polling is backing off', () => {
    const health = handlerHealth(
      metrics({
        telegram_enabled: 1,
        telegram_poll_failed: 1,
        telegram_poll_error_code: 502,
        telegram_last_poll_timestamp_seconds: now / 1000 - 50,
        last_loop_completed_timestamp_seconds: now / 1000,
      }),
      now,
    )
    expect(health.telegram).toBe('Failing')
    expect(health.issues[0].detail).toContain('502')
    expect(health.issues[0].detail).toContain('retrying')
  })

  it('clears the current connection warning after a successful empty poll', () => {
    const health = handlerHealth(
      metrics({
        telegram_enabled: 1,
        telegram_poll_failed: 0,
        telegram_poll_failures_total: 12,
        telegram_last_poll_timestamp_seconds: now / 1000,
      }),
      now,
    )
    expect(health.telegram).toBe('Connected')
    expect(health.issues).toEqual([])
  })

  it('shows stalled polling even if no first poll ever completes', () => {
    const health = handlerHealth(
      metrics({
        telegram_enabled: 1,
        process_start_time_seconds: now / 1000 - 600,
      }),
      now,
    )
    expect(health.telegram).toBe('Stale')
    expect(health.issues[0].key).toBe('telegram')
  })

  it('does not label absent metrics or a disabled connector as connected', () => {
    expect(handlerHealth(metrics({}), now).telegram).toBe('Unknown')
    expect(handlerHealth(metrics({ telegram_enabled: 0 }), now)).toMatchObject({
      telegram: 'Off',
      issues: [],
    })
    expect(handlerHealth(metrics({ telegram_enabled: 1 }), now).telegram).toBe('Connecting')
  })

  it('keeps recent delivery failures visible after Telegram recovers', () => {
    const health = handlerHealth(
      metrics({
        telegram_enabled: 1,
        telegram_last_poll_timestamp_seconds: now / 1000,
        egress_dropped_total: 3,
        egress_last_dropped_timestamp_seconds: now / 1000 - 60,
      }),
      now,
    )
    expect(health.telegram).toBe('Connected')
    expect(health.issues[0].detail).toContain('successful run does not confirm reply delivery')
  })

  it('supports older handlers and stops flagging old drops as a current incident', () => {
    expect(handlerHealth(metrics({ egress_dropped_total: 3 }), now).issues).toHaveLength(1)
    expect(
      handlerHealth(
        metrics({
          egress_dropped_total: 3,
          egress_last_dropped_timestamp_seconds: now / 1000 - 86401,
        }),
        now,
      ).issues,
    ).toEqual([])
  })
})
