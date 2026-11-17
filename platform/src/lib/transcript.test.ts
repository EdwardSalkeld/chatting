import { describe, expect, it } from 'vitest'
import { transcript } from './transcript'
import type { ActivityEvent } from './types'

const event = (id: number, content: string, phase = 'executor_stdout'): ActivityEvent => ({
  activity_id: id,
  occurred_at: '2026-11-16T12:00:00Z',
  phase,
  summary: phase,
  detail: { content },
})
describe('durable executor transcript', () => {
  it('reassembles native app-server messages split across polls', () => {
    const line =
      JSON.stringify({
        method: 'item/completed',
        params: { item: { type: 'agentMessage', text: 'Complete response' } },
      }) + '\n'
    expect(transcript([event(1, line.slice(0, 20))], false, false)).toEqual([])
    expect(
      transcript([event(1, line.slice(0, 20)), event(2, line.slice(20))], false, false),
    ).toMatchObject([{ label: 'Assistant', body: 'Complete response' }])
  })
  it('shows legacy command output, exit codes and file changes', () => {
    const content =
      [
        {
          type: 'item.completed',
          item: {
            type: 'command_execution',
            command: 'go test ./...',
            exit_code: 1,
            aggregated_output: 'test failed',
          },
        },
        {
          type: 'item.completed',
          item: { type: 'file_change', changes: [{ path: 'src/app.tsx' }] },
        },
      ]
        .map((row) => JSON.stringify(row))
        .join('\n') + '\n'
    expect(transcript([event(1, content)], false, true)).toMatchObject([
      { label: 'Command finished · exit 1', body: 'go test ./...\ntest failed' },
      { label: 'Files changed', body: 'src/app.tsx' },
    ])
  })
  it('keeps malformed output and final unterminated lines visible', () => {
    expect(
      transcript([event(1, 'plain output\ntrailing line')], false, true).map((e) => e.body),
    ).toEqual(['plain output', 'trailing line'])
    expect(transcript([event(1, 'warning', 'executor_stderr')], false, false)[0].body).toBe(
      'warning',
    )
  })
  it('can toggle sent messages without losing other output', () => {
    const events = [event(1, 'sent message', 'egress_message'), event(2, 'text\n')]
    expect(transcript(events, false, true).map((e) => e.body)).toEqual(['text'])
    expect(transcript(events, true, true).map((e) => e.body)).toEqual(['sent message', 'text'])
  })
  it('retains unrecognised events but avoids repeating text deltas', () => {
    const events = [
      event(1, '{"method":"item/agentMessage/delta","params":{}}\n{"type":"new.event"}\n'),
    ]
    expect(transcript(events, false, true)).toHaveLength(1)
    expect(transcript(events, false, true)[0].body).toContain('new.event')
  })
})
