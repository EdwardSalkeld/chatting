import { describe, expect, it } from 'vitest'
import {
  foldRepeats,
  formatShow,
  matchesFind,
  parseShow,
  transcript,
  type TranscriptEntry,
} from './transcript'
import type { ActivityEvent } from './types'

const event = (
  id: number,
  content: string,
  phase = 'executor_stdout',
  detail: Record<string, unknown> = { content },
): ActivityEvent => ({
  activity_id: id,
  occurred_at: '2026-11-16T12:00:00Z',
  phase,
  summary: phase,
  detail,
})
const lines = (...rows: unknown[]) => rows.map((row) => JSON.stringify(row)).join('\n') + '\n'

describe('durable executor transcript', () => {
  it('reassembles native app-server messages split across polls', () => {
    const line = lines({
      method: 'item/completed',
      params: { item: { type: 'agentMessage', text: 'Complete response' } },
    })
    expect(transcript([event(1, line.slice(0, 20))], false).entries).toEqual([])
    expect(
      transcript([event(1, line.slice(0, 20)), event(2, line.slice(20))], false).entries,
    ).toMatchObject([{ kind: 'assistant', body: 'Complete response' }])
  })
  it('merges command start and finish into one entry with exit code and output', () => {
    const content = lines(
      {
        type: 'item.started',
        item: { id: 'item_0', type: 'command_execution', command: 'go test ./...' },
      },
      {
        type: 'item.completed',
        item: {
          id: 'item_0',
          type: 'command_execution',
          command: 'go test ./...',
          exit_code: 1,
          aggregated_output: 'test failed',
        },
      },
      { type: 'item.completed', item: { type: 'file_change', changes: [{ path: 'src/app.tsx' }] } },
    )
    expect(transcript([event(1, content)], true).entries).toMatchObject([
      { kind: 'command', command: 'go test ./...', exit: 1, output: 'test failed', tone: 'danger' },
      { label: 'Files changed', body: 'src/app.tsx' },
    ])
  })
  it('marks a started command as running until it completes', () => {
    const start = lines({
      method: 'item/started',
      params: { item: { id: 'c1', type: 'commandExecution', command: 'ls' } },
    })
    expect(transcript([event(1, start)], false).entries).toMatchObject([
      { command: 'ls', running: true },
    ])
  })
  it('never leaves a command running once its run or attempt has ended', () => {
    const start = (id: string) =>
      lines({
        method: 'item/started',
        params: { item: { id, type: 'commandExecution', command: `run ${id}` } },
      })
    const finished = event(5, '', 'task_finished', {
      result_status: 'dead_letter',
      reason_codes: [],
    })
    const { entries } = transcript(
      [
        event(1, '', 'executor_started', {}),
        event(2, start('a')),
        event(3, '', 'executor_started', {}),
        event(4, start('b')),
        finished,
      ],
      false,
    )
    const commands = entries.filter((e) => e.command)
    expect(commands).toMatchObject([
      { command: 'run a', running: false, interrupted: true, tone: 'warn' },
      { command: 'run b', running: false, interrupted: true },
    ])
    expect(transcript([event(1, start('c'))], true).entries).toMatchObject([
      { command: 'run c', running: false, interrupted: true },
    ])
  })
  it('keeps malformed output and final unterminated lines visible', () => {
    expect(
      transcript([event(1, 'plain output\ntrailing line')], true).entries.map((e) => e.body),
    ).toEqual(['plain output', 'trailing line'])
  })
  it('keeps delivery events as their own kind', () => {
    const events = [event(1, 'sent message', 'egress_message'), event(2, 'text\n')]
    expect(transcript(events, true).entries.map((e) => [e.kind, e.body])).toEqual([
      ['delivery', 'sent message'],
      ['command', 'text'],
    ])
  })
  it('retains unrecognised events but drops deltas and turn bookkeeping', () => {
    const events = [
      event(
        1,
        lines(
          { method: 'item/agentMessage/delta', params: {} },
          { method: 'turn/started', params: {} },
          { type: 'new.event' },
        ),
      ),
    ]
    const { entries } = transcript(events, true)
    expect(entries).toHaveLength(1)
    expect(entries[0].body).toContain('new.event')
  })
})

describe('session meta', () => {
  it('sums cumulative token usage per thread and tracks context fill', () => {
    const tokenUsage = (thread: string, total: number, input: number) => ({
      method: 'thread/tokenUsage/updated',
      params: {
        threadId: thread,
        tokenUsage: {
          last: { inputTokens: input, outputTokens: 10, totalTokens: input + 10 },
          total: {
            inputTokens: total,
            cachedInputTokens: 100,
            outputTokens: 50,
            reasoningOutputTokens: 20,
            totalTokens: total + 50,
          },
          modelContextWindow: 1000,
        },
      },
    })
    const { meta, entries } = transcript(
      [
        event(1, lines(tokenUsage('a', 300, 300), tokenUsage('a', 600, 300))),
        event(2, lines(tokenUsage('b', 400, 190))),
      ],
      true,
    )
    expect(entries).toEqual([])
    expect(meta.tokens).toEqual({
      input: 1000,
      cached: 200,
      output: 100,
      reasoning: 40,
      total: 1100,
    })
    expect(meta.contextWindow).toBe(1000)
    expect(meta.contextTokens).toBe(200)
  })
  it('sums exec turn usage', () => {
    const turn = {
      type: 'turn.completed',
      usage: { input_tokens: 100, cached_input_tokens: 40, output_tokens: 10 },
    }
    expect(transcript([event(1, lines(turn, turn))], true).meta.tokens).toEqual({
      input: 200,
      cached: 80,
      output: 20,
      reasoning: 0,
      total: 220,
    })
  })
  it('records the selected model once and a reroute', () => {
    const selected = { method: 'chatting/modelSelected', params: { model: 'gpt-a' } }
    const { meta, entries } = transcript(
      [
        event(
          1,
          lines(selected, selected, {
            method: 'model/rerouted',
            params: { fromModel: 'gpt-a', toModel: 'gpt-b', reason: 'risk' },
          }),
        ),
      ],
      true,
    )
    expect(meta).toMatchObject({
      model: 'gpt-b',
      reroute: { from: 'gpt-a', to: 'gpt-b', reason: 'risk', count: 1 },
    })
    expect(entries.map((e) => [e.label, e.tone])).toEqual([
      ['Model selected', undefined],
      ['Model rerouted', 'warn'],
    ])
  })
  it('counts attempts and the run id from lifecycle events', () => {
    const finished = {
      ...event(3, '', 'task_finished', { result_status: 'success', reason_codes: [] }),
      run_id: 'run:1',
    }
    const { meta } = transcript(
      [event(1, '', 'executor_started', {}), event(2, '', 'executor_started', {}), finished],
      true,
    )
    expect(meta).toMatchObject({ attempts: 2, runID: 'run:1', finishedStatus: 'success' })
  })
})

describe('error classification', () => {
  it('classifies failures as errors in the danger tone', () => {
    const { entries } = transcript(
      [
        event(1, 'Traceback (most recent call last):\n', 'executor_stderr'),
        event(2, '', 'executor_failed_attempt', { error: 'RuntimeError: overloaded' }),
        event(
          3,
          lines(
            { method: 'error', params: { error: { message: 'stream disconnected' } } },
            { method: 'turn/completed', params: { turn: { status: 'failed' } } },
            { type: 'turn.failed', error: { message: 'boom' } },
          ),
        ),
        event(4, '', 'task_finished', { result_status: 'execution_error', reason_codes: ['x'] }),
      ],
      true,
    )
    expect(entries.map((e) => [e.kind, e.tone])).toEqual(Array(6).fill(['error', 'danger']))
    expect(entries.map((e) => e.body)).toContain('stream disconnected')
  })
  it('keeps warnings on stderr and successful finishes out of the errors', () => {
    const { entries } = transcript(
      [
        event(1, 'WARN codex_core::config: ignoring unknown key\n', 'executor_stderr'),
        event(2, '', 'task_finished', { result_status: 'success', reason_codes: [] }),
        event(3, lines({ method: 'turn/completed', params: { turn: { status: 'completed' } } })),
      ],
      true,
    )
    expect(entries.map((e) => [e.kind, e.tone])).toEqual([
      ['system', 'warn'],
      ['system', 'ok'],
    ])
  })
})

describe('filters', () => {
  it('round-trips the show parameter', () => {
    expect(formatShow(parseShow(null))).toBeNull()
    expect([...parseShow('command,error,bogus')]).toEqual(['command', 'error'])
    expect(formatShow(parseShow('error,command'))).toBe('command,error')
    expect(formatShow(new Set())).toBe('none')
    expect(parseShow('none').size).toBe(0)
  })
  it('finds text in labels, bodies and commands', () => {
    const [entry] = transcript(
      [
        event(
          1,
          lines({
            method: 'item/completed',
            params: {
              item: { type: 'commandExecution', command: 'git status', aggregatedOutput: 'clean' },
            },
          }),
        ),
      ],
      true,
    ).entries
    expect(matchesFind(entry, 'GIT')).toBe(true)
    expect(matchesFind(entry, 'clean')).toBe(true)
    expect(matchesFind(entry, 'nope')).toBe(false)
  })
  it('handles long sessions', () => {
    const rows = Array.from({ length: 2000 }, (_, i) =>
      event(
        i,
        lines({
          method: 'item/completed',
          params: { item: { type: 'agentMessage', text: `step ${i}` } },
        }),
      ),
    )
    expect(transcript(rows, false).entries).toHaveLength(2000)
  })
})

describe('foldRepeats', () => {
  const entry = (key: string, body: string, kind: TranscriptEntry['kind'] = 'assistant') =>
    ({ key, kind, label: 'Assistant', body, at: `2026-11-17T12:00:0${key}Z` }) as TranscriptEntry
  it('folds consecutive lines that differ only in numbers and keeps errors separate', () => {
    const folded = foldRepeats([
      entry('1', 'tick · 12:00:01 UTC'),
      entry('2', 'tick · 12:00:06 UTC'),
      entry('3', 'tick · 12:00:11 UTC'),
      entry('4', 'Done'),
      entry('5', 'boom', 'error'),
      entry('6', 'boom', 'error'),
      entry('7', 'tick · 12:00:16 UTC'),
    ])
    expect(folded.map((f) => f.members.length)).toEqual([3, 1, 1, 1, 1])
    expect(folded[0].entry.key).toBe('1')
  })
})
