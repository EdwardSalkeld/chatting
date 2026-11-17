import { afterEach, expect, it, vi } from 'vitest'
import { api, ApiError, apiText, runsPath } from './api'

afterEach(() => vi.unstubAllGlobals())

it('explains worker 404 responses whose JSON body is null', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('null', { status: 404 })))
  await expect(api('worker/runs/missing')).rejects.toMatchObject({
    message: 'This item or run could not be found.',
    status: 404,
  })
})

it('retains the handler validation error and the upstream status', async () => {
  vi.stubGlobal(
    'fetch',
    vi
      .fn()
      .mockResolvedValue(
        new Response('{"error":"cron must contain exactly 5 fields"}', { status: 400 }),
      ),
  )
  await expect(api('handler/schedules')).rejects.toBeInstanceOf(ApiError)
})

it('handles successful empty mutation responses', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
  await expect(api('handler/schedules/example', { method: 'DELETE' })).resolves.toBeUndefined()
})

it('builds run list paths without empty filters', () => {
  expect(runsPath()).toBe('worker/runs')
  expect(
    runsPath({ status: 'dead_letter', source: '', limit: 50, since: '2026-11-17T00:00:00Z' }),
  ).toBe('worker/runs?status=dead_letter&limit=50&since=2026-11-17T00%3A00%3A00Z')
})

it('returns metrics as raw text and keeps upstream errors', async () => {
  vi.stubGlobal(
    'fetch',
    vi
      .fn()
      .mockResolvedValueOnce(new Response('chatting_up 1\n', { status: 200 }))
      .mockResolvedValueOnce(new Response('{"error":"Service unavailable."}', { status: 502 })),
  )
  await expect(apiText('handler/metrics')).resolves.toBe('chatting_up 1\n')
  await expect(apiText('worker/metrics')).rejects.toMatchObject({
    message: 'Service unavailable.',
    status: 502,
  })
})
