import { afterEach, expect, it, vi } from 'vitest'
import { api, ApiError } from './api'

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
