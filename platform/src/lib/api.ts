import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type {
  ActivitySnapshot,
  DeadLettersSnapshot,
  InboxSnapshot,
  ItemsSnapshot,
  Reminder,
  RunsQuery,
  RunsSnapshot,
  Schedule,
} from './types'

export class ApiError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.status = status
  }
}
async function request(path: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(`/api/${path}`, {
    ...init,
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json', ...init?.headers },
  })
  if (!response.ok) {
    const text = await response.text()
    let message =
      response.status === 404
        ? 'This item or run could not be found.'
        : `Request failed (${response.status})`
    try {
      const payload: unknown = JSON.parse(text)
      if (
        payload &&
        typeof payload === 'object' &&
        'error' in payload &&
        typeof payload.error === 'string'
      )
        message = payload.error
    } catch {
      if (text.length < 500) message = text.trim() || message
    }
    throw new ApiError(message, response.status)
  }
  return response
}
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  return response.status === 204 ? (undefined as T) : response.json()
}
export async function apiText(path: string): Promise<string> {
  return (await request(path)).text()
}
export function runsPath(params: RunsQuery = {}): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params))
    if (value !== undefined && value !== '') search.set(key, String(value))
  const query = search.toString()
  return query ? `worker/runs?${query}` : 'worker/runs'
}
export const useItems = () =>
  useQuery({
    queryKey: ['items'],
    queryFn: () => api<ItemsSnapshot>('worker/items'),
    refetchInterval: 4000,
  })
export const useActivity = () =>
  useQuery({
    queryKey: ['activity'],
    queryFn: () => api<ActivitySnapshot>('worker/activity'),
    refetchInterval: 4000,
  })
export const useRuns = (params: RunsQuery = {}) =>
  useQuery({
    queryKey: ['runs', params],
    queryFn: () => api<RunsSnapshot>(runsPath(params)),
    refetchInterval: 5000,
    placeholderData: (previous) => previous,
  })
function useMinute() {
  const [minute, setMinute] = useState(() => Math.floor(Date.now() / 60000) * 60000)
  useEffect(() => {
    const id = setInterval(() => setMinute(Math.floor(Date.now() / 60000) * 60000), 5000)
    return () => clearInterval(id)
  }, [])
  return minute
}
/** Runs from the last 24h; `since` is bucketed to the minute so every caller shares one query. */
export function useDayRuns() {
  return useRuns({ limit: 1000, since: new Date(useMinute() - 86400e3).toISOString() })
}
export const useDeadLetters = (limit?: number) =>
  useQuery({
    queryKey: ['dead-letters', limit],
    queryFn: () =>
      api<DeadLettersSnapshot>(
        limit ? `worker/dead-letters?limit=${limit}` : 'worker/dead-letters',
      ),
    refetchInterval: 10000,
  })
export const useHandlerMetrics = () =>
  useQuery({
    queryKey: ['metrics', 'handler'],
    queryFn: () => apiText('handler/metrics'),
    refetchInterval: 10000,
  })
export const useWorkerMetrics = () =>
  useQuery({
    queryKey: ['metrics', 'worker'],
    queryFn: () => apiText('worker/metrics'),
    refetchInterval: 10000,
  })
export const useSchedules = () =>
  useQuery({
    queryKey: ['schedules'],
    queryFn: () => api<{ schedules: Schedule[] }>('handler/schedules'),
    refetchInterval: 10000,
  })
export const useReminders = () =>
  useQuery({
    queryKey: ['reminders'],
    queryFn: () => api<{ reminders: Reminder[] }>('handler/reminders?status=all&latest=1'),
    refetchInterval: 10000,
  })
export const useInbox = () =>
  useQuery({
    queryKey: ['inbox'],
    queryFn: () => api<InboxSnapshot>('handler/inbox'),
    refetchInterval: 10000,
    retry: false,
  })
