import { useQuery } from '@tanstack/react-query'
import type { InboxSnapshot, ItemsSnapshot, Reminder, Schedule } from './types'

export class ApiError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.status = status
  }
}
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
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
  return response.status === 204 ? (undefined as T) : response.json()
}
export const useItems = () =>
  useQuery({
    queryKey: ['items'],
    queryFn: () => api<ItemsSnapshot>('worker/items'),
    refetchInterval: 4000,
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
