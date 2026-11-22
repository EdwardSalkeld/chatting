import { expect, it } from 'vitest'
import { conversationMessages, type ChatMessage } from './conversations'

it('replaces durable queued receipts with the worker message and preserves its state', () => {
  const message: ChatMessage = {
    id: 'user:task:web:1',
    task_id: 'task:web:1',
    role: 'user',
    content: 'Hello',
    occurred_at: '2026-11-19T22:00:00Z',
    transport: 'web',
    status: 'answered',
  }
  const rows = conversationMessages(
    [message],
    [
      {
        task_id: message.task_id!,
        content: 'Hello',
        occurred_at: message.occurred_at,
        state: 'queued',
      },
    ],
  )
  expect(rows).toEqual([message])
})

it('shows accepted submissions before the worker receives them and deduplicates paged history', () => {
  const message: ChatMessage = {
    id: 'reply:event',
    task_id: 'task:web:1',
    role: 'assistant',
    content: 'Answer',
    occurred_at: '2026-11-19T22:01:00Z',
    transport: 'web',
    status: 'delivered',
  }
  const rows = conversationMessages(
    [message, message],
    [
      {
        task_id: 'task:web:1',
        content: 'Hello',
        occurred_at: '2026-11-19T22:00:00Z',
        state: 'queued',
      },
    ],
  )
  expect(rows).toHaveLength(2)
  expect(rows[0].status).toBe('queued')
  expect(rows[1]).toEqual(message)
})
