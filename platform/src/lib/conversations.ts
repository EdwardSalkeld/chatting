export interface Conversation {
  id: string
  title: string
  target: string
  topic_id: number | null
  work_item_id: string
}
export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string | null
  sender?: string | null
  task_id: string | null
  occurred_at: string
  transport: 'web' | 'telegram'
  status: string
  attachments?: { name?: string; uri: string }[]
}
export interface Submission {
  task_id: string
  content: string
  occurred_at: string
  state: string
}
export function conversationMessages(history: ChatMessage[], submissions: Submission[]) {
  const rows = new Map(history.map((message) => [message.id, message]))
  const received = new Set(
    history.filter((m) => m.role === 'user' && m.transport === 'web').map((m) => m.task_id),
  )
  for (const submission of submissions) {
    if (received.has(submission.task_id)) continue
    const id = 'user:' + submission.task_id
    rows.set(id, {
      id,
      role: 'user',
      content: submission.content,
      task_id: submission.task_id,
      occurred_at: submission.occurred_at,
      transport: 'web',
      status: 'queued',
    })
  }
  return [...rows.values()].sort(
    (a, b) => a.occurred_at.localeCompare(b.occurred_at) || a.id.localeCompare(b.id),
  )
}
