import type { ActivityEvent } from './types'

export interface TranscriptEntry {
  label: string
  body: string
  at: string
  kind: 'assistant' | 'command' | 'system' | 'delivery'
  runID?: string
}
interface NativeItem {
  type?: string
  text?: string
  command?: string
  exit_code?: number
  exitCode?: number
  aggregated_output?: string
  aggregatedOutput?: string
  changes?: { path: string }[]
}
interface NativeEvent {
  method?: string
  type?: string
  message?: string
  item?: NativeItem
  params?: { item?: NativeItem; turn?: { status?: string }; error?: { message?: string } }
}

// Rebuild from durable events so split stdout lines survive polling boundaries
// and toggling delivery visibility cannot lose transcript content.
export function transcript(
  events: ActivityEvent[],
  showDelivery: boolean,
  finished: boolean,
): TranscriptEntry[] {
  const entries: TranscriptEntry[] = []
  let buffer = '',
    bufferAt = ''
  function add(
    label: string,
    body: string,
    at: string,
    kind: TranscriptEntry['kind'] = 'system',
    runID?: string,
  ) {
    entries.push({ label, body, at, kind, runID })
  }
  function output(line: string, at: string) {
    if (!line.trim()) return
    try {
      const entry = JSON.parse(line) as NativeEvent
      const item = entry.params?.item || entry.item || {}
      const completed = entry.method === 'item/completed' || entry.type === 'item.completed'
      const started = entry.method === 'item/started' || entry.type === 'item.started'
      if (completed && ['agentMessage', 'agent_message'].includes(item.type || ''))
        add('Assistant', item.text || '', at, 'assistant')
      else if (
        (started || completed) &&
        ['commandExecution', 'command_execution'].includes(item.type || '')
      ) {
        const exit = item.exitCode ?? item.exit_code
        add(
          completed
            ? `Command finished${exit == null ? '' : ` · exit ${exit}`}`
            : 'Command started',
          [item.command, completed ? item.aggregatedOutput || item.aggregated_output : '']
            .filter(Boolean)
            .join('\n'),
          at,
          'command',
        )
      } else if (completed && item.type === 'file_change')
        add(
          'Files changed',
          item.changes?.map((change) => change.path).join('\n') || '',
          at,
          'command',
        )
      else if (entry.type === 'turn.completed' || entry.method === 'turn/completed')
        add(`Turn ${entry.params?.turn?.status || 'completed'}`, '', at)
      else if (entry.type === 'error' || entry.method === 'error')
        add('Executor error', entry.params?.error?.message || entry.message || line, at)
      // Keep unrecognised native output inspectable instead of silently dropping it.
      else if (!entry.method?.includes('/delta')) add('Executor event', line, at, 'command')
    } catch {
      add('Executor output', line, at, 'command')
    }
  }
  for (const event of events) {
    const detail = event.detail || {}
    const body = detail.content ?? detail.body ?? detail.error
    if (event.phase === 'executor_stdout' && typeof body === 'string') {
      buffer += body
      bufferAt = event.occurred_at
      const parts = buffer.split('\n')
      buffer = parts.pop() || ''
      for (const line of parts) output(line, event.occurred_at)
      continue
    }
    if (event.phase.startsWith('egress_') && !showDelivery) continue
    const content = event.phase === 'task_received' ? (detail.request ?? body) : body
    add(
      event.summary || event.phase,
      typeof content === 'string' ? content : content ? JSON.stringify(content, null, 2) : '',
      event.occurred_at,
      event.phase.startsWith('egress_') ? 'delivery' : 'system',
      event.run_id,
    )
  }
  if (buffer.trim()) {
    if (finished) output(buffer, bufferAt)
    else {
      try {
        JSON.parse(buffer)
        output(buffer, bufferAt)
      } catch {
        /* Incomplete JSON waits for the next chunk. */
      }
    }
  }
  return entries
}
