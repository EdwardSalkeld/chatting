import { promValue, type PromMetrics } from './prom'

const H = 'chatting_message_handler_'

export interface HealthIssue {
  key: string
  title: string
  detail: string
}

export function handlerHealth(metrics: PromMetrics, now: number) {
  const value = (name: string) => promValue(metrics, H + name)
  const enabled = value('telegram_enabled')
  const lastPoll = value('telegram_last_poll_timestamp_seconds') || 0
  const failed = value('telegram_poll_failed') === 1
  const started = value('process_start_time_seconds') || 0
  const lastCheck = lastPoll || started
  const stale = enabled === 1 && lastCheck > 0 && now - lastCheck * 1000 > 300_000
  const code = value('telegram_poll_error_code')
  const lastSuccess = value('telegram_last_success_timestamp_seconds') || 0
  const telegram =
    enabled == null
      ? 'Unknown'
      : enabled === 0
        ? 'Off'
        : failed
          ? 'Failing'
          : stale
            ? 'Stale'
            : lastPoll === 0
              ? 'Connecting'
              : 'Connected'
  const issues: HealthIssue[] = []
  if (enabled === 1 && (failed || stale)) {
    issues.push({
      key: 'telegram',
      title: failed ? 'Telegram connection failing' : 'Telegram polling stalled',
      detail: failed
        ? `Telegram polling failed${code ? ` (error ${code})` : ''}. Incoming messages cannot be received until polling recovers; the handler is retrying.`
        : 'No Telegram API poll has completed in over five minutes. Incoming messages may be delayed.',
    })
  }
  const dropped = value('egress_dropped_total') || 0
  const lastDrop = value('egress_last_dropped_timestamp_seconds')
  // Old handlers expose only the lifetime count. Do not hide their delivery failures.
  if (dropped > 0 && (lastDrop == null || (lastDrop > 0 && now - lastDrop * 1000 < 86_400_000))) {
    issues.push({
      key: 'delivery',
      title: 'Outgoing messages were dropped',
      detail: `${dropped.toLocaleString()} egress messages dropped since the handler started. A successful run does not confirm reply delivery. Inspect the handler logs before resending.`,
    })
  }
  return { telegram, issues, lastSuccess, lastPoll, failed, stale }
}
