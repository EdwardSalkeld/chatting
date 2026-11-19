import { Link } from 'react-router-dom'
import { useHandlerMetrics } from '@/lib/api'
import { handlerHealth } from '@/lib/health'
import { parseProm } from '@/lib/prom'
import { useNow } from './shared'

function useCommunicationHealth() {
  const metrics = useHandlerMetrics()
  const health = handlerHealth(parseProm(metrics.data || ''), useNow(1000))
  return {
    ...health,
    telegram: metrics.isError ? 'Unknown' : health.telegram,
    issues: metrics.isError
      ? [
          ...health.issues,
          {
            key: 'metrics',
            title: 'Connection health unavailable',
            detail:
              'Cannot check Telegram or delivery health because handler metrics could not be refreshed.',
          },
        ]
      : health.issues,
  }
}

export function CommunicationBanner() {
  const { issues } = useCommunicationHealth()
  if (!issues.length) return null
  return (
    <section
      className="communication-banner"
      role="alert"
      aria-label="Communication failures"
      data-tone="danger"
    >
      {issues.map((issue) => (
        <div key={issue.key}>
          <strong>{issue.title}</strong>
          <span>{issue.detail}</span>
        </div>
      ))}
      <Link to="/system">View system details</Link>
    </section>
  )
}

export function TelegramStatus() {
  const { telegram } = useCommunicationHealth()
  const tone =
    telegram === 'Failing' || telegram === 'Stale'
      ? 'danger'
      : telegram === 'Connected'
        ? 'ok'
        : telegram === 'Off'
          ? 'neutral'
          : 'warn'
  return (
    <Link to="/system" className="age-chip" data-tone={tone} title="Telegram polling connectivity">
      <span className="status-dot" data-tone={tone} />
      <span>Telegram {telegram.toLowerCase()}</span>
    </Link>
  )
}
