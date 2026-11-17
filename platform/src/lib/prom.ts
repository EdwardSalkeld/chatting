export interface PromSample {
  name: string
  labels: Record<string, string>
  value: number
}
export interface PromMetrics {
  samples: PromSample[]
  help: Record<string, string>
  type: Record<string, string>
}

const NUMBER: Record<string, number> = {
  NaN: NaN,
  '+Inf': Infinity,
  Inf: Infinity,
  '-Inf': -Infinity,
}

function parseLabels(text: string, start: number): [Record<string, string>, number] {
  const labels: Record<string, string> = {}
  let i = start
  while (i < text.length) {
    while (text[i] === ' ' || text[i] === ',') i++
    if (text[i] === '}') return [labels, i + 1]
    const eq = text.indexOf('=', i)
    if (eq < 0 || text[eq + 1] !== '"') break
    const key = text.slice(i, eq).trim()
    let value = ''
    i = eq + 2
    while (i < text.length && text[i] !== '"') {
      if (text[i] === '\\' && i + 1 < text.length) {
        const next = text[i + 1]
        value += next === 'n' ? '\n' : next
        i += 2
      } else value += text[i++]
    }
    labels[key] = value
    i++
  }
  throw new Error('unterminated label set')
}

/** Parses the Prometheus text exposition format. Malformed lines are skipped. */
export function parseProm(text: string): PromMetrics {
  const metrics: PromMetrics = { samples: [], help: {}, type: {} }
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line) continue
    if (line.startsWith('#')) {
      const match = /^#\s+(HELP|TYPE)\s+(\S+)\s?(.*)$/.exec(line)
      if (match) metrics[match[1] === 'HELP' ? 'help' : 'type'][match[2]] = match[3]
      continue
    }
    try {
      const brace = line.indexOf('{')
      const space = line.search(/\s/)
      let name: string,
        labels: Record<string, string> = {},
        rest: string
      if (brace >= 0 && (space < 0 || brace < space)) {
        name = line.slice(0, brace)
        const [parsed, end] = parseLabels(line, brace + 1)
        labels = parsed
        rest = line.slice(end)
      } else {
        if (space < 0) continue
        name = line.slice(0, space)
        rest = line.slice(space)
      }
      const token = rest.trim().split(/\s+/)[0]
      const value = token in NUMBER ? NUMBER[token] : Number(token)
      if (!name || token === '' || (Number.isNaN(value) && token !== 'NaN')) continue
      metrics.samples.push({ name, labels, value })
    } catch {
      continue
    }
  }
  return metrics
}

function matches(sample: PromSample, labels?: Record<string, string>) {
  return !labels || Object.entries(labels).every(([k, v]) => sample.labels[k] === v)
}

export function promSamples(
  metrics: PromMetrics | undefined,
  name: string,
  labels?: Record<string, string>,
): PromSample[] {
  return metrics?.samples.filter((s) => s.name === name && matches(s, labels)) || []
}

/** The first sample of `name` whose labels include `labels`, or undefined. */
export function promValue(
  metrics: PromMetrics | undefined,
  name: string,
  labels?: Record<string, string>,
): number | undefined {
  return metrics?.samples.find((s) => s.name === name && matches(s, labels))?.value
}

export function sampleKey(sample: PromSample) {
  const labels = Object.entries(sample.labels)
    .map(([k, v]) => `${k}="${v}"`)
    .join(',')
  return labels ? `${sample.name}{${labels}}` : sample.name
}

/** Sum of every sample of `name` matching `labels`, or undefined when there is none. */
export function promSum(
  metrics: PromMetrics | undefined,
  name: string,
  labels?: Record<string, string>,
): number | undefined {
  const samples = promSamples(metrics, name, labels)
  return samples.length ? samples.reduce((sum, s) => sum + s.value, 0) : undefined
}
