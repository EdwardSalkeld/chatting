import type { RunRow } from './types'

export interface ModelUsage {
  model: string
  runs: number
  input: number
  cached: number
  output: number
  reasoning: number
  total: number
}

export function usageByModel(runs: RunRow[]): ModelUsage[] {
  const map = new Map<string, ModelUsage>()
  for (const run of runs) {
    const t = run.tokens
    if (!t) continue
    const model = run.model || run.executed_model_tier || 'unknown'
    const row = map.get(model) || {
      model,
      runs: 0,
      input: 0,
      cached: 0,
      output: 0,
      reasoning: 0,
      total: 0,
    }
    row.runs++
    row.input += t.input_tokens || 0
    row.cached += t.cached_input_tokens || 0
    row.output += t.output_tokens || 0
    row.reasoning += t.reasoning_output_tokens || 0
    row.total += t.total_tokens || 0
    map.set(model, row)
  }
  return [...map.values()]
}
