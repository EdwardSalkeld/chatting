import { expect, it } from 'vitest'
import { usageByModel } from './usage'
import type { RunRow } from './types'

const run = (model: string | null, total: number | null, tier: string | null = null): RunRow =>
  ({
    run_id: `r${Math.random()}`,
    model,
    executed_model_tier: tier,
    tokens:
      total == null
        ? null
        : {
            input_tokens: total - 10,
            cached_input_tokens: 5,
            output_tokens: 10,
            reasoning_output_tokens: 2,
            total_tokens: total,
          },
  }) as RunRow

it('sums token usage per model and skips runs without usage', () => {
  const rows = usageByModel([run('a', 100), run('a', 50), run('b', 20), run(null, null)])
  expect(rows).toEqual([
    { model: 'a', runs: 2, input: 130, cached: 10, output: 20, reasoning: 4, total: 150 },
    { model: 'b', runs: 1, input: 10, cached: 5, output: 10, reasoning: 2, total: 20 },
  ])
})
it('falls back to the tier when the model id is unknown', () => {
  expect(usageByModel([run(null, 30, 'high')])[0].model).toBe('high')
})
