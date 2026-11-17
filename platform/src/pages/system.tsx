import { InlineEmpty, Panel, usePageTitle } from '@/components/shared'

export function System() {
  usePageTitle('System')
  return (
    <Panel title="System">
      <InlineEmpty>System health is not built yet</InlineEmpty>
    </Panel>
  )
}
