import { Fragment } from 'react'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import { SHORTCUTS } from '@/lib/keys'
import { StatusBadge } from '@/components/shared'

const LEGEND: { status: string; label: string; meaning: string }[] = [
  { status: 'live', label: 'Live', meaning: 'An executor is running' },
  { status: 'queued', label: 'Queued', meaning: 'Waiting for a slot or a scheduled time' },
  { status: 'success', label: 'Success', meaning: 'Finished normally' },
  {
    status: 'execution_error',
    label: 'Failed',
    meaning: 'Amber: the run failed; the item can run again',
  },
  {
    status: 'dead_letter',
    label: 'Dead letter',
    meaning: 'Red: retries are exhausted, or a service is down',
  },
  { status: 'idle', label: 'Idle', meaning: 'Nothing pending' },
]

export function ShortcutHelp({
  open,
  onOpenChange,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent title="Keyboard shortcuts" aria-describedby={undefined}>
        <dl className="shortcut-list">
          {SHORTCUTS.map(({ keys, label }) => (
            <Fragment key={label}>
              <dt>{label}</dt>
              <dd>
                {keys.map((key) => (
                  <kbd key={key}>{key}</kbd>
                ))}
              </dd>
            </Fragment>
          ))}
        </dl>
        <h3 className="legend-title">Status colours</h3>
        <dl className="shortcut-list legend-list">
          {LEGEND.map(({ status, label, meaning }) => (
            <Fragment key={status}>
              <dt>
                <StatusBadge status={status} kind="run">
                  {label}
                </StatusBadge>
              </dt>
              <dd>{meaning}</dd>
            </Fragment>
          ))}
        </dl>
      </SheetContent>
    </Sheet>
  )
}
