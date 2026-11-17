import { Fragment } from 'react'
import { Sheet, SheetContent } from '@/components/ui/sheet'
import { SHORTCUTS } from '@/lib/keys'

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
      </SheetContent>
    </Sheet>
  )
}
