import * as React from 'react'
import { XIcon } from 'lucide-react'
import { Dialog as SheetPrimitive } from 'radix-ui'
import { cn } from '@/lib/utils'

function Sheet(props: React.ComponentProps<typeof SheetPrimitive.Root>) {
  return <SheetPrimitive.Root data-slot="sheet" {...props} />
}
function SheetTrigger(props: React.ComponentProps<typeof SheetPrimitive.Trigger>) {
  return <SheetPrimitive.Trigger data-slot="sheet-trigger" {...props} />
}
function SheetClose(props: React.ComponentProps<typeof SheetPrimitive.Close>) {
  return <SheetPrimitive.Close data-slot="sheet-close" {...props} />
}

function SheetContent({
  className,
  children,
  title,
  description,
  actions,
  footer,
  onOpenAutoFocus,
  ...props
}: Omit<React.ComponentProps<typeof SheetPrimitive.Content>, 'title'> & {
  title: React.ReactNode
  description?: React.ReactNode
  actions?: React.ReactNode
  footer?: React.ReactNode
}) {
  return (
    <SheetPrimitive.Portal>
      <SheetPrimitive.Overlay className="overlay" />
      <SheetPrimitive.Content
        data-slot="sheet-content"
        className={cn('sheet-content', className)}
        aria-describedby={description ? undefined : props['aria-describedby']}
        {...props}
        onOpenAutoFocus={(event) => {
          onOpenAutoFocus?.(event)
          if (event.defaultPrevented) return
          // Radix focuses the first button (often Close or Previous), which shows a stray focus ring.
          event.preventDefault()
          ;(event.currentTarget as HTMLElement | null)?.focus()
        }}
      >
        <div className="sheet-header">
          <div>
            <SheetPrimitive.Title className="sheet-title">{title}</SheetPrimitive.Title>
            {description && (
              <SheetPrimitive.Description className="sheet-description">
                {description}
              </SheetPrimitive.Description>
            )}
          </div>
          {actions}
          <SheetPrimitive.Close className="icon-button" aria-label="Close">
            <XIcon />
          </SheetPrimitive.Close>
        </div>
        <div className="sheet-body">{children}</div>
        {footer && <div className="sheet-footer">{footer}</div>}
      </SheetPrimitive.Content>
    </SheetPrimitive.Portal>
  )
}

export { Sheet, SheetTrigger, SheetClose, SheetContent }
