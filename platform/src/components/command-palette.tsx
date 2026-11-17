import { Command } from 'cmdk'
import { Dialog as DialogPrimitive } from 'radix-ui'
import { useNavigate } from 'react-router-dom'
import { Bell, CalendarClock, Keyboard, ListTodo, Moon, RefreshCw } from 'lucide-react'
import { useItems, useReminders, useSchedules } from '@/lib/api'
import { encode, itemName } from '@/lib/format'
import { NAV } from '@/components/nav'

export function CommandPalette({
  open,
  onOpenChange,
  onRefetch,
  onHelp,
  onToggleTheme,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onRefetch: () => void
  onHelp: () => void
  onToggleTheme: () => void
}) {
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="overlay" />
        <DialogPrimitive.Content className="palette" aria-describedby={undefined}>
          <DialogPrimitive.Title className="sr-only">Command palette</DialogPrimitive.Title>
          {open && (
            <PaletteBody
              close={() => onOpenChange(false)}
              onRefetch={onRefetch}
              onHelp={onHelp}
              onToggleTheme={onToggleTheme}
            />
          )}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}

function PaletteBody({
  close,
  onRefetch,
  onHelp,
  onToggleTheme,
}: {
  close: () => void
  onRefetch: () => void
  onHelp: () => void
  onToggleTheme: () => void
}) {
  const navigate = useNavigate()
  const items = useItems().data?.items || []
  const schedules = useSchedules().data?.schedules || []
  const reminders = useReminders().data?.reminders || []
  function go(to: string) {
    close()
    navigate(to)
  }
  function run(action: () => void) {
    close()
    action()
  }
  return (
    <Command label="Command palette" loop>
      <Command.Input placeholder="Jump to a page, work item, schedule or reminder…" autoFocus />
      <Command.List>
        <Command.Empty>No matches</Command.Empty>
        <Command.Group heading="Pages">
          {NAV.map(({ to, label, icon: Icon, key }) => (
            <Command.Item key={to} value={`page ${label}`} onSelect={() => go(to)}>
              <Icon />
              <span>{label}</span>
              <small>g {key}</small>
            </Command.Item>
          ))}
        </Command.Group>
        {items.length > 0 && (
          <Command.Group heading="Work items">
            {items.map((item) => (
              <Command.Item
                key={item.work_item_id}
                value={`item ${itemName(item)} ${item.work_item_id}`}
                onSelect={() => go(`/items/${encode(item.work_item_id)}`)}
              >
                <ListTodo />
                <span>{itemName(item)}</span>
                <small>{item.run_count} runs</small>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        {schedules.length > 0 && (
          <Command.Group heading="Schedules">
            {schedules.map((s) => (
              <Command.Item
                key={s.schedule_id}
                value={`schedule ${s.job_name} ${s.schedule_id}`}
                onSelect={() => go(`/schedules?edit=${encode(s.schedule_id)}`)}
              >
                <CalendarClock />
                <span>{s.job_name}</span>
                <small className="mono">{s.cron}</small>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        {reminders.length > 0 && (
          <Command.Group heading="Reminders">
            {reminders.map((r) => (
              <Command.Item
                key={r.reminder_id}
                value={`reminder ${r.prompt} ${r.reminder_id}`}
                onSelect={() => go(`/reminders?edit=${encode(r.reminder_id)}`)}
              >
                <Bell />
                <span>{r.prompt}</span>
                <small>{r.status}</small>
              </Command.Item>
            ))}
          </Command.Group>
        )}
        <Command.Group heading="Actions">
          <Command.Item value="action refetch refresh data" onSelect={() => run(onRefetch)}>
            <RefreshCw />
            <span>Refetch data</span>
            <small>r</small>
          </Command.Item>
          <Command.Item value="action theme dark light" onSelect={() => run(onToggleTheme)}>
            <Moon />
            <span>Toggle theme</span>
          </Command.Item>
          <Command.Item value="action keyboard shortcuts help" onSelect={() => run(onHelp)}>
            <Keyboard />
            <span>Keyboard shortcuts</span>
            <small>?</small>
          </Command.Item>
        </Command.Group>
      </Command.List>
    </Command>
  )
}
