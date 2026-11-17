import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { ErrorStrip, Panel, StatusBadge } from '@/components/shared'
import { DataTable, type Column } from '@/components/table'
import { useToast } from '@/components/toast'
import { ApiError, api, useInboxEffects } from '@/lib/api'
import type { InboxEffect } from '@/lib/types'

type Outcome = 'done' | 'retry'
const OUTCOMES: Record<Outcome, { title: string; button: string; description: string }> = {
  done: {
    title: 'Mark this effect as done?',
    button: 'Mark done',
    description:
      'Use this when you have checked that the effect happened, for example the Telegram alert arrived or the message is already in its folder. It will not be attempted again.',
  },
  retry: {
    title: 'Retry this effect?',
    button: 'Retry',
    description:
      'The handler sets it back to pending and performs it again on its next pass. If it did happen, it may happen twice.',
  },
}

/**
 * Inbox effects whose outcome is unknown, with the handler's existing resolve action. Renders
 * nothing when the platform has no handler_admin_url or nothing needs resolving.
 */
export function InboxEffects({ subjects }: { subjects: Map<string, string> }) {
  const query = useInboxEffects()
  const [pending, setPending] = useState<{ effect: InboxEffect; outcome: Outcome } | null>(null)
  const missing = query.error instanceof ApiError && query.error.status === 404
  const effects = query.data?.effects
  if (missing || (!query.error && !effects?.length)) return null

  const columns: Column<InboxEffect>[] = [
    {
      key: 'status',
      header: 'Status',
      width: 104,
      priority: 2,
      cell: (e) => <StatusBadge status={e.status} kind="effect" />,
    },
    { key: 'kind', header: 'Effect', width: 92, priority: 2, cell: (e) => e.kind },
    {
      key: 'message',
      header: 'Message',
      cell: (e) => (
        <span className="ov-primary">
          <span className="cell-primary truncate">
            {subjects.get(e.message_id) || e.message_id}
          </span>
          <span className="cell-secondary truncate h-short">{e.kind}</span>
        </span>
      ),
      title: (e) => `${subjects.get(e.message_id) || ''}\n${e.message_id}`.trim(),
    },
    {
      key: 'target',
      header: 'Target',
      width: 140,
      priority: 2,
      mono: true,
      cell: (e) => e.target || '—',
      title: (e) => e.target,
    },
    {
      key: 'note',
      header: 'Note',
      priority: 3,
      cell: (e) => <span className="muted truncate">{e.note || '—'}</span>,
      title: (e) => e.note,
    },
    {
      key: 'actions',
      header: <span className="sr-only">Actions</span>,
      width: 150,
      narrowWidth: 116,
      align: 'right',
      cell: (e) => (
        <span className="row-actions">
          <Button
            size="xs"
            variant="outline"
            onClick={() => setPending({ effect: e, outcome: 'done' })}
            title="Mark done"
          >
            Done
          </Button>
          <Button
            size="xs"
            variant="outline"
            onClick={() => setPending({ effect: e, outcome: 'retry' })}
          >
            Retry
          </Button>
        </span>
      ),
    },
  ]
  return (
    <Panel
      title="Uncertain effects"
      count={effects?.length || null}
      meta="outcome unknown"
      id="effects"
    >
      <ErrorStrip query={query} label="Inbox effects" />
      <DataTable
        label="Uncertain inbox effects"
        columns={columns}
        rows={effects}
        rowKey={(e) => e.id}
        sortParam={false}
        loading={query.isPending}
        skeletonRows={2}
      />
      <ResolveDialog pending={pending} onClose={() => setPending(null)} />
    </Panel>
  )
}

function ResolveDialog({
  pending,
  onClose,
}: {
  pending: { effect: InboxEffect; outcome: Outcome } | null
  onClose: () => void
}) {
  const client = useQueryClient()
  const toast = useToast()
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<Error | null>(null)
  const copy = pending ? OUTCOMES[pending.outcome] : OUTCOMES.done
  async function submit() {
    if (!pending || !note.trim()) return
    setBusy(true)
    setError(null)
    try {
      await api('handler/inbox/effects/resolve', {
        method: 'POST',
        body: JSON.stringify({
          id: pending.effect.id,
          outcome: pending.outcome,
          note: note.trim(),
        }),
      })
      toast(
        pending.outcome === 'done'
          ? `${pending.effect.kind} effect marked done`
          : `${pending.effect.kind} effect queued to retry`,
      )
      setNote('')
      onClose()
      await client.invalidateQueries({ queryKey: ['inbox-effects'] })
    } catch (e) {
      const failure = e instanceof Error ? e : Error('Could not resolve the effect.')
      setError(failure)
      toast(`Resolve failed: ${failure.message}`, { tone: 'danger' })
    } finally {
      setBusy(false)
    }
  }
  return (
    <AlertDialog
      open={!!pending}
      onOpenChange={(open) => {
        if (open || busy) return
        setError(null)
        onClose()
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{copy.title}</AlertDialogTitle>
          <AlertDialogDescription>{copy.description}</AlertDialogDescription>
        </AlertDialogHeader>
        {pending && (
          <p className="resolve-target mono" title={pending.effect.id}>
            {pending.effect.kind} → {pending.effect.target || '—'} · {pending.effect.id}
          </p>
        )}
        <div className="resolve-note">
          <Label htmlFor="resolve-note">Reconciliation note (required)</Label>
          <Textarea
            id="resolve-note"
            value={note}
            rows={2}
            placeholder="What you checked"
            onChange={(e) => setNote(e.target.value)}
          />
        </div>
        <ErrorStrip error={error} message="Not resolved" />
        <AlertDialogFooter>
          <AlertDialogCancel disabled={busy}>Cancel</AlertDialogCancel>
          <Button
            type="button"
            variant={pending?.outcome === 'retry' ? 'outline' : 'default'}
            disabled={busy || !note.trim()}
            onClick={() => void submit()}
          >
            {busy ? 'Working…' : copy.button}
          </Button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
