import { useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { ExternalLink, Mail, ShieldCheck } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Empty,
  ErrorNotice,
  Loading,
  PageHeading,
  Panel,
  SearchInput,
  StatusBadge,
} from '@/components/shared'
import { ApiError, useInbox } from '@/lib/api'
import { dateTime, safeMailLink } from '@/lib/format'
import type { InboxRow } from '@/lib/types'

export function InboxPage() {
  const query = useInbox()
  const [search, setSearch] = useState(''),
    [selected, setSelected] = useState<InboxRow | null>(null)
  const [params, setParams] = useSearchParams()
  const category = ['action', 'review', 'order', 'news', 'waiting'].includes(
    params.get('category') || '',
  )
    ? params.get('category')!
    : 'all'
  function setCategory(value: string) {
    setParams(value === 'all' ? {} : { category: value })
  }
  const data = query.data
  const rows = (data?.rows || []).filter(
    (row) =>
      (category === 'all' || row.decision === category) &&
      `${row.subject} ${row.sender} ${row.why}`.toLowerCase().includes(search.toLowerCase()),
  )
  const missing = query.error instanceof ApiError && query.error.status === 404
  return (
    <>
      <PageHeading
        title="Personal inbox"
        action={
          data &&
          !query.isError && (
            <StatusBadge status={data.dry_run ? 'preview' : 'active'}>
              {data.dry_run ? 'Preview' : 'Active'}
            </StatusBadge>
          )
        }
      />
      {query.isPending ? (
        <Panel>
          <Loading />
        </Panel>
      ) : missing ? (
        <Panel>
          <Empty
            title="Personal inbox isn’t available"
            description="Enable personal inbox in the handler configuration, and use a handler version with the decisions API."
            action={
              <Button
                variant="outline"
                onClick={() => {
                  void query.refetch()
                }}
              >
                Check again
              </Button>
            }
          />
        </Panel>
      ) : (
        <>
          <ErrorNotice
            error={query.error}
            retry={() => {
              void query.refetch()
            }}
          />
          {data && (
            <>
              <div className="inbox-health">
                <div>
                  <ShieldCheck size={20} />
                  <span>
                    Confidence threshold<strong>{Math.round(Number(data.threshold) * 100)}%</strong>
                  </span>
                </div>
                <div>
                  <span>
                    Awaiting classification<strong>{data.awaiting}</strong>
                  </span>
                </div>
                <div>
                  <span>
                    Stuck<strong>{data.stuck}</strong>
                  </span>
                </div>
                <div>
                  <span>
                    Rejected results<strong>{data.dead_letters}</strong>
                  </span>
                </div>
              </div>
              {!!data.awaiting && (
                <p className="subtle-notice">
                  {data.awaiting} {data.awaiting === 1 ? 'message' : 'messages'} awaiting
                  classification · Oldest waiting {data.oldest_wait}
                </p>
              )}
              {data.last_error && <ErrorNotice error={new Error(data.last_error)} />}
              <div className="list-toolbar">
                <div className="filter-tabs inbox-filters">
                  {['all', 'action', 'review', 'order', 'news', 'waiting'].map((value) => (
                    <button
                      key={value}
                      aria-pressed={category === value}
                      className={category === value ? 'selected' : ''}
                      onClick={() => setCategory(value)}
                    >
                      {value === 'all' ? 'All decisions' : value[0].toUpperCase() + value.slice(1)}
                    </button>
                  ))}
                </div>
                <SearchInput
                  value={search}
                  onChange={setSearch}
                  placeholder="Search inbox decisions…"
                />
              </div>
              <Panel>
                {rows.length ? (
                  <div className="inbox-list">
                    {rows.map((row, index) => (
                      <button
                        className="inbox-row"
                        key={`${row.received_at}-${index}`}
                        onClick={() => setSelected(row)}
                      >
                        <div className="mail-icon">
                          <Mail size={18} />
                        </div>
                        <div className="inbox-row-main">
                          <div>
                            <strong>{row.sender}</strong>
                            <time>{dateTime(row.received_at)}</time>
                          </div>
                          <h3>{row.subject || '(No subject)'}</h3>
                          <p>
                            {row.why ||
                              (row.unclassified
                                ? 'Waiting for classification'
                                : 'No reason recorded')}
                          </p>
                        </div>
                        <div className="inbox-row-status">
                          <StatusBadge status={row.decision} />
                          {row.score && (
                            <span>{Math.round(Number(row.score) * 100)}% confidence</span>
                          )}
                        </div>
                      </button>
                    ))}
                  </div>
                ) : (
                  <Empty
                    title={search || category !== 'all' ? 'No matching decisions' : 'No decisions'}
                  />
                )}
              </Panel>
              <p className="list-caption">
                {rows.length} of {data.rows.length} decisions
              </p>
            </>
          )}
        </>
      )}
      <Dialog
        open={!!selected}
        onOpenChange={(value) => {
          if (!value) setSelected(null)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{selected?.subject || '(No subject)'}</DialogTitle>
            <DialogDescription>
              {selected?.sender} · {dateTime(selected?.received_at)}
            </DialogDescription>
          </DialogHeader>
          {selected && (
            <>
              <div className="decision-detail">
                <StatusBadge status={selected.decision} />
                {selected.score && (
                  <strong>{Math.round(Number(selected.score) * 100)}% confidence</strong>
                )}
              </div>
              <div className="decision-reason">
                <h3>Decision reason</h3>
                <p>
                  {selected.why ||
                    (selected.unclassified ? 'Awaiting classification.' : 'No reason recorded.')}
                </p>
              </div>
              <div className="form-footer">
                <Button variant="outline" onClick={() => setSelected(null)}>
                  Close
                </Button>
                {safeMailLink(selected.link) && (
                  <Button asChild>
                    <a href={safeMailLink(selected.link)} target="_blank" rel="noopener noreferrer">
                      Open message
                      <ExternalLink />
                    </a>
                  </Button>
                )}
              </div>
            </>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
