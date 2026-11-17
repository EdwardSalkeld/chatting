import { useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { ArrowLeft, ArrowRight, Clock3, Layers3, MessageSquare, Radio } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { api, useItems } from '@/lib/api'
import type { ItemSnapshot } from '@/lib/types'
import { dateTime, duration, encode, human, itemName, relative } from '@/lib/format'
import {
  Empty,
  ErrorNotice,
  Loading,
  PageHeading,
  Panel,
  SearchInput,
  StatusBadge,
} from '@/components/shared'

export function WorkItems() {
  const query = useItems()
  const [search, setSearch] = useState('')
  const [params, setParams] = useSearchParams()
  const filter = ['live', 'queued'].includes(params.get('filter') || '')
    ? params.get('filter')!
    : 'all'
  function setFilter(value: string) {
    setParams(value === 'all' ? {} : { filter: value })
  }
  const items = query.data?.items || [],
    active = query.data?.active_executors || []
  const visible = items.filter(
    (item) =>
      `${itemName(item)} ${item.work_item_id} ${item.reply?.type} ${item.reply?.target}`
        .toLowerCase()
        .includes(search.toLowerCase()) &&
      (filter === 'all' ||
        (filter === 'live'
          ? active.some((run) => run.work_item_id === item.work_item_id)
          : item.queued > 0)),
  )
  return (
    <>
      <PageHeading title="Work items" />
      <ErrorNotice
        error={query.error}
        retry={() => {
          void query.refetch()
        }}
      />
      <div className="list-toolbar">
        <div className="filter-tabs" aria-label="Work item filters">
          {[
            ['all', 'All items'],
            ['live', 'Running'],
            ['queued', 'Queued'],
          ].map(([value, label]) => (
            <button
              key={value}
              aria-pressed={filter === value}
              className={filter === value ? 'selected' : ''}
              onClick={() => setFilter(value)}
            >
              {label}
              {value === 'all' && <span>{items.length}</span>}
            </button>
          ))}
        </div>
        <SearchInput value={search} onChange={setSearch} placeholder="Search work items…" />
      </div>
      <Panel>
        {query.isPending ? (
          <Loading />
        ) : visible.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>WORK ITEM</th>
                  <th>STATUS</th>
                  <th>MODEL</th>
                  <th>RUNS</th>
                  <th>LAST RUN</th>
                  <th>
                    <span className="sr-only">Open item</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {visible.map((item) => {
                  const live = active.some((run) => run.work_item_id === item.work_item_id)
                  return (
                    <tr key={item.work_item_id}>
                      <td>
                        <Link className="table-title" to={`/items/${encode(item.work_item_id)}`}>
                          <div className="channel-icon">
                            <MessageSquare size={18} />
                          </div>
                          <div>
                            <strong>{itemName(item)}</strong>
                            <span>
                              {item.reply?.type || 'internal'}
                              {item.reply?.target ? ` · ${item.reply.target}` : ''}
                            </span>
                          </div>
                        </Link>
                      </td>
                      <td>
                        <StatusBadge status={live ? 'live' : item.queued ? 'queued' : 'idle'} />
                      </td>
                      <td>
                        <span className="model-label">{item.model_tier}</span>
                      </td>
                      <td>{item.run_count}</td>
                      <td>
                        <time title={dateTime(item.last_run_at)}>{relative(item.last_run_at)}</time>
                      </td>
                      <td>
                        <Link
                          className="icon-link"
                          aria-label={`Open ${itemName(item)}`}
                          to={`/items/${encode(item.work_item_id)}`}
                        >
                          <ArrowRight size={17} />
                        </Link>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty
            title={search || filter !== 'all' ? 'No matching work items' : 'No work items'}
            action={
              (search || filter !== 'all') && (
                <Button
                  variant="outline"
                  onClick={() => {
                    setSearch('')
                    setFilter('all')
                  }}
                >
                  Clear filters
                </Button>
              )
            }
          />
        )}
      </Panel>
    </>
  )
}
export function WorkItemDetail() {
  const { id = '' } = useParams()
  const query = useQuery({
    queryKey: ['item', id],
    queryFn: () => api<ItemSnapshot>(`worker/items/${encode(id)}/runs`),
    refetchInterval: 4000,
  })
  const [filter, setFilter] = useState('all')
  const data = query.data
  if (query.isPending) return <Loading />
  if (!data)
    return (
      <>
        <Button asChild variant="ghost">
          <Link to="/items">
            <ArrowLeft />
            Work items
          </Link>
        </Button>
        <ErrorNotice
          error={query.error}
          retry={() => {
            void query.refetch()
          }}
        />
      </>
    )
  const runs = data.runs.filter(
    (run) =>
      filter === 'all' ||
      (filter === 'success' ? run.result_status === 'success' : run.result_status !== 'success'),
  )
  return (
    <>
      <Link to="/items" className="back-link">
        <ArrowLeft size={15} />
        All work items
      </Link>
      <PageHeading title={itemName(data.item)} description={id} />
      <ErrorNotice
        error={query.error}
        retry={() => {
          void query.refetch()
        }}
      />
      <div className="detail-facts">
        <div>
          <Layers3 size={17} />
          <strong>{data.item.run_count}</strong> runs
        </div>
        <div>
          <Clock3 size={17} />
          <strong>{data.item.queued}</strong> queued
        </div>
        <div>
          Model <strong>{data.item.model_tier}</strong>
        </div>
        <div>
          Explicit finish <strong>{data.item.explicit_finish ? 'On' : 'Off'}</strong>
        </div>
      </div>
      {!!data.active_executors.length && (
        <Panel title="Running now">
          <div className="live-list">
            {data.active_executors.map((run) => (
              <Link className="live-card" key={run.task_id} to={`/tasks/${encode(run.task_id)}`}>
                <div className="live-symbol">
                  <Radio size={20} />
                </div>
                <div className="live-copy">
                  <StatusBadge status="live" />
                  <h3>{run.preview || 'Task in progress'}</h3>
                  <p>
                    Started {dateTime(run.started_at)} · Attempt {run.attempt}
                  </p>
                </div>
                <ArrowRight size={18} />
              </Link>
            ))}
          </div>
        </Panel>
      )}
      <div className="section-toolbar">
        <h2>Run history</h2>
        <div className="filter-tabs">
          {[
            ['all', 'All runs'],
            ['success', 'Successful'],
            ['attention', 'Needs attention'],
          ].map(([value, label]) => (
            <button
              key={value}
              aria-pressed={filter === value}
              className={filter === value ? 'selected' : ''}
              onClick={() => setFilter(value)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      <Panel>
        {runs.length ? (
          <div className="run-list">
            {runs.map((run) => (
              <Link className="run-row" key={run.run_id} to={`/runs/${encode(run.run_id)}`}>
                <div className="run-main">
                  <StatusBadge status={run.result_status} />
                  <h3>{run.preview || run.task_id || run.run_id}</h3>
                  <p>
                    {dateTime(run.created_at)} · {human(run.source)} · {duration(run.latency_ms)}
                  </p>
                </div>
                <ArrowRight size={18} />
              </Link>
            ))}
          </div>
        ) : (
          <Empty title="No runs" />
        )}
      </Panel>
    </>
  )
}
