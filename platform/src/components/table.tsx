import {
  Fragment,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react'
import { useNavigate } from 'react-router-dom'
import { ChevronDown, ChevronRight, ChevronUp, ChevronsUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'
import { parseSort, sortRows, toggleSort, useUrlState } from '@/lib/url'
import { InlineEmpty, SkeletonRows } from '@/components/shared'
import { focusRow } from '@/lib/keys'

export interface Column<T> {
  key: string
  header: ReactNode
  cell: (row: T, index: number) => ReactNode
  /** Enables sorting on this column; the value rows are ordered by. */
  sortValue?: (row: T) => string | number | null | undefined
  /** First click sorts descending (dates, counts). */
  sortDescFirst?: boolean
  /** CSS width, e.g. 120 or '30%'. Columns without one share the rest. */
  width?: number | string
  /** Width below 760px, where fixed widths that fit a desktop table can overflow a phone. */
  narrowWidth?: number | string
  align?: 'left' | 'right'
  mono?: boolean
  /** 3 hides below 1100px, 2 hides below 760px. */
  priority?: 1 | 2 | 3
  className?: string
  /** Native tooltip for the cell, e.g. the full untruncated value. */
  title?: (row: T) => string | undefined
}

export interface DataTableProps<T> {
  columns: Column<T>[]
  rows: readonly T[] | undefined
  rowKey: (row: T) => string
  label: string
  /** Navigates on click / Enter; Cmd/Ctrl-click opens a new tab. */
  rowHref?: (row: T) => string | undefined
  onRowClick?: (row: T) => void
  loading?: boolean
  /** Shown as a single inline row when `rows` is empty. */
  empty?: ReactNode
  /** URL param holding the sort (default `sort`); pass `false` to disable URL sorting. */
  sortParam?: string | false
  defaultSort?: string
  selectedKey?: string | null
  rowTone?: (row: T) => string | undefined
  /** Rows whose version changes between polls flash briefly. */
  rowVersion?: (row: T) => string
  /** Below this width the wrapper scrolls horizontally instead of squeezing columns. */
  minWidth?: number
  /** Sticky offset for the header (default the 40px top bar). */
  stickyTop?: number
  className?: string
  skeletonRows?: number
  /** Rows kept above the sorted rows in their given order, e.g. running executors. */
  pinned?: (row: T) => boolean
  /**
   * Detail shown in a full-width row under `row`; rows returning nothing are not expandable.
   * Adds a chevron column; →/← and Space open and close the focused row.
   */
  expand?: (row: T) => ReactNode
  expandLabel?: string
  /** Accessible name for a clickable row, e.g. "Morning briefing, failed, 2h ago". */
  rowLabel?: (row: T) => string
}

const px = (value: number | string | undefined) =>
  typeof value === 'number' ? `${value}px` : value
function widthStyle(column: { width?: number | string; narrowWidth?: number | string }) {
  return {
    ...(column.width != null && { '--w': px(column.width) }),
    ...(column.narrowWidth != null && { '--w-narrow': px(column.narrowWidth) }),
  } as CSSProperties
}

const INTERACTIVE =
  'a, button, input, select, textarea, label, [role="button"], [data-no-row-click]'

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  label,
  rowHref,
  onRowClick,
  loading,
  empty = 'Nothing to show',
  sortParam = 'sort',
  defaultSort = '',
  selectedKey,
  rowTone,
  rowVersion,
  minWidth,
  stickyTop,
  className,
  skeletonRows = 6,
  pinned,
  expand,
  expandLabel = 'details',
  rowLabel,
}: DataTableProps<T>) {
  const navigate = useNavigate()
  const [urlSort, setUrlSort] = useUrlState(sortParam || '__nosort', defaultSort)
  const [localSort, setLocalSort] = useState(defaultSort)
  const sortValue = sortParam ? urlSort : localSort
  const sort = parseSort(sortValue)
  const [focused, setFocused] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set())

  const sorted = useMemo(() => {
    if (!rows) return undefined
    const column = sort && columns.find((c) => c.key === sort.key)
    const order = (list: readonly T[]) =>
      column?.sortValue ? sortRows(list, column.sortValue, sort!.desc) : list
    if (!pinned) return order(rows)
    return [...rows.filter(pinned), ...order(rows.filter((row) => !pinned(row)))]
  }, [rows, columns, sort?.key, sort?.desc, pinned])

  const wrap = useRef<HTMLDivElement>(null)
  const [scrolls, setScrolls] = useState(false)
  useLayoutEffect(() => {
    const el = wrap.current
    if (!el || !minWidth) return
    const measure = () => setScrolls(el.clientWidth < minWidth)
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(el)
    return () => observer.disconnect()
  }, [minWidth])

  const versions = useRef(new Map<string, string>())
  const flashing = new Set<string>()
  if (rowVersion && sorted) {
    for (const row of sorted) {
      const key = rowKey(row),
        version = rowVersion(row),
        previous = versions.current.get(key)
      if (previous !== undefined && previous !== version) flashing.add(key)
    }
  }
  useEffect(() => {
    if (!rowVersion || !sorted) return
    versions.current = new Map(sorted.map((row) => [rowKey(row), rowVersion(row)]))
  })

  const clickable = !!(rowHref || onRowClick)
  function openRow(row: T, newTab = false) {
    const href = rowHref?.(row)
    if (href) {
      if (newTab) window.open(href, '_blank', 'noopener')
      else navigate(href)
    } else onRowClick?.(row)
  }
  function toggle(key: string, open?: boolean) {
    setExpanded((current) => {
      const next = new Set(current)
      if (open ?? !next.has(key)) next.add(key)
      else next.delete(key)
      return next
    })
  }
  function setSort(column: Column<T>) {
    const next = toggleSort(sortValue, column.key, column.sortDescFirst)
    if (sortParam) setUrlSort(next, { replace: true })
    else setLocalSort(next)
  }

  const span = columns.length + (expand ? 1 : 0)
  return (
    <div ref={wrap} className={cn('table-wrap', scrolls && 'is-scroll')}>
      <table
        className={cn('dt', className)}
        aria-label={label}
        style={{
          minWidth: scrolls ? minWidth : undefined,
          ...(stickyTop != null && { ['--dt-top' as string]: `${stickyTop}px` }),
        }}
      >
        <thead>
          <tr>
            {expand && <th className="col-expand" aria-label={`Expand ${expandLabel}`} />}
            {columns.map((column) => {
              const active = sort?.key === column.key
              const ariaSort = column.sortValue
                ? active
                  ? sort.desc
                    ? 'descending'
                    : 'ascending'
                  : 'none'
                : undefined
              const Icon = !active ? ChevronsUpDown : sort.desc ? ChevronDown : ChevronUp
              return (
                <th
                  key={column.key}
                  scope="col"
                  style={widthStyle(column)}
                  data-align={column.align}
                  data-priority={column.priority}
                  aria-sort={ariaSort}
                  className={column.className}
                >
                  {column.sortValue ? (
                    <button type="button" onClick={() => setSort(column)}>
                      {column.align === 'right' && <Icon />}
                      <span className="truncate">{column.header}</span>
                      {column.align !== 'right' && <Icon />}
                    </button>
                  ) : (
                    column.header
                  )}
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody>
          {!sorted && loading ? (
            <SkeletonRows rows={skeletonRows} colSpan={span} />
          ) : !sorted?.length ? (
            <tr>
              <td className="inline-empty-cell" colSpan={span}>
                <InlineEmpty>{empty}</InlineEmpty>
              </td>
            </tr>
          ) : (
            sorted.map((row, index) => {
              const key = rowKey(row)
              const selected = selectedKey === key || (selectedKey == null && focused === key)
              const detail = expand?.(row)
              const open = detail != null && detail !== false && expanded.has(key)
              const canExpand = detail != null && detail !== false
              return (
                <Fragment key={key}>
                  <tr
                    data-row={clickable || undefined}
                    data-tone={rowTone?.(row)}
                    tabIndex={clickable ? 0 : undefined}
                    aria-label={clickable ? rowLabel?.(row) : undefined}
                    aria-keyshortcuts={clickable ? 'Enter' : undefined}
                    aria-selected={clickable ? selected : undefined}
                    aria-expanded={canExpand ? open : undefined}
                    className={cn(flashing.has(key) && 'is-flash')}
                    onFocus={(event) => {
                      if (event.target === event.currentTarget) setFocused(key)
                    }}
                    onClick={(event) => {
                      if (!clickable) return
                      const target = event.target as HTMLElement
                      if (
                        target.closest(INTERACTIVE) &&
                        target.closest('tr') === event.currentTarget
                      )
                        return
                      if (window.getSelection()?.toString()) return
                      openRow(row, event.metaKey || event.ctrlKey)
                    }}
                    onAuxClick={(event) => {
                      if (event.button === 1 && rowHref?.(row)) openRow(row, true)
                    }}
                    onKeyDown={(event) => {
                      if (event.target !== event.currentTarget) return
                      if (event.key === 'Enter' || event.key === 'o') {
                        event.preventDefault()
                        openRow(row, event.metaKey || event.ctrlKey)
                        return
                      }
                      if (
                        canExpand &&
                        (event.key === 'ArrowRight' ||
                          event.key === 'ArrowLeft' ||
                          event.key === ' ')
                      ) {
                        event.preventDefault()
                        toggle(key, event.key === ' ' ? undefined : event.key === 'ArrowRight')
                        return
                      }
                      const step =
                        event.key === 'ArrowDown' || event.key === 'j'
                          ? 1
                          : event.key === 'ArrowUp' || event.key === 'k'
                            ? -1
                            : 0
                      if (!step) return
                      event.preventDefault()
                      event.stopPropagation()
                      let sibling = (
                        step > 0
                          ? event.currentTarget.nextElementSibling
                          : event.currentTarget.previousElementSibling
                      ) as HTMLElement | null
                      while (sibling && !sibling.hasAttribute('data-row'))
                        sibling = (
                          step > 0 ? sibling.nextElementSibling : sibling.previousElementSibling
                        ) as HTMLElement | null
                      if (sibling) focusRow(sibling)
                    }}
                  >
                    {expand && (
                      <td className="col-expand">
                        {canExpand && (
                          <button
                            type="button"
                            className="expand-button"
                            aria-label={`${open ? 'Hide' : 'Show'} ${expandLabel}`}
                            aria-expanded={open}
                            onClick={() => toggle(key)}
                          >
                            {open ? <ChevronDown /> : <ChevronRight />}
                          </button>
                        )}
                      </td>
                    )}
                    {columns.map((column) => (
                      <td
                        key={column.key}
                        data-align={column.align}
                        data-priority={column.priority}
                        data-mono={column.mono || undefined}
                        className={column.className}
                        title={column.title?.(row)}
                      >
                        {column.cell(row, index)}
                      </td>
                    ))}
                  </tr>
                  {open && (
                    <tr className="expand-row">
                      <td colSpan={span}>{detail}</td>
                    </tr>
                  )}
                </Fragment>
              )
            })
          )}
        </tbody>
      </table>
    </div>
  )
}
