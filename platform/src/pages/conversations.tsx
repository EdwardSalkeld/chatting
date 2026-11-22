import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { MessageSquare, Send } from 'lucide-react'
import { api } from '@/lib/api'
import {
  conversationMessages,
  type ChatMessage,
  type Conversation,
  type Submission,
} from '@/lib/conversations'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { ErrorStrip, usePageTitle } from '@/components/shared'
import { fmtWhen, encode } from '@/lib/format'
import '@/styles/conversations.css'

function saved(key: string) {
  try {
    return sessionStorage.getItem(key) || ''
  } catch {
    return ''
  }
}
function save(key: string, value: string) {
  try {
    if (value) sessionStorage.setItem(key, value)
    else sessionStorage.removeItem(key)
  } catch {
    /* Drafts still live in component state. */
  }
}
function savedAttempt(key: string): { text: string; id: string } | null {
  try {
    const value = JSON.parse(saved(key))
    return typeof value?.text === 'string' && /^[a-zA-Z0-9_-]{16,80}$/.test(value?.id)
      ? value
      : null
  } catch {
    return null
  }
}

const STATES: Record<string, string> = {
  queued: 'Queued',
  working: 'Working',
  answered: 'Answered',
  failed: 'Failed — no web answer recorded',
  delivered: 'Delivered',
}

export function Conversations() {
  const { id } = useParams()
  const conversations = useQuery({
    queryKey: ['conversations'],
    queryFn: () => api<{ conversations: Conversation[] }>('handler/conversations'),
    refetchInterval: 10000,
  })
  const [search, setSearch] = useState('')
  const selected = conversations.data?.conversations.find((c) => c.id === id)
  usePageTitle(selected?.title || 'Conversations')
  return (
    <div className="conversations">
      <aside className="conversation-list" aria-label="Conversations">
        <input
          aria-label="Find conversation"
          placeholder="Find a conversation…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <ErrorStrip query={conversations} label="Conversations" />
        {conversations.isPending && <p>Loading conversations…</p>}
        {conversations.data?.conversations
          .filter((c) => (c.title + c.target).toLowerCase().includes(search.toLowerCase()))
          .map((c) => (
            <Link
              key={c.id}
              to={`/conversations/${encode(c.id)}`}
              aria-current={c.id === id ? 'page' : undefined}
            >
              <MessageSquare size={16} />
              <span>
                {c.title}
                <small>
                  {c.target}
                  {c.topic_id ? ` · topic ${c.topic_id}` : ''}
                </small>
              </span>
            </Link>
          ))}
        {conversations.data?.conversations.length === 0 && (
          <p>No recorded Telegram conversations yet.</p>
        )}
      </aside>
      {selected ? (
        <ConversationThread key={selected.id} conversation={selected} />
      ) : (
        <section className="conversation-empty">
          <MessageSquare size={32} />
          <h2>
            {id && conversations.data ? 'Conversation not found' : 'Your conversations, here too'}
          </h2>
          <p>
            Choose a conversation to read its recorded history and talk to Billy through the web.
          </p>
          <p>Web messages and replies work independently of Telegram.</p>
        </section>
      )}
    </div>
  )
}

function ConversationThread({ conversation }: { conversation: Conversation }) {
  const client = useQueryClient()
  const path = `conversations/${encode(conversation.id)}/messages`
  const history = useInfiniteQuery({
    queryKey: ['conversation-history', conversation.id],
    initialPageParam: '',
    queryFn: ({ pageParam }) =>
      api<{ messages: ChatMessage[]; next_cursor: string | null }>(
        `worker/${path}${pageParam ? `?before=${encode(pageParam)}` : ''}`,
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
    refetchInterval: 3000,
  })
  const submissions = useQuery({
    queryKey: ['conversation-submissions', conversation.id],
    queryFn: () => api<{ messages: Submission[] }>(`handler/${path}`),
    refetchInterval: 3000,
  })
  const [drafts, setDrafts] = useState(() => saved('conversation-draft:' + conversation.id))
  const [notice, setNotice] = useState('')
  const attempt = useRef(savedAttempt('conversation-attempt:' + conversation.id))
  const scroll = useRef<HTMLDivElement>(null)
  const follow = useRef(true)
  const prependHeight = useRef<number | null>(null)
  const rows = conversationMessages(
    history.data?.pages.flatMap((p) => p.messages) || [],
    submissions.data?.messages || [],
  )
  useEffect(() => {
    save('conversation-draft:' + conversation.id, drafts)
  }, [drafts, conversation.id])
  useLayoutEffect(() => {
    const el = scroll.current
    if (!el) return
    if (prependHeight.current != null && !history.isFetchingNextPage) {
      el.scrollTop += el.scrollHeight - prependHeight.current
      prependHeight.current = null
    } else if (follow.current) el.scrollTop = el.scrollHeight
  }, [rows.length, history.isFetchingNextPage])
  const send = useMutation({
    mutationFn: async () => {
      if (!attempt.current || attempt.current.text !== drafts)
        attempt.current = { text: drafts, id: crypto.randomUUID().replaceAll('-', '') }
      save('conversation-attempt:' + conversation.id, JSON.stringify(attempt.current))
      return api<Submission>(`handler/${path}`, {
        method: 'POST',
        body: JSON.stringify({
          message: attempt.current.text,
          client_message_id: attempt.current.id,
        }),
      })
    },
    onSuccess: (submission) => {
      setDrafts('')
      attempt.current = null
      save('conversation-attempt:' + conversation.id, '')
      setNotice('Queued. Billy will reply here.')
      follow.current = true
      client.setQueryData<{ messages: Submission[] }>(
        ['conversation-submissions', conversation.id],
        (old) => ({
          messages: [
            ...(old?.messages || []).filter((m) => m.task_id !== submission.task_id),
            submission,
          ],
        }),
      )
      void client.invalidateQueries({ queryKey: ['conversation-submissions', conversation.id] })
      void client.invalidateQueries({ queryKey: ['conversation-history', conversation.id] })
    },
  })
  const sentStatus = rows.find(
    (row) => row.role === 'user' && row.task_id === send.data?.task_id,
  )?.status
  return (
    <section className="conversation-thread" aria-label={conversation.title}>
      <header>
        <div>
          <h2>{conversation.title}</h2>
          <p>Replies stay here. Nothing is automatically sent to Telegram.</p>
        </div>
        <Link to={`/items/${encode(conversation.work_item_id)}`}>Work item</Link>
      </header>
      <div
        className="conversation-history"
        ref={scroll}
        onScroll={() => {
          const el = scroll.current
          if (el) follow.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80
        }}
        aria-label="Message history"
        role="log"
        aria-live="polite"
        aria-relevant="additions text"
      >
        <p className="conversation-history-note">
          Recorded history only. Messages from before Chatting began recording may be missing.
        </p>
        <ErrorStrip query={history} label="History" />
        <ErrorStrip query={submissions} label="Queued messages" />
        {history.hasNextPage && (
          <Button
            variant="outline"
            disabled={history.isFetchingNextPage}
            onClick={() => {
              prependHeight.current = scroll.current?.scrollHeight || 0
              void history.fetchNextPage()
            }}
          >
            Load older messages
          </Button>
        )}
        {history.isPending && <p>Loading history…</p>}
        {!history.isPending && rows.length === 0 && (
          <p>No recorded messages yet. Send the first web message below.</p>
        )}
        {rows.map((message) => (
          <article key={message.id} className="conversation-message" data-role={message.role}>
            <div className="conversation-message-meta">
              <strong>{message.role === 'assistant' ? 'Billy' : message.sender || 'You'}</strong>
              <span>{message.transport === 'web' ? 'via web' : 'via Telegram'}</span>
              <time dateTime={message.occurred_at}>{fmtWhen(message.occurred_at)}</time>
            </div>
            {message.content && <div className="conversation-message-body">{message.content}</div>}
            {message.attachments?.map((attachment, i) => (
              <p className="conversation-attachment" key={i}>
                Attachment: {attachment.name || 'file'}
              </p>
            ))}
            {message.transport === 'web' && message.role === 'user' && (
              <p className="conversation-message-status" data-failed={message.status === 'failed'}>
                {STATES[message.status] || message.status}
                {message.task_id && (
                  <>
                    {' '}
                    · <Link to={`/tasks/${encode(message.task_id)}`}>View task</Link>
                  </>
                )}
              </p>
            )}
          </article>
        ))}
      </div>
      <form
        className="conversation-composer"
        onSubmit={(e) => {
          e.preventDefault()
          if (drafts.trim() && !send.isPending) {
            setNotice('')
            send.mutate()
          }
        }}
      >
        <label htmlFor="conversation-message">Message Billy via web</label>
        <Textarea
          id="conversation-message"
          value={drafts}
          disabled={send.isPending}
          onChange={(e) => {
            setDrafts(e.target.value)
            setNotice('')
            if (send.isError) send.reset()
          }}
          placeholder="Write a message…"
          maxLength={8000}
          rows={3}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
              e.preventDefault()
              e.currentTarget.form?.requestSubmit()
            }
          }}
        />
        {send.isError && (
          <p role="alert" className="conversation-send-error">
            Send could not be confirmed: {send.error.message}. Your draft is kept; retrying
            unchanged reuses the same message ID.
          </p>
        )}
        <div>
          <p role="status">
            {send.isPending
              ? 'Queuing…'
              : notice && sentStatus === 'answered'
                ? 'Reply received.'
                : notice || 'Text only · Ctrl/⌘ + Enter to send'}
          </p>
          <Button type="submit" disabled={!drafts.trim() || send.isPending}>
            <Send size={16} />
            {send.isError ? 'Retry send' : 'Send'}
          </Button>
        </div>
      </form>
    </section>
  )
}
