import type { Plugin } from "@opencode/plugin"
import type { LocationRef, OpenCodeEvent } from "@opencode/client"
import { debugFocusState as debug } from "./focus"
import { createNotifier } from "./notifier"
import type { Delivery, DeliveryHooks } from "./notifier"

type Client = {
  session: Pick<Plugin.Context["session"], "get" | "context">
  permission: Pick<Plugin.Context["permission"], "list">
}

// A receipt is captured synchronously, before TUI event handling waits on
// ownership, Job, inbox, or delivery APIs. It protects completion from a later
// execution.started event even if the earlier completion has not reached idle().
type EventReceipt = {
  startVersion: number
  startedEarly: boolean
  stoppedEarly?: boolean
  ignored?: boolean
}

// One bounded deadline for both server-side checks. Timeout rejects and the
// notifier retains its pending completion rather than guessing "finished".
export const OUTSTANDING_WORK_TIMEOUT_MS = 2500

async function boundedWorkQuery<T>(query: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      query(controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort()
          reject(new Error("OpenCode background-work query timed out"))
        }, OUTSTANDING_WORK_TIMEOUT_MS)
        timer.unref()
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

// These APIs are present on a V2 TUI OpenCodeClient (2.3.x), but not on the
// server's Plugin.Context. Keep them optional for older clients/hosts.
type WorkClient = {
  experimental?: {
    job?: {
      list(
        input: { sessionID: string; descendants: true },
        options?: { signal?: AbortSignal },
      ): Promise<readonly { kind: string; status: string }[]>
    }
  }
  session: {
    inbox?: {
      list(
        input: { sessionID: string },
        options?: { signal?: AbortSignal },
      ): Promise<readonly {
        type: string
        payload?: { metadata?: { source?: string } }
      }[]>
    }
  }
}

export function createV2Notifier(client: Client, location: LocationRef, delivery: Delivery, hooks: DeliveryHooks = {}) {
  const work = client as Client & WorkClient
  const jobs = work.experimental?.job
  const inbox = work.session?.inbox
  const notifier = createNotifier({
    async info(sessionID) {
      try {
        const session = await boundedWorkQuery(signal => client.session.get({ sessionID }, { signal }))
        return { isChild: !!session.parentID, title: session.title ?? null, agentName: session.parentID ? session.agent ?? null : null }
      } catch {
        return { isChild: null, title: null }
      }
    },
    async elapsed(sessionID, now) {
      try {
        const messages = await client.session.context({ sessionID })
        const times = messages.flatMap(message => message.type === "user" ? [message.time.created] : [])
        return times.length ? (now - Math.max(...times)) / 1000 : null
      } catch {
        return null
      }
    },
    async permissionPending(sessionID, permissionID) {
      if (!sessionID) return true
      try {
        return (await client.permission.list({ sessionID })).some(request => request.id === permissionID)
      } catch {
        return true
      }
    },
    waitForParentResume: true,
    ...(jobs && typeof jobs.list === "function" && inbox && typeof inbox.list === "function" ? {
      async hasOutstandingWork(sessionID: string) {
        // Job state survives missed session.created events and includes descendants.
        // A finished child can already have been removed from the Job registry
        // while its completion report is still queued for the parent.
        return boundedWorkQuery(async signal => {
          // The Job registry includes unconsumed terminal Subagent reports in
          // the durable outbox. Filtering to "running" misses the settlement
          // -> inbox-admission gap and causes premature parent completion.
          const records = await jobs.list({ sessionID, descendants: true }, { signal })
          if (records.some(job => job.status === "running" || job.kind === "subagent")) return true
          // Read the inbox AFTER the Job registry. OpenCode admits the synthetic
          // report before it consumes/deletes its outbox row. A sequential check
          // therefore avoids taking an empty pre-admission inbox snapshot next
          // to an empty post-acknowledgement Job snapshot.
          const queued = await inbox.list({ sessionID }, { signal })
          return queued.some(item =>
            item.type === "synthetic" && item.payload?.metadata?.source === "subagent"
          )
        })
      },
    } : {}),
  }, location.directory, delivery, hooks)
  const seen = new Set<string>()
  // null means ownership is unresolved, not that the session belongs here.
  // Completion must fail closed on unknown ownership; permission/question
  // prompts retain their historical fail-open behavior.
  const owned = new Map<string, Promise<boolean | null>>()
  // Versions are per-session and removed after terminal handling. A global
  // monotonically increasing token prevents accidental reuse across sessions.
  const startVersions = new Map<string, number>()
  let nextStartVersion = 0
  function advance(sessionID: string) {
    const version = ++nextStartVersion
    startVersions.delete(sessionID)
    startVersions.set(sessionID, version)
    // Protect long-running clients from unbounded state if many sessions
    // never emit a terminal event. Evicted sessions fail closed on a pending
    // completion rather than accepting an out-of-order stale completion.
    if (startVersions.size > 4096) {
      const oldest = startVersions.keys().next().value
      if (oldest !== undefined) startVersions.delete(oldest)
    }
    return version
  }
  function forget(sessionID: string, receipt?: EventReceipt) {
    if (receipt && startVersions.get(sessionID) === receipt.startVersion) startVersions.delete(sessionID)
  }
  let disposed = false

  function here(other: { directory: string; workspaceID?: string }) {
    return other.directory === location.directory && other.workspaceID === location.workspaceID
  }

  // Some V2 events carry only a sessionID, so ownership comes from the session itself.
  // Lookup failures fail open and are retried on the next event.
  function ours(sessionID: string) {
    let result = owned.get(sessionID)
    if (!result) {
      result = boundedWorkQuery(signal => client.session.get({ sessionID }, { signal })).then(
        session => !session.location || here(session.location),
        () => {
          debug(`v2 session ${sessionID}: ownership lookup unavailable`)
          owned.delete(sessionID)
          return null
        },
      )
      owned.set(sessionID, result)
      if (owned.size > 2048) {
        const oldest = owned.keys().next().value
        if (oldest !== undefined) owned.delete(oldest)
      }
    }
    return result
  }

  function first(key: string) {
    if (seen.has(key)) return false
    seen.add(key)
    // Event identities bound replay dedupe without retaining whole event payloads.
    if (seen.size > 2048) {
      const oldest = seen.values().next().value
      if (oldest !== undefined) seen.delete(oldest)
    }
    return true
  }

  return {
    connected: () => notifier.notify("client_connected"),
    // TUI uses this in the synchronous data.listen callback, before enqueuing
    // async processing. An early busy invalidates an in-flight completion and
    // drops pending alerts immediately; the version also rejects a succeeded
    // event that has not reached notifier.idle() yet.
    received(event: OpenCodeEvent): EventReceipt | undefined {
      if (disposed || (event.location && !here(event.location))) return
      if (event.type === "session.created" && event.data.parentID) {
        // OpenCode's durable session.created event carries its location in
        // data.location, not necessarily in the envelope. Check that location
        // before tracking the child synchronously; it can start while the
        // parent has an in-flight Job/inbox check.
        if (here(event.data.location) && first(`early-child:${event.data.sessionID}`)) {
          void notifier.created(event.data.sessionID, event.data.parentID, event.data.title ?? null)
        }
        return
      }
      // Enqueuing a new user prompt or subagent result means an older parent
      // completion cannot be trusted, even if execution.started is delayed.
      if (event.type === "session.inbox.enqueued" &&
          (event.data.item.type === "user" ||
           (event.data.item.type === "synthetic" &&
            event.data.item.payload.metadata?.source === "subagent"))) {
        if (!first(`receipt:${event.id}`))
          return { startVersion: startVersions.get(event.data.sessionID) ?? 0, startedEarly: false, ignored: true }
        const startVersion = advance(event.data.sessionID)
        notifier.stopped(event.data.sessionID)
        return { startVersion, startedEarly: false, stoppedEarly: true }
      }
      if (event.type !== "session.execution.started" &&
          event.type !== "session.execution.succeeded" &&
          event.type !== "session.execution.failed" &&
          event.type !== "session.execution.interrupted" &&
          event.type !== "session.deleted" &&
          event.type !== "session.moved") return
      const sessionID = event.data.sessionID
      if (!first(`receipt:${event.id}`))
        return { startVersion: startVersions.get(sessionID) ?? 0, startedEarly: false, ignored: true }
      if (event.type === "session.execution.started") {
        const startVersion = advance(sessionID)
        notifier.busy(sessionID)
        return { startVersion, startedEarly: true }
      }
      if (event.type !== "session.execution.succeeded") {
        // Failed, interrupted and deleted sessions must also invalidate an
        // older in-flight completion immediately, not after a queue backlog.
        const startVersion = advance(sessionID)
        notifier.stopped(sessionID)
        return { startVersion, startedEarly: false, stoppedEarly: true }
      }
      return { startVersion: startVersions.get(sessionID) ?? 0, startedEarly: false }
    },
    async event(event: OpenCodeEvent, receipt?: EventReceipt) {
      try {
        if (disposed || receipt?.ignored) return
        const sessionID = event.type === "form.created" ? event.data.form.sessionID : "sessionID" in event.data ? event.data.sessionID : undefined
        const cached = sessionID ? owned.get(sessionID) : undefined
        if (sessionID && (event.type === "session.moved" || event.type === "session.deleted")) owned.delete(sessionID)
        // A deleted session can no longer be looked up, so only a cached answer can drop it.
        // session.created carries authoritative location in its payload even
        // when there is no location envelope. Never override that with a
        // fail-open API lookup that may return the wrong workspace.
        const owner = event.location ? here(event.location)
          : event.type === "session.created" && event.data.location ? here(event.data.location)
          : !sessionID ? true
          : event.type === "session.deleted" ? !cached || await cached
          : await ours(sessionID)
        const mine = owner === null
          ? event.type !== "session.execution.succeeded"
          : owner
        if (!mine) {
          // A move can be delivered to both the former and new locations.
          // Its synchronous receipt already invalidated a pending completion
          // for the former owner; release its generation record here.
          if (event.type === "session.moved" && sessionID) forget(sessionID, receipt)
          debug(`v2 ${event.type}: session ${sessionID ?? "none"} belongs to another location, skipping`)
          return
        }
        switch (event.type) {
          case "session.created":
            if (first(`created:${event.data.sessionID}`)) {
              await notifier.created(event.data.sessionID, event.data.parentID ?? null, event.data.title ?? null, `created:${event.data.sessionID}`)
            }
            break
          case "session.execution.started":
            // The TUI applied busy() at receipt time; the server adapter has no
            // receipt and continues to process its events normally.
            if (!receipt?.startedEarly) notifier.busy(event.data.sessionID)
            break
          case "session.execution.succeeded":
            // A later start/stop may already be received while this completion
            // was waiting on its session queue or an API lookup.
            if (receipt && (startVersions.get(event.data.sessionID) ?? 0) !== receipt.startVersion) break
            if (first(`complete:${event.id}`)) await notifier.idle(event.data.sessionID, true, `complete:${event.id}`)
            forget(event.data.sessionID, receipt)
            break
          case "session.execution.failed":
            if (receipt && startVersions.get(event.data.sessionID) !== receipt.startVersion) break
            if (first(`failed:${event.id}`)) await notifier.failed(event.data.sessionID, "error", `failed:${event.id}`)
            forget(event.data.sessionID, receipt)
            break
          case "session.execution.interrupted":
            if (receipt && startVersions.get(event.data.sessionID) !== receipt.startVersion) break
            // Shutdown, supersession and inactivity are not user cancellations or failures.
            if (event.data.reason === "user" && first(`cancelled:${event.id}`)) {
              await notifier.failed(event.data.sessionID, "user_cancelled", `cancelled:${event.id}`)
            } else if (event.data.reason !== "user" && first(`stopped:${event.id}`)) {
              await notifier.stopped(event.data.sessionID)
            }
            forget(event.data.sessionID, receipt)
            break
          case "session.deleted":
            if (receipt && startVersions.get(event.data.sessionID) !== receipt.startVersion) break
            notifier.deleted(event.data.sessionID)
            forget(event.data.sessionID, receipt)
            break
          case "session.moved":
            // Moving a Session changes its owner and invalidates completion
            // generated at the previous location. The ownership cache was
            // cleared before the move's location check.
            forget(event.data.sessionID, receipt)
            break
          case "permission.asked":
            if (first(`permission:${event.data.id}`)) await notifier.permission(event.data.sessionID, event.data.id, false, `permission:${event.data.id}`)
            break
          case "session.inbox.enqueued":
            if (event.data.item.type === "user" && first(`message:${event.data.inboxID}`)) {
              await notifier.userMessage(event.data.sessionID, true, `message:${event.data.inboxID}`)
            }
            // Synthetic Subagent messages only invalidate stale completion.
            // They must never appear as a user-message notification.
            break
          case "form.created":
            if (event.data.form.metadata?.kind === "question" && first(`form:${event.data.form.id}`)) {
              await notifier.notify("question", event.data.form.sessionID, { key: `form:${event.data.form.id}` })
            }
            break
          // V2 has no plan_exit tool; changing agents is not a plan-ready signal.
        }
      } catch {
        // Notification failures must not interrupt the host or event stream.
      }
    },
    dispose() {
      disposed = true
      notifier.dispose()
      seen.clear()
      owned.clear()
      startVersions.clear()
    },
  }
}

export const setupV2: Plugin.Plugin["setup"] = (ctx) => {
  const notifier = createV2Notifier(ctx, ctx.location, "command")
  void notifier.connected().catch(() => undefined)
  const controller = new AbortController()
  // This subscription also serves headless clients. Terminal delivery lives in ./tui.
  const task = (async () => {
    try {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        await notifier.event(event)
      }
    } catch {
      // Aborting or losing a subscription must not cause an unhandled rejection.
    }
  })()
  return async () => {
    notifier.dispose()
    controller.abort()
    await task
  }
}
