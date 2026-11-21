import type { Plugin } from "@opencode/plugin"
import { createHash } from "node:crypto"
import type { LocationRef, OpenCodeEvent } from "@opencode/client"
import { loadConfig } from "./config"
import { debugFocusState as debug } from "./focus"
import { createNotifier, OutstandingWorkError } from "./notifier"
import type { Delivery, DeliveryHooks, SessionInfo } from "./notifier"
import { createBackgroundObserver } from "./v2-background"
import type { IdleBackground } from "./v2-background"
import { InboxStoreUnavailable, managedServiceReadback, nativeActivity, readLegacyInbox, standaloneFamilyReader } from "./v2-service-readback"
import type { ManagedServiceReadback } from "./v2-service-readback"
import type { InboxFamily, InboxMetadata, InboxReport, StoredInboxFamily } from "./v2-inbox-worker.js"
import { nextLinuxNotificationOrder } from "./notify-linux"

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
  completionSequence?: number
  workVersion?: number
  ownership?: object
  blocked?: boolean
  ignored?: boolean
  formOrder?: number
  popupOrder?: number
}
type InboxAcknowledgement = Extract<OpenCodeEvent, { type: "session.inbox.delivered" | "session.inbox.cancelled" }>

// One bounded deadline for all server-side checks. Timeout rejects and the
// notifier retains its pending completion rather than guessing "finished".
export const OUTSTANDING_WORK_TIMEOUT_MS = 2500

async function boundedQuery<T>(query: (signal: AbortSignal) => Promise<T>, lifetime?: AbortSignal): Promise<T> {
  const controller = new AbortController()
  const signal = lifetime ? AbortSignal.any([controller.signal, lifetime]) : controller.signal
  signal.throwIfAborted()
  let cancelled!: () => void
  const aborted = new Promise<never>((_, reject) => {
    cancelled = () => reject(signal.reason)
    signal.addEventListener("abort", cancelled, { once: true })
  })
  const timer = setTimeout(() => controller.abort(new OutstandingWorkError("query timed out")), OUTSTANDING_WORK_TIMEOUT_MS)
  timer.unref()
  try {
    return await Promise.race([query(signal), aborted])
  } finally {
    clearTimeout(timer)
    signal.removeEventListener("abort", cancelled)
    controller.abort()
  }
}

// Filter out token deltas and other events unrelated to notification delivery.
export function notificationEvent(event: OpenCodeEvent) {
  switch (event.type) {
    case "session.created": case "session.deleted": case "session.moved":
    case "session.execution.started": case "session.execution.succeeded":
    case "session.execution.failed": case "session.execution.interrupted":
    case "session.inbox.enqueued": case "session.inbox.delivered": case "session.inbox.cancelled":
    case "permission.asked": case "permission.replied": case "form.created": case "form.replied": case "form.cancelled":
      return true
    default:
      return false
  }
}

function reportKind(item: { type: string; payload?: unknown }): InboxReport {
  if (item.type !== "synthetic") return 0
  const payload = item.payload as { metadata?: { source?: unknown } } | undefined
  switch (payload?.metadata?.source) {
    case "subagent": return 1
    case "shell": case "codemode": case "experimental_image_generate": return 2
    default: return 0
  }
}

function isOutstandingReport(item: InboxMetadata, root: string) {
  return item.report === 1 || (item.report === 2 && item.sessionID !== root)
}

type FamilySession = {
  id: string
  parentID?: string | null
  time?: { busy?: number; idle?: number }
}

// The TUI can read active executions, Jobs and inboxes. The server plugin can
// list sessions and wait for an execution to settle. Older hosts vary.
type WorkClient = {
  experimental?: {
    job?: {
      list(
        input: { sessionID: string; descendants: true },
        options?: { signal?: AbortSignal },
      ): Promise<readonly { sessionID?: string; kind: string; status: string }[]>
    }
  }
  session: {
    active?(options?: { signal?: AbortSignal }): Promise<Readonly<Record<string, unknown>>>
    list?(
      input: { parentID: string; cursor?: string; limit: number },
      options?: { signal?: AbortSignal },
    ): Promise<{
      data: readonly FamilySession[]
      cursor?: { next?: string | null }
    }>
    wait?(input: { sessionID: string }, options?: { signal?: AbortSignal }): Promise<unknown>
    inbox?: { list(input: { sessionID: string }, options: { signal: AbortSignal; headers: Record<string, string> }):
      Promise<readonly { id?: string; type: string; payload?: unknown }[]> }
  }
}

const SESSION_BATCH_SIZE = 24
// Leave two of the eight shared native readers available to other families.
const LEGACY_INBOX_FAMILY_BATCH_SIZE = 6
const PENDING_INBOX_LIMIT = 4096
const NATIVE_WAIT_LIMIT = 128
let activeNativeWaits = 0

async function activeSessionIDs(session: WorkClient["session"], signal: AbortSignal) {
  return session.active ? Object.keys(nativeActivity(await session.active({ signal }))) : []
}

export const inboxRpc = {
  id: "opencode-notifier",
  methods: {
    inboxFamily: {
      input: { type: "object", properties: {
        sessionID: { type: "string" },
        workspaceID: { type: "string" },
        completedAt: { type: "number" },
      }, required: ["sessionID"] },
      output: { type: "object" },
    },
  },
  events: {},
} as const

type FamilyRequest = { sessionID: string; workspaceID?: string; completedAt?: number }
type ReadInboxFamily = (input: FamilyRequest, signal: AbortSignal) => Promise<InboxFamily | undefined>

function activityDigest(id: string) {
  return createHash("sha256").update(id).digest("hex")
}

function storedDirectory(path: string) {
  return process.platform === "win32" ? path.replaceAll("\\", "/") : path
}

type StoreReader = (sessionID: string, options?: { signal?: AbortSignal }) => Promise<StoredInboxFamily>
function scopedReader(read: StoreReader, location: LocationRef): ReadInboxFamily {
  return async ({ sessionID }, signal) => {
    const family = await read(sessionID, { signal })
    const root = family.sessions.find(entry => entry.id === sessionID)
    if (!root || storedDirectory(root.directory) !== storedDirectory(location.directory) || root.workspaceID !== (location.workspaceID ?? null))
      throw new OutstandingWorkError("session belongs to a different location")
    return family
  }
}

async function scanSessionFamily(
  session: WorkClient["session"],
  root: string,
  deadline: AbortSignal,
  hasOutstanding: (entry: FamilySession, signal: AbortSignal) => Promise<boolean>,
  batchSize = SESSION_BATCH_SIZE,
) {
  const controller = new AbortController()
  const signal = AbortSignal.any([deadline, controller.signal])
  const sessions = new Set([root])
  const pending: FamilySession[] = [{ id: root }]
  let foundOutstanding!: (result: null) => void
  const outstanding = new Promise<null>(resolve => { foundOutstanding = resolve })
  for (let offset = 0; offset < pending.length;) {
    const batch = pending.slice(offset, offset + batchSize)
    offset += batch.length
    const reads = Promise.all(batch.map(async entry => {
      signal.throwIfAborted()
      if (await hasOutstanding(entry, signal)) {
        foundOutstanding(null)
        controller.abort()
        return
      }
      if (!session.list) return

      let cursor: string | undefined
      const cursors = new Set<string>()
      do {
        signal.throwIfAborted()
        const page = await session.list({ parentID: entry.id, limit: 100, ...(cursor ? { cursor } : {}) }, { signal })
        for (const child of page.data) {
          if (child.parentID !== entry.id || sessions.has(child.id)) continue
          sessions.add(child.id)
          pending.push(child)
        }
        cursor = page.cursor?.next ?? undefined
        if (cursor && cursors.has(cursor)) throw new Error("OpenCode session list repeated a cursor")
        if (cursor) cursors.add(cursor)
      } while (cursor)
    }))
    if (await Promise.race([reads, outstanding]) === null) return null
  }
  return sessions
}

async function hasOutstandingLegacyInbox(
  session: Client["session"] & WorkClient["session"], root: string, initial: string[], signal: AbortSignal,
  observed: {
    visit(id: string): void; changed(id: string): boolean; knownChildren(): boolean; runningChild(ids: string[]): boolean
    inboxItem(sessionID: string, inboxID: string, report: InboxReport): void
  }, completedAt?: number,
) {
  const inbox = session.inbox
  if (!inbox) throw new OutstandingWorkError("server inbox metadata is unavailable")
  const active = new Set(initial)
  const family = await scanSessionFamily(session, root, signal, async ({ id }, scanSignal) => {
    const changed = observed.changed(id)
    observed.visit(id)
    if (changed || (id !== root && active.has(id))) return true
    const items = await readLegacyInbox(signal => inbox.list({ sessionID: id }, {
      signal, headers: { "X-OpenCode-Capabilities": "experimental-media-uri" },
    }), scanSignal)
    const invalid = () => new OutstandingWorkError("invalid native inbox metadata")
    if (!Array.isArray(items)) throw invalid()
    let outstanding = false
    for (const item of items) {
      if (!item || typeof item.id !== "string" || !item.id || !["user", "synthetic", "compaction", "move"].includes(item.type)) throw invalid()
      if (item.type === "synthetic" && (!item.payload || typeof item.payload !== "object" || Array.isArray(item.payload))) throw invalid()
      const report = reportKind(item)
      observed.inboxItem(id, item.id, report)
      outstanding ||= isOutstandingReport({ sessionID: id, id: item.id, report }, root)
    }
    return outstanding
  }, LEGACY_INBOX_FAMILY_BATCH_SIZE)
  if (!family) return true
  if (family.size === 1 && !observed.knownChildren()) return observed.changed(root)
  const current = await activeSessionIDs(session, signal)
  if (observed.runningChild(current) || current.some(id => id !== root && family.has(id))) return true
  if (completedAt !== undefined) {
    const ids = [...family].filter(id => id !== root)
    for (let offset = 0; offset < ids.length; offset += SESSION_BATCH_SIZE) {
      signal.throwIfAborted()
      const children = await Promise.all(ids.slice(offset, offset + SESSION_BATCH_SIZE).map(sessionID => session.get({ sessionID }, { signal })))
      if (children.some(child => (child.time?.idle ?? -Infinity) >= completedAt)) return true
    }
  }
  return [...family].some(observed.changed)
}

async function hasOutstandingServerWork(
  session: Client["session"] & WorkClient["session"],
  root: string,
  signal: AbortSignal,
  completedAt: number | undefined,
  waitForIdle: ((sessionID: string, signal: AbortSignal) => Promise<unknown>) | undefined,
  observe: (sessionID: string) => boolean,
  changed: (sessionID: string) => boolean,
  active?: ReadonlySet<string>,
) {
  const sessions = await scanSessionFamily(session, root, signal, async ({ id, time }, signal) => {
    if (id === root) return false
    if (observe(id) || active?.has(id)) return true
    if (completedAt === undefined) return false
    const idle = time?.idle ?? -Infinity
    if (idle >= completedAt) return true
    if (!waitForIdle || time?.busy === undefined || time.busy < idle) return false

    // Shutdown can leave an old busy timestamp. Wait on the live execution
    // within the caller's deadline and check for settlement after the root.
    await waitForIdle(id, signal)
    signal.throwIfAborted()
    const child = await session.get({ sessionID: id }, { signal })
    return (child.time?.idle ?? -Infinity) >= completedAt
  })
  if (!sessions) return true
  if (active && sessions.size > 1) {
    const current = await activeSessionIDs(session, signal)
    if (current.some(id => id !== root && sessions.has(id))) return true
  }
  return [...sessions].some(changed)
}

export function createV2Notifier(
  client: Client, location: LocationRef, delivery: Delivery, hooks: DeliveryHooks = {},
  background?: ReturnType<typeof createBackgroundObserver>, managed?: ManagedServiceReadback,
  remoteFamily?: ReadInboxFamily,
) {
  const work = client as Client & WorkClient
  const session = managed ? { ...work.session, active: managed.active } : work.session
  const liveOnly = managed ? undefined : background
  const jobs = managed ? { list: managed.jobs } : work.experimental?.job
  const hasNativeInbox = typeof session?.inbox?.list === "function"
  const readFamily = managed ? scopedReader(managed.family, location) : remoteFamily
  const lifetime = new AbortController()
  const wait = typeof work.session?.wait === "function" ? work.session.wait.bind(work.session) : undefined
  type WaitResult = { ok: true } | { ok: false; error: unknown }
  type NativeWait = { controller: AbortController; subscribers: Set<(result: WaitResult) => void> }
  const serverWaits = new Map<string, NativeWait>()
  const waitForIdle = wait ? (sessionID: string, signal: AbortSignal) => {
    signal.throwIfAborted()
    // Some native Promise adapters ignore request options. Share one wait
    // and detach each caller as soon as its own check is cancelled.
    let running = serverWaits.get(sessionID)
    if (!running) {
      if (activeNativeWaits >= NATIVE_WAIT_LIMIT) throw new OutstandingWorkError("native wait capacity reached")
      const pending: NativeWait = { controller: new AbortController(), subscribers: new Set() }
      running = pending
      serverWaits.set(sessionID, pending)
      activeNativeWaits++
      const finish = (result: WaitResult) => {
        activeNativeWaits--
        if (serverWaits.get(sessionID) === pending) serverWaits.delete(sessionID)
        for (const subscriber of pending.subscribers) subscriber(result)
      }
      void Promise.resolve().then(() => {
        const signal = AbortSignal.any([lifetime.signal, pending.controller.signal])
        signal.throwIfAborted()
        return wait({ sessionID }, { signal })
      }).then(
        () => finish({ ok: true }),
        error => finish({ ok: false, error }),
      )
    }
    const pending = running
    return new Promise<void>((resolve, reject) => {
      function remove() {
        pending.subscribers.delete(settled)
        signal.removeEventListener("abort", cancelled)
      }
      function settled(result: WaitResult) {
        remove()
        if (result.ok) resolve()
        else reject(result.error)
      }
      function cancelled() {
        remove()
        if (!pending.subscribers.size) pending.controller.abort()
        reject(signal.reason)
      }
      pending.subscribers.add(settled)
      signal.addEventListener("abort", cancelled, { once: true })
    })
  } : undefined
  const inboxItems = new Map<string, { sessionID: string; report: InboxReport; sequence?: number }>()
  const inboxAcknowledgements = new Map<string, InboxAcknowledgement>()
  const deletedInboxOwners = new Map<string, true>()
  const closedForms = new Map<string, { created: number; order: number }>()
  let formOrder = 0
  const closedPermissions = new Map<string, true>()
  const familyWork = new Map<string, { root?: string; changedVersion?: number; descendantVersion?: number; foreignStartVersion?: number }>()
  const externalRoots = new Map<string, string>()
  const sessionKinds = new Map<string, boolean | null>()
  const liveFallback = new Map<string, true>()
  function rememberWork<T>(map: Map<string, T>, key: string, value: T) {
    map.delete(key)
    map.set(key, value)
    if (map.size > PENDING_INBOX_LIMIT) map.delete(map.keys().next().value!)
  }
  function observeInboxItem(sessionID: string, inboxID: string, report: InboxReport, sequence?: number) {
    const earlier = inboxItems.get(inboxID)
    rememberWork(inboxItems, inboxID, { sessionID, report, sequence: sequence ?? (earlier?.sessionID === sessionID ? earlier.sequence : undefined) })
  }
  function recordFamilyChange(sessionID: string, version: number, descendantOnly = false) {
    const field = descendantOnly ? "descendantVersion" : "changedVersion"
    rememberWork(familyWork, sessionID, { ...familyWork.get(sessionID), [field]: version })
  }
  function readbackRoots(sessionID: string) {
    const local = familyWork.get(sessionID)?.root
    const external = externalRoots.size ? externalRoots.get(activityDigest(sessionID)) : undefined
    return [...new Set([local, external])].filter((root): root is string => !!root && root !== sessionID)
  }
  function invalidateReadbackRoot(sessionID: string, defer?: boolean) {
    if (!(defer ?? loadConfig().deferCompleteUntilChildrenIdle)) return
    for (const root of readbackRoots(sessionID)) {
      advance(root)
      notifier.invalidate(root)
    }
  }
  function rememberFamilyChild(root: string, id: string) {
    rememberWork(familyWork, id, { ...familyWork.get(id), root })
  }
  function changedSince(root: string, id: string, version: number | undefined) {
    const work = familyWork.get(id)
    return version !== undefined && ((work?.changedVersion ?? 0) > version || (id !== root &&
      Math.max(work?.descendantVersion ?? 0, work?.foreignStartVersion ?? 0) > version))
  }
  async function sessionInfo(sessionID: string): Promise<SessionInfo> {
    try {
      const session = await boundedQuery(signal => client.session.get({ sessionID }, { signal }))
      rememberWork(sessionKinds, sessionID, !!session.parentID)
      return { isChild: !!session.parentID, title: session.title ?? null, agentName: session.parentID ? session.agent ?? null : null }
    } catch {
      if (!sessionKinds.has(sessionID)) rememberWork(sessionKinds, sessionID, null)
      return { isChild: null, title: null }
    }
  }
  const liveWait = background?.reportsDirectChildren ? waitForIdle : undefined
  const canListFamily = typeof work.session?.list === "function"
  async function liveFamilyWork(root: string, completedAt: number | undefined, workVersion: number | undefined, signal: AbortSignal, active?: string[]) {
    if (background?.outstanding(root)) return true
    if (!canListFamily || (!active && (!liveWait || completedAt === undefined))) return false
    return hasOutstandingServerWork(session, root, signal, completedAt, liveWait, id => {
      const changed = changedSince(root, id, workVersion)
      rememberFamilyChild(root, id)
      return changed
    }, id => changedSince(root, id, workVersion), active && new Set(active))
  }
  const notifier = createNotifier({
    info: sessionInfo,
    async elapsed(sessionID, now) {
      try {
        const messages = await boundedQuery(signal => client.session.context({ sessionID }, { signal }))
        const times = messages.flatMap(message => message.type === "user" && message.time.created <= now ? [message.time.created] : [])
        return times.length ? (now - Math.max(...times)) / 1000 : null
      } catch {
        return null
      }
    },
    async permissionPending(sessionID, permissionID, lifetime) {
      if (!sessionID) return true
      try {
        return (await boundedQuery(signal => client.permission.list({ sessionID }, { signal }), lifetime)).some(request => request.id === permissionID)
      } catch {
        return true
      }
    },
    waitForParentResume: true,
    ...(liveOnly ? {
      hasObservedOutstandingWork: liveOnly.outstanding,
      ...(readFamily || (canListFamily && liveWait) ? {
        hasOutstandingWork: (sessionID: string, completedAt?: number, workVersion?: number, check?: AbortSignal) => boundedQuery(async signal => {
          if (await liveFamilyWork(sessionID, completedAt, workVersion, signal)) return true
          try {
            const family = await readFamily?.({ sessionID, completedAt }, signal)
            if (!family) return false
            for (const child of family.sessions) rememberFamilyChild(sessionID, child.id)
            for (const item of family.items) observeInboxItem(item.sessionID, item.id, item.report)
            return family.items.some(item => isOutstandingReport(item, sessionID)) || family.sessions.some(child => changedSince(sessionID, child.id, workVersion))
          } catch (error) {
            if (error instanceof InboxStoreUnavailable) return false
            throw error
          }
        }, check),
      } : {}),
    } : jobs && typeof jobs.list === "function" && (readFamily || hasNativeInbox) ? {
      authoritativeChildWork: typeof session?.active === "function" && (!!readFamily || typeof session.list === "function"),
      hasObservedOutstandingWork: id => liveFallback.has(id) && !!background?.outstanding(id),
      async hasOutstandingWork(sessionID: string, completedAt?: number, workVersion?: number, check?: AbortSignal, placed?: boolean): Promise<boolean> {
        // Job state survives missed session.created events and includes descendants.
        // A finished child can already have been removed from the Job registry
        // while its completion report is still queued for the parent.
        return boundedQuery(async signal => {
          // Direct child executions can be active without a background Job.
          // Capture activity before reading the outbox and inboxes it feeds.
          const activeIDs = await activeSessionIDs(session, signal)
          // The Job registry includes unconsumed terminal Subagent reports in
          // the durable outbox. Filtering to "running" misses the settlement
          // -> inbox-admission gap and causes premature parent completion.
          const records = await jobs.list({ sessionID, descendants: true }, { signal })
          if (records.some(job => job.kind === "subagent" || (!!job.sessionID && job.sessionID !== sessionID) ||
              (!managed && job.status === "running"))) return true
          const observed = {
            visit: (id: string) => rememberFamilyChild(sessionID, id),
            changed: (id: string) => changedSince(sessionID, id, workVersion),
            inboxItem: observeInboxItem,
            knownChildren: () => {
              if (notifier.hasTrackedChildren(sessionID)) return true
              for (const [id, work] of familyWork) if (id !== sessionID && work.root === sessionID) return true
              return false
            },
            runningChild: (active: Iterable<string>) => {
              const ids = new Set(active)
              ids.delete(sessionID)
              return [...ids].some(id => familyWork.get(id)?.root === sessionID) || notifier.hasRunningChildren(sessionID, ids)
            },
          }
          if (observed.runningChild(activeIDs)) return true
          // OpenCode admits reports before removing their outbox entries.
          // Compare the two live snapshots with one metadata query per read.
          let family: InboxFamily | undefined
          try {
            family = await readFamily?.({ sessionID, completedAt }, signal)
            liveFallback.delete(sessionID)
          } catch (error) {
            if (!(error instanceof InboxStoreUnavailable) || !managed || !background) throw error
            const root = await work.session.get({ sessionID }, { signal })
            const placement = root.location as LocationRef | undefined
            // The Promise API redacts workspaceID. Native execution events retain
            // it, and a later move or deletion cancels the in-flight check.
            if (!placement || placement.directory !== location.directory ||
                (!here(placement) && !(placed && placement.workspaceID === undefined)))
              throw new OutstandingWorkError("session belongs to a different location")
            rememberWork(liveFallback, sessionID, true)
            return liveFamilyWork(sessionID, completedAt, workVersion, signal, activeIDs)
          }
          if (!family) return hasOutstandingLegacyInbox(session, sessionID, activeIDs, signal, observed, completedAt)
          const outstanding = ({ sessions, items, externalActivity, externalWork }: InboxFamily, running: string[]) => {
              const changed = sessions.some(child => observed.changed(child.id))
              for (const child of sessions) observed.visit(child.id)
              for (const item of items) observeInboxItem(item.sessionID, item.id, item.report)
              const ids = new Set(running)
              const external = new Set(externalActivity)
              for (const digest of external) rememberWork(externalRoots, digest, sessionID)
              const externalRunning = external.size > 0 && running.some(id => external.has(activityDigest(id)))
              const externalChanged = external.size > 0 && [...familyWork.keys()].some(id =>
                changedSince(sessionID, id, workVersion) && external.has(activityDigest(id)))
              return externalRunning || externalChanged || externalWork || changed || items.some(item => isOutstandingReport(item, sessionID)) || sessions.some(child =>
                child.id !== sessionID && (ids.has(child.id) ||
                  (completedAt !== undefined && child.idle !== null && child.idle >= completedAt)))
          }
          if (outstanding(family, activeIDs)) return true
          if (family.sessions.length === 1 && !family.externalActivity?.length && !observed.knownChildren()) return false
          const current = await activeSessionIDs(session, signal)
          if (observed.runningChild(current)) return true
          const latest = await readFamily?.({ sessionID, completedAt }, signal)
          if (!latest) return hasOutstandingLegacyInbox(session, sessionID, current, signal, observed, completedAt)
          return outstanding(latest, current)
        }, check)
      },
    } : {}),
  }, location.directory, delivery, { ...hooks, dispatchIndependent: delivery === "terminal" })
  const seen = new Set<string>()
  // null means ownership is unresolved, not that the session belongs here.
  // Completion must fail closed on unknown ownership; permission/question
  // prompts retain their historical fail-open behavior.
  const owned = new Map<string, boolean | Promise<boolean | null>>()
  // Reuse changes the execution generation, but only moves and deletion
  // invalidate delivery of an already completed turn in this location.
  const ownership = new Map<string, object>()
  function ownershipToken(id: string) {
    const token = ownership.get(id) ?? {}
    rememberWork(ownership, id, token)
    return token
  }
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
  // Timed-out/unavailable ownership is unknown (null) and retried on the next
  // event: completion fails closed, but actionable permission/question alerts
  // retain their historical fail-open behavior.
  function rememberOwner(sessionID: string, owner: boolean | Promise<boolean | null>) {
    owned.set(sessionID, owner)
    if (owned.size > 2048) {
      const oldest = owned.keys().next().value
      if (oldest !== undefined) owned.delete(oldest)
    }
  }

  function ours(sessionID: string) {
    let result = owned.get(sessionID)
    if (result === undefined) {
      result = boundedQuery(signal => client.session.get({ sessionID }, { signal })).then(
        session => {
          rememberWork(sessionKinds, sessionID, !!session.parentID)
          return !session.location || here(session.location)
        },
        () => {
          debug(`v2 session ${sessionID}: ownership lookup unavailable`)
          if (owned.get(sessionID) === result) owned.delete(sessionID)
          return null
        },
      )
      rememberOwner(sessionID, result)
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

  async function notifyFailure(sessionID: string, event: "error" | "user_cancelled", key: string, receipt?: EventReceipt) {
    if (!first(key)) return
    if (receipt && startVersions.get(sessionID) !== receipt.startVersion) {
      // The event still happened, but its old terminal state must not replace
      // the newer execution's state.
      await notifier.notify(event, sessionID, { key })
    } else {
      await notifier.failed(sessionID, event, key)
    }
  }

  function observeBackground(event: OpenCodeEvent, defer?: boolean) {
    const cancelledReportOwner = background?.event(event)
    if (cancelledReportOwner && (liveOnly || liveFallback.has(cancelledReportOwner)) && (defer ?? loadConfig().deferCompleteUntilChildrenIdle)) {
      advance(cancelledReportOwner)
      notifier.invalidate(cancelledReportOwner)
    }
  }

  function observeWorkReceipt(sessionID: string, report: InboxReport, local: boolean) {
    const descendantOnly = report === 2
    const version = descendantOnly ? ++nextStartVersion : advance(sessionID)
    recordFamilyChange(sessionID, version, descendantOnly)
    if (local && !descendantOnly) notifier.invalidate(sessionID)
    invalidateReadbackRoot(sessionID, true)
    notifier.invalidateAncestors(sessionID, true)
    return version
  }

  const queues = new Map<string, Promise<void>>()
  const adapter = {
    connected: () => notifier.notify("client_connected"),
    received(event: OpenCodeEvent, defer?: boolean): EventReceipt | undefined {
      if (disposed) return
      if (event.type === "session.inbox.delivered" || event.type === "session.inbox.cancelled") {
        const { sessionID, inboxID } = event.data
        const item = inboxItems.get(inboxID)
        if ((item && item.sessionID !== sessionID) || !first(`receipt:${event.id}`)) return
        inboxItems.delete(inboxID)
        rememberWork(inboxAcknowledgements, `${sessionID}\0${inboxID}`, event)
        // Delivery can remove a report before the older success reads the
        // inbox. Delivery may start processing an unidentified item. With
        // native readback an unidentified cancellation can also remove a
        // cold report before the first snapshot can classify it.
        const unknown = event.type === "session.inbox.delivered" || !!(readFamily || hasNativeInbox)
        const report = item?.report ?? (unknown ? 1 : 0)
        if (report && (defer ?? loadConfig().deferCompleteUntilChildrenIdle)) observeWorkReceipt(sessionID, report, true)
        return
      }
      const foreign = (event.type !== "session.moved" && event.location && !here(event.location)) ||
        (event.type === "session.execution.started" && !event.location && owned.get(event.data.sessionID) === false)
      if (foreign) {
        // Record foreign starts before ancestry is known. A later family
        // snapshot can discover the descendant after the activity snapshot.
        if (event.type === "session.execution.started" && (defer ?? loadConfig().deferCompleteUntilChildrenIdle) && first(`receipt:${event.id}`)) {
          const id = event.data.sessionID
          rememberWork(familyWork, id, { ...familyWork.get(id), foreignStartVersion: ++nextStartVersion })
          invalidateReadbackRoot(id, true)
          notifier.invalidateAncestors(id, true)
        } else if (event.type === "session.inbox.enqueued") {
          const report = reportKind(event.data.item)
          if (report && (defer ?? loadConfig().deferCompleteUntilChildrenIdle) && first(`receipt:${event.id}`))
            observeWorkReceipt(event.data.sessionID, report, false)
        }
        return
      }
      if (event.type === "permission.asked" || event.type === "form.created") {
        const id = event.type === "form.created" ? event.data.form.sessionID : event.data.sessionID
        return { startVersion: 0, startedEarly: false, ownership: ownershipToken(id),
          formOrder: event.type === "form.created" ? ++formOrder : undefined, popupOrder: nextLinuxNotificationOrder() }
      }
      if (event.type === "session.created" && event.data.parentID) {
        // OpenCode's durable session.created event carries its location in
        // data.location, not necessarily in the envelope. Check that location
        // before tracking the child synchronously; it can start while the
        // parent has an in-flight Job/inbox check.
        const placement = event.data.location ?? event.location
        if (placement && here(placement) && first(`early-child:${event.data.sessionID}`)) {
          notifier.track(event.data.sessionID, event.data.parentID)
        }
        return
      }
      // Enqueuing a new user prompt or subagent result means an older parent
      // completion cannot be trusted, even if execution.started is delayed.
      if (event.type === "session.inbox.enqueued") {
        const report = reportKind(event.data.item)
        if ((event.data.item.type !== "user" && !report) || !(defer ?? loadConfig().deferCompleteUntilChildrenIdle)) return
        if (!first(`receipt:${event.id}`))
          return { startVersion: startVersions.get(event.data.sessionID) ?? 0, startedEarly: false, ignored: true }
        if (report) return { startVersion: observeWorkReceipt(event.data.sessionID, report, true), startedEarly: false }
        const startVersion = advance(event.data.sessionID)
        notifier.invalidate(event.data.sessionID)
        return { startVersion, startedEarly: false }
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
      if (event.type === "session.moved") {
        ownership.delete(sessionID)
        sessionKinds.delete(sessionID)
        const mine = here(event.data.location)
        rememberOwner(sessionID, mine)
        if (!mine) {
          notifier.movedAway(sessionID)
          background?.movedAway(sessionID)
        } else {
          notifier.invalidate(sessionID)
        }
        return { startVersion: advance(sessionID), startedEarly: false }
      }
      if (event.type === "session.execution.started") {
        const startVersion = advance(sessionID)
        notifier.busy(sessionID)
        recordFamilyChange(sessionID, startVersion)
        invalidateReadbackRoot(sessionID)
        return { startVersion, startedEarly: true }
      }
      if (event.type === "session.execution.succeeded") {
        const { sequence, runningChildren } = notifier.settled(sessionID)
        const blocked = runningChildren || (!!liveOnly && loadConfig().deferCompleteUntilChildrenIdle && liveOnly.outstanding(sessionID))
        // Inbox delivery that arrives later must not retroactively make an old
        // success eligible for notification.
        return { startVersion: advance(sessionID), startedEarly: false, completionSequence: sequence,
          blocked, workVersion: nextStartVersion, ownership: ownershipToken(sessionID) }
      }
      // Failed, interrupted, moved and deleted sessions invalidate completions
      // before the asynchronous queue reaches them.
      const startVersion = advance(sessionID)
      if (event.type === "session.deleted") {
        ownership.delete(sessionID)
        sessionKinds.delete(sessionID)
        notifier.deleted(sessionID)
      }
      else notifier.stopped(sessionID)
      return { startVersion, startedEarly: false }
    },
    enqueue(event: OpenCodeEvent, defer?: boolean) {
      if (disposed) return
      if (event.type === "permission.replied") {
        rememberWork(closedPermissions, `${event.data.sessionID}\0${event.data.requestID}`, true)
        return
      }
      if (event.type === "form.replied" || event.type === "form.cancelled") {
        const key = `${event.data.sessionID}\0${event.data.id}`
        const created = event.created ?? Date.now()
        if (first(`form-close:${event.id}`) && (closedForms.get(key)?.created ?? -Infinity) <= created)
          rememberWork(closedForms, key, { created, order: ++formOrder })
        return
      }
      if (event.type === "session.deleted") rememberWork(deletedInboxOwners, event.data.sessionID, true)
      const placement = event.type === "session.created" ? event.data.location ?? event.location : event.location
      const sessionID = event.type === "form.created" ? event.data.form.sessionID : "sessionID" in event.data ? event.data.sessionID : undefined
      const foreign = !!placement && !here(placement)
      const acknowledged = event.type === "session.inbox.delivered" || event.type === "session.inbox.cancelled"
      if (acknowledged) {
        const item = inboxItems.get(event.data.inboxID)
        if (item && (item.sessionID !== event.data.sessionID || (item.sequence !== undefined && event.durable?.seq !== undefined &&
            event.durable.seq <= item.sequence))) return
      }
      let receiptOnly = acknowledged
      let foreignReport = false
      let receivedAfterAcknowledgement = false
      if (event.type === "session.inbox.enqueued") {
        const { sessionID, inboxID, item } = event.data
        if (deletedInboxOwners.has(sessionID)) return
        const report = reportKind(item)
        const child = item.type === "synthetic" ? item.payload.metadata?.childID : undefined
        foreignReport = report === 1 && !!background && (background.knowsChild(sessionID) ||
          (typeof child === "string" && background.knowsChild(child, sessionID)))
        const identity = `${sessionID}\0${inboxID}`
        let acknowledgement = inboxAcknowledgements.get(identity)
        if (acknowledgement?.durable?.seq !== undefined && event.durable?.seq !== undefined && event.durable.seq > acknowledgement.durable.seq) {
          inboxAcknowledgements.delete(identity)
          acknowledgement = undefined
        }
        receiptOnly = foreign || item.type !== "user"
        if (acknowledgement) {
          // Different location streams can forward an acknowledgement first.
          // Join its report to a live Job and retire both in the same turn.
          if (background && report === 1 && (!foreign || foreignReport) && first(`background:${event.id}`)) {
            observeBackground(event, defer)
            observeBackground(acknowledgement, defer)
          }
          if (receiptOnly) return
          receivedAfterAcknowledgement = true
        } else observeInboxItem(sessionID, inboxID, report, event.durable?.seq)
      }
      if (!placement && sessionID && owned.get(sessionID) === false && event.type !== "session.moved" &&
          event.type !== "session.deleted" && !receiptOnly) {
        if (event.type === "session.execution.started") adapter.received(event, defer)
        return
      }
      const removedInbox = event.type === "session.deleted" && !!background?.knowsChild(event.data.sessionID)
      if (background?.accepts(event) && (!foreign || acknowledged || foreignReport || removedInbox) && first(`background:${event.id}`)) {
        observeBackground(event, defer)
      }
      if (!notificationEvent(event)) return
      const receipt = receivedAfterAcknowledgement ? undefined : adapter.received(event, defer)
      if (receipt?.ignored) return
      if (receiptOnly) return
      const key = sessionID ?? ""
      const task = (queues.get(key) ?? Promise.resolve()).then(() => adapter.event(event, receipt)).finally(() => {
        if (queues.get(key) === task) queues.delete(key)
      })
      queues.set(key, task)
    },
    async event(event: OpenCodeEvent, receipt?: EventReceipt) {
      try {
        if (disposed || receipt?.ignored) return
        const sessionID = event.type === "form.created" ? event.data.form.sessionID : "sessionID" in event.data ? event.data.sessionID : undefined
        const questionOrder = event.type === "form.created" ? receipt?.formOrder ?? ++formOrder : 0
        const popupOrder = event.type === "form.created" || event.type === "permission.asked"
          ? receipt?.popupOrder ?? nextLinuxNotificationOrder() : undefined
        const promptToken = sessionID && (event.type === "permission.asked" || event.type === "form.created")
          ? receipt?.ownership ?? ownershipToken(sessionID) : undefined
        const promptOwner = () => !promptToken || (sessionID !== undefined && ownership.get(sessionID) === promptToken)
        const cached = sessionID ? owned.get(sessionID) : undefined
        if (sessionID && (event.type === "session.deleted" || (event.type === "session.moved" && !receipt))) owned.delete(sessionID)
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
              const { sessionID, parentID, title } = event.data
              rememberWork(sessionKinds, sessionID, !!parentID)
              if (parentID) notifier.track(sessionID, parentID)
              else await notifier.created(sessionID, null, title ?? null, `created:${sessionID}`)
            }
            break
          case "session.execution.started":
            if (!receipt?.startedEarly) notifier.busy(event.data.sessionID)
            break
          case "session.execution.succeeded": {
            const id = event.data.sessionID
            const key = `complete:${event.id}`
            if (!first(key)) break
            const token = receipt?.ownership ?? ownershipToken(id)
            const superseded = () => !!receipt && (startVersions.get(id) !== receipt.startVersion ||
              (receipt.completionSequence !== undefined && !notifier.completionCurrent(id, receipt.completionSequence)))
            const tracked = notifier.isTrackedChild(id)
            if (tracked && !notifier.delivers("subagent_complete")) {
              if (!receipt) notifier.settled(id)
              forget(id, receipt)
              break
            }
            const remembered = sessionKinds.get(id)
            if (remembered === false && !tracked &&
                loadConfig().deferCompleteUntilChildrenIdle && superseded()) break
            const info = await sessionInfo(id)
            const child = info.isChild || notifier.isTrackedChild(id)
            if (ownership.get(id) !== token) break
            if (child || (info.isChild === false && !loadConfig().deferCompleteUntilChildrenIdle)) {
              // Completion remains a real event after a new execution starts.
              if (!receipt) notifier.settled(id)
              await notifier.notify(child ? "subagent_complete" : "complete", id, {
                title: info.title, key, now: event.created, valid: () => ownership.get(id) === token,
              })
              forget(id, receipt)
              break
            }
            // A later start/stop may already be received while this completion
            // was waiting on its session queue or an API lookup.
            if (superseded()) break
            if (!receipt) notifier.settled(id)
            await notifier.idle(id, true, key, receipt?.blocked, event.created, receipt?.workVersion,
              { info, placed: !!event.location })
            forget(id, receipt)
            break
          }
          case "session.execution.failed":
            await notifyFailure(event.data.sessionID, "error", `failed:${event.id}`, receipt)
            forget(event.data.sessionID, receipt)
            break
          case "session.execution.interrupted":
            // Shutdown, supersession and inactivity are not user cancellations or failures.
            if (event.data.reason === "user") {
              await notifyFailure(event.data.sessionID, "user_cancelled", `cancelled:${event.id}`, receipt)
            } else if (!receipt && first(`stopped:${event.id}`)) {
              notifier.stopped(event.data.sessionID)
            }
            forget(event.data.sessionID, receipt)
            break
          case "session.deleted":
            if (receipt && startVersions.get(event.data.sessionID) !== receipt.startVersion) break
            if (!receipt) {
              ownership.delete(event.data.sessionID)
              notifier.deleted(event.data.sessionID)
            }
            forget(event.data.sessionID, receipt)
            break
          case "session.moved":
            // Moving a Session changes its owner and invalidates completion
            // generated at the previous location. The ownership cache was
            // cleared before the move's location check.
            forget(event.data.sessionID, receipt)
            break
          case "permission.asked":
            if (first(`permission:${event.data.id}`)) {
              const { id, sessionID } = event.data
              await notifier.permission(sessionID, id, false, `permission:${id}`, () => promptOwner() &&
                (delivery !== "terminal" || !closedPermissions.has(`${sessionID}\0${id}`)), popupOrder)
            }
            break
          case "session.inbox.enqueued":
            if (event.data.item.type === "user" && first(`message:${event.data.inboxID}`)) {
              await notifier.userMessage(event.data.sessionID, true, `message:${event.data.inboxID}`)
            }
            // Synthetic Subagent messages only invalidate stale completion.
            // They must never appear as a user-message notification.
            break
          case "form.created":
            if (event.data.form.metadata?.kind === "question" && first(`form:${event.id}`)) {
              const { id, sessionID } = event.data.form
              const created = event.created ?? Date.now()
              await notifier.notify("question", sessionID, { key: `form:${event.id}`, order: popupOrder, valid: () => {
                if (!promptOwner()) return false
                if (delivery !== "terminal") return true
                const closure = closedForms.get(`${sessionID}\0${id}`)
                return !closure || closure.created < created || (closure.created === created && closure.order < questionOrder)
              } })
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
      lifetime.abort()
      notifier.dispose()
      seen.clear()
      owned.clear()
      ownership.clear()
      sessionKinds.clear()
      liveFallback.clear()
      serverWaits.clear()
      inboxItems.clear()
      inboxAcknowledgements.clear()
      deletedInboxOwners.clear()
      closedForms.clear()
      closedPermissions.clear()
      familyWork.clear()
      externalRoots.clear()
      startVersions.clear()
      queues.clear()
    },
  }
  return adapter
}

type IdleHook = (name: "experimental.idle", observe: (event: { sessionID: string; background: readonly IdleBackground[] }) => void) => Promise<{ dispose(): Promise<void> }>
const serverEventObservers = new Set<(event: OpenCodeEvent, defer?: boolean) => void>()

export const setupV2: Plugin.Plugin["setup"] = async (ctx) => {
  const version = ctx.app?.version?.match(/^2\.(\d+)\.\d+$/)
  const reportsDirectChildren = !!version && Number(version[1]) >= 3
  const background = createBackgroundObserver({ reportsDirectChildren })
  const controller = new AbortController()
  const standalone = standaloneFamilyReader(ctx.app)
  const ready = managedServiceReadback(ctx.app).then(managed => {
    const notifier = createV2Notifier(ctx, ctx.location, "command", {}, background, managed,
      !managed && standalone ? scopedReader(standalone, ctx.location) : undefined)
    serverEventObservers.add(notifier.enqueue)
    void notifier.connected().catch(() => undefined)
    return { notifier, managed }
  })
  // Pull from the host before readback setup finishes. The native event
  // stream retains subsequent events while its first event awaits the adapter.
  const task = (async () => {
    try {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        const { notifier } = await ready
        if (event.type === "session.inbox.cancelled" || event.type === "session.inbox.delivered" || event.type === "session.inbox.enqueued" ||
            event.type === "session.deleted" || event.type === "session.execution.started") {
          // A linked child can start in another location after the parent's
          // work snapshot. Native deletion also removes pending inbox rows.
          const receipt = event.location ? event : { ...event, location: ctx.location }
          const defer = event.type === "session.deleted" ? undefined : loadConfig().deferCompleteUntilChildrenIdle
          for (const receive of serverEventObservers) try { receive(receipt, defer) } catch { /* Keep the subscription open. */ }
        } else try { notifier.enqueue(event) } catch { /* Keep the subscription open. */ }
      }
    } catch {
      // Aborting or losing a subscription must not cause an unhandled rejection.
    }
  })()
  const { notifier, managed } = await ready
  const read = managed?.family ?? standalone
  const rpc = read && ctx.rpc?.register ? await ctx.rpc.register(inboxRpc, {
    async inboxFamily(input, { signal }) {
      const { sessionID, workspaceID, completedAt } = input as FamilyRequest
      const directory = storedDirectory(ctx.location.directory)
      if (ctx.location.workspaceID !== undefined && workspaceID !== undefined && workspaceID !== ctx.location.workspaceID)
        throw new OutstandingWorkError("requested workspace differs from the plugin location")
      const family = await read(sessionID, { signal }).catch(error => {
        if (!(error instanceof InboxStoreUnavailable)) throw error
      })
      if (!family) return { availability: "unsupported-store" }
      const { sessions, items } = family
      const root = sessions.find(session => session.id === sessionID)
      if (!root || storedDirectory(root.directory) !== directory) throw new OutstandingWorkError("session belongs to a different location")
      if (workspaceID !== undefined && root.workspaceID !== workspaceID)
        throw new OutstandingWorkError("requested workspace differs from the session location")
      const workspace = ctx.location.workspaceID ?? null
      if (root.workspaceID !== workspace) {
        if (workspace === null) return { availability: "unsupported-location" }
        throw new OutstandingWorkError("session belongs to a different location")
      }
      const visible = sessions.filter(session => storedDirectory(session.directory) === directory && session.workspaceID === workspace)
      const ids = new Set(visible.map(session => session.id))
      if (!ids.has(sessionID)) throw new OutstandingWorkError("session belongs to a different location")
      const external = sessions.filter(session => !ids.has(session.id))
      const externalReport = items.some(item => isOutstandingReport(item, sessionID) && !ids.has(item.sessionID))
      const externalSettlement = completedAt !== undefined && external.some(({ idle }) => idle !== null && idle >= completedAt)
      return {
        sessions: visible.map(({ id, idle }) => ({ id, idle })),
        items: items.filter(item => ids.has(item.sessionID)),
        // The terminal matches live activity without receiving foreign session IDs.
        externalActivity: external.map(session => activityDigest(session.id)),
        externalWork: externalReport || externalSettlement,
      } satisfies InboxFamily
    },
  }).catch(() => undefined) : undefined
  let registration: Awaited<ReturnType<IdleHook>> | undefined
  try {
    // The pinned SDK predates this hook. Older hosts still supply the lifecycle
    // events, while current hosts can also report already-running subagents.
    const session = ctx.session as (Client["session"] & { hook?: IdleHook }) | undefined
    if (background) registration = await session?.hook?.("experimental.idle", event => {
      try {
        if (!controller.signal.aborted && Array.isArray(event.background)) background.idle(event.sessionID, event.background)
      } catch { /* The observer must not interrupt model execution. */ }
    })
  } catch {
    debug("v2 experimental.idle hook unavailable")
  }
  return async () => {
    serverEventObservers.delete(notifier.enqueue)
    notifier.dispose()
    controller.abort()
    await rpc?.dispose().catch(() => undefined)
    await registration?.dispose().catch(() => undefined)
    await task
  }
}
