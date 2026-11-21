import type { Message, MessageBus } from "@particle/dbus-next"

const DESTINATION = "org.freedesktop.Notifications"
const PATH = "/org/freedesktop/Notifications"
const CALL_TIMEOUT_MS = 2000
// Close the oldest popup before retiring its handler, so visible popups never
// lose their actions just because they passed the requested expiry time.
const MAX_ACTIONS = 128
const MAX_QUEUED_NOTIFICATIONS = 32
export const LINUX_NOTIFICATION_WAIT_MS = 10_000

interface BusStream {
  ref?: () => void
  unref?: () => void
  on?: (event: string, callback: () => void) => void
}

interface Connection {
  bus: MessageBus
  stream?: BusStream
  owner: string
  callbacks: Map<number, () => void>
  pendingSignals: Array<[string, number, unknown]> | null
  cancellation: AbortController
}

let connection: Promise<Connection> | undefined
let current: Connection | undefined
let generation = 0
let lastID = 0
let lastIDFromDBus = false
type QueueBlocker = { ready: boolean }
type QueuedNotification = { group?: string; queuedAt: number; blocker?: QueueBlocker; skip(): void; send(): Promise<void> }
type NotificationQueue = { pending: QueuedNotification[]; running: number; concurrency: number }
let queue: NotificationQueue = { pending: [], running: 0, concurrency: 1 }
const fallbackQueue: NotificationQueue = { pending: [], running: 0, concurrency: 4 }
const queueBlockers = new WeakMap<Promise<void>, QueueBlocker>()
class NotificationCallTimeout extends Error {}

export function getLinuxNotificationID(): number | null {
  return lastID || null
}

export function hasLinuxDBusReplacement(): boolean {
  return lastIDFromDBus && current !== undefined
}

export function rememberLinuxNotificationID(id: number): void {
  lastID = id
  lastIDFromDBus = false
  // A plain notify-send replacement has no action, so retire its old callback.
  current?.callbacks.delete(id)
}

function debug(message: string): void {
  if (process.env.OPENCODE_NOTIFIER_DEBUG) console.error(`[opencode-notifier] ${message}`)
}

async function call(state: Connection, message: Message): Promise<Message | null> {
  const signal = state.cancellation.signal
  signal.throwIfAborted()
  let timer: ReturnType<typeof setTimeout> | undefined
  let cancelled: (() => void) | undefined
  try {
    return await new Promise((resolve, reject) => {
      cancelled = () => reject(signal.reason)
      signal.addEventListener("abort", cancelled, { once: true })
      timer = setTimeout(() => {
        // dbus-next 0.11.4 exposes no cancellation API. Retire this serial
        // without disconnecting the sender that owns other visible actions.
        const pending = (state.bus as MessageBus & { _methodReturnHandlers?: Record<number, unknown> })._methodReturnHandlers
        reject(new NotificationCallTimeout("notification D-Bus call timed out"))
        if (pending && message.serial !== null) delete pending[message.serial]
        else if (current === state) resetConnection()
      }, CALL_TIMEOUT_MS)
      timer.unref()
      state.bus.call(message).then(resolve, reject)
    })
  } finally {
    clearTimeout(timer)
    if (cancelled) signal.removeEventListener("abort", cancelled)
  }
}

function resetConnection(): void {
  connection = undefined
  const old = current
  current = undefined
  lastID = 0
  lastIDFromDBus = false
  old?.callbacks.clear()
  old?.cancellation.abort(new Error("notification connection closed"))
  try { old?.bus.disconnect() } catch {}
}

export function disposeLinuxNotifications(): void {
  generation++
  for (const item of queue.pending.splice(0)) item.skip()
  for (const item of fallbackQueue.pending.splice(0)) item.skip()
  queue = { pending: [], running: 0, concurrency: 1 }
  resetConnection()
}

let requestOrder = 0
export function nextLinuxNotificationOrder(): number {
  return ++requestOrder
}

export function linuxDeliveryGuard(): () => boolean {
  const epoch = generation
  return () => epoch === generation
}

function drainQueue(state: NotificationQueue) {
  while (state.running < state.concurrency) {
    const now = performance.now()
    const ready = state.pending.findIndex(item => !item.blocker || item.blocker.ready || now - item.queuedAt > LINUX_NOTIFICATION_WAIT_MS)
    if (ready < 0) return
    const [item] = state.pending.splice(ready, 1)
    if (now - item.queuedAt > LINUX_NOTIFICATION_WAIT_MS) {
      item.skip()
      continue
    }
    state.running++
    void item.send().finally(() => {
      state.running--
      drainQueue(state)
    })
  }
}

function waitInQueue(waitFor: Promise<void>): QueueBlocker {
  const existing = queueBlockers.get(waitFor)
  if (existing) return existing
  const blocker = { ready: false }
  const resume = () => {
    blocker.ready = true
    drainQueue(queue)
    drainQueue(fallbackQueue)
  }
  queueBlockers.set(waitFor, blocker)
  void waitFor.then(resume, resume)
  return blocker
}

export function queueLinuxNotification<T>(send: () => Promise<T>, skipped: T, group?: string, fallback = false, queuedAt = performance.now(), waitFor?: Promise<void>): Promise<T> {
  return new Promise((resolve, reject) => {
    const state = fallback ? fallbackQueue : queue
    const existing = group === undefined ? -1 : state.pending.findIndex(item => item.group === group)
    if (existing !== -1) state.pending.splice(existing, 1)[0].skip()
    if (state.pending.length >= MAX_QUEUED_NOTIFICATIONS) state.pending.shift()?.skip()
    state.pending.push({ group, queuedAt, blocker: waitFor && waitInQueue(waitFor), skip: () => resolve(skipped), send: async () => {
      try { resolve(await send()) } catch (error) { reject(error) }
    } })
    drainQueue(state)
  })
}

function handleSignal(state: Connection, member: string, id: number, value: unknown): void {
  if (member === "NotificationClosed") state.callbacks.delete(id)
  if (member === "ActionInvoked" && value === "focus-terminal") {
    const callback = state.callbacks.get(id)
    state.callbacks.delete(id)
    try { callback?.() } catch {}
  }
}

function flushSignals(state: Connection) {
  const signals = state.pendingSignals
  state.pendingSignals = null
  for (const [member, id, value] of signals ?? []) handleSignal(state, member, id, value)
}

async function connect(): Promise<Connection> {
  const epoch = generation
  const dbus = (await import("@particle/dbus-next")).default
  if (epoch !== generation) throw new Error("notification channel disposed")
  const bus = dbus.sessionBus()
  const state: Connection = { bus, owner: "", callbacks: new Map(), pendingSignals: null, cancellation: new AbortController() }
  current = state
  bus.on("error", error => {
    debug(`notification D-Bus error: ${String(error)}`)
    if (current === state) resetConnection()
  })
  // dbus-next has no public unref API. Unref its socket so retained popups do
  // not keep a terminated OpenCode process alive; explicit disposal also closes it.
  const stream = (bus as MessageBus & { _connection?: { stream?: BusStream } })._connection?.stream
  state.stream = stream
  stream?.on?.("close", () => {
    if (current === state) resetConnection()
  })
  bus.on("message", message => {
    if (message.type !== dbus.MessageType.SIGNAL) return
    if (message.sender === "org.freedesktop.DBus" && message.interface === "org.freedesktop.DBus" && message.member === "NameOwnerChanged" && message.body[0] === DESTINATION) {
      // A restarted daemon has a new ID namespace and no surviving popups.
      state.owner = message.body[2]
      state.callbacks.clear()
      lastID = 0
      lastIDFromDBus = false
      return
    }
    if (message.sender !== state.owner || message.path !== PATH || message.interface !== DESTINATION) return
    const [id, value] = message.body
    if (state.pendingSignals) state.pendingSignals.push([message.member, id, value])
    else handleSignal(state, message.member, id, value)
  })
  // Subscribe before Notify, including signals emitted before its reply.
  for (const rule of [
    `type='signal',sender='${DESTINATION}',path='${PATH}',interface='${DESTINATION}'`,
    `type='signal',sender='org.freedesktop.DBus',interface='org.freedesktop.DBus',member='NameOwnerChanged',arg0='${DESTINATION}'`,
  ]) {
    await call(state, new dbus.Message({
      destination: "org.freedesktop.DBus", path: "/org/freedesktop/DBus", interface: "org.freedesktop.DBus",
      member: "AddMatch", signature: "s", body: [rule],
    }))
  }
  try {
    const reply = await call(state, new dbus.Message({
      destination: "org.freedesktop.DBus", path: "/org/freedesktop/DBus", interface: "org.freedesktop.DBus",
      member: "GetNameOwner", signature: "s", body: [DESTINATION],
    }))
    state.owner = reply?.body[0] ?? ""
  } catch (error) {
    // An installed but inactive daemon has no owner yet. Notify starts it, and
    // NameOwnerChanged reports its owner before the daemon can send signals.
    if ((error as { type?: string }).type !== "org.freedesktop.DBus.Error.NameHasNoOwner") throw error
  }
  if (current !== state || epoch !== generation) throw new Error("notification connection closed")
  return state
}

export async function sendLinuxDBusNotification(
  render: () => { title: string; message: string }, timeout: number, iconPath: string | undefined,
  grouping: boolean, onClick: (() => void) | undefined, label: string, isCurrent: () => boolean,
  permit?: () => Promise<boolean>,
): Promise<"shown" | "skipped" | "failed"> {
  if (!isCurrent()) return "skipped"
  let state: Connection | undefined
  let replacement = 0
  const pendingConnection = connection ??= connect()
  try {
    state = await pendingConnection
    if (!isCurrent()) return "skipped"
    if (current !== state) return "failed"
    // Pending calls keep the client alive until delivery resolves. Idle
    // notification listeners alone must not prolong the client's lifetime.
    state.stream?.ref?.()
    const dbus = (await import("@particle/dbus-next")).default
    if (!isCurrent()) return "skipped"
    if (current !== state) return "failed"
    if (permit && !(await permit())) return "skipped"
    if (!isCurrent()) return "skipped"
    if (current !== state) return "failed"
    const connection = state
    const makeRoom = async (replaces: number) => {
      if (!onClick || connection.callbacks.size < MAX_ACTIONS || connection.callbacks.has(replaces)) return true
      const id = connection.callbacks.keys().next().value!
      await call(connection, new dbus.Message({
        destination: DESTINATION, path: PATH, interface: DESTINATION,
        member: "CloseNotification", signature: "u", body: [id],
      }))
      connection.callbacks.delete(id)
      return !permit || !isCurrent() || current !== connection || await permit()
    }
    if (!(await makeRoom(grouping ? lastID : 0))) return "skipped"
    if (!isCurrent()) return "skipped"
    if (current !== state) return "failed"
    replacement = grouping ? lastID : 0
    state.pendingSignals = []
    const send = (replaces: number) => {
      const { title, message } = render()
      return call(connection, new dbus.Message({
        destination: DESTINATION, path: PATH, interface: DESTINATION,
        member: "Notify", signature: "susssasa{sv}i",
        body: ["opencode", replaces, iconPath ?? "", title, message,
          onClick ? ["focus-terminal", label] : [], {}, Math.max(1, timeout) * 1000],
      }))
    }
    let reply
    try {
      reply = await send(replacement)
    } catch (error) {
      if (!replacement || !(error instanceof dbus.DBusError) || error.type !== "org.freedesktop.DBus.Error.InvalidArgs" || error.text !== "Invalid notification ID") throw error
      if (current !== state || !isCurrent()) {
        if (current === state) flushSignals(state)
        return "skipped"
      }
      if (permit && !(await permit())) {
        if (current === state) flushSignals(state)
        return "skipped"
      }
      if (!isCurrent()) {
        if (current === state) flushSignals(state)
        return "skipped"
      }
      if (current !== state) return "failed"
      replacement = 0
      if (!(await makeRoom(0)) || !isCurrent()) {
        if (current === state) flushSignals(state)
        return "skipped"
      }
      if (current !== state) return "failed"
      reply = await send(replacement)
    }
    if (current !== state) return "skipped"
    if (reply?.sender) state.owner = reply.sender
    const id = reply?.body[0]
    if (!Number.isInteger(id) || id <= 0) throw new Error("invalid notification ID")
    if (grouping) {
      lastID = id
      lastIDFromDBus = true
    }
    if (onClick) state.callbacks.set(id, onClick)
    else state.callbacks.delete(id)
    flushSignals(state)
    return "shown"
  } catch (error) {
    const reply = (error as { name?: string; reply?: { sender?: string } } | undefined)
    // A timed-out replacement may already own this ID. Its buffered click
    // cannot safely use the previous notification's action.
    if (replacement && error instanceof NotificationCallTimeout) state?.callbacks.delete(replacement)
    if (state && current === state && state.owner && (error instanceof NotificationCallTimeout ||
      reply?.name === "DBusError" && reply.reply?.sender === state.owner)) flushSignals(state)
    else if (connection === pendingConnection) resetConnection()
    if (!isCurrent()) return "skipped"
    debug(`persistent notification failed: ${String(error)}`)
    return "failed"
  } finally {
    state?.stream?.unref?.()
  }
}
