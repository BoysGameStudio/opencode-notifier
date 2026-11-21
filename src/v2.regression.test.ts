import { afterAll, test, expect } from "bun:test"
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync } from "fs"
import { join } from "path"
import { tmpdir } from "os"
import { createHash } from "node:crypto"
import { createV2Notifier, setupV2, OUTSTANDING_WORK_TIMEOUT_MS } from "./v2"
import { createBackgroundObserver } from "./v2-background"
import type { Delivery } from "./notifier"
import type { ManagedServiceReadback } from "./v2-service-readback"
import { InboxStoreUnavailable } from "./v2-service-readback"
import type { OpenCodeEvent, SessionMessageUser } from "@opencode/client"
import { createDeliveryClaimer } from "./claim"

const fixture = mkdtempSync(join(tmpdir(), "notifier-v2-test-"))
const configPath = join(fixture, "config.json")
const commandLog = join(fixture, "commands.txt")
const commandScript = join(fixture, "record-command.mjs")
writeFileSync(commandScript, 'import { appendFileSync } from "node:fs"; appendFileSync(process.argv[2], process.argv[3]+"\\n")')
// Share on-disk claims across simulated terminals inside an isolated temp root,
// so these tests never read or write a running session's markers.
const claim = createDeliveryClaimer(fixture)
afterAll(() => {
  rmSync(fixture, { recursive: true, force: true })
})
let eventID = 0

function event(type: string, data: object): OpenCodeEvent {
  // The fixtures supply only fields the notifier reads; real event types remain checked in v2.ts.
  return { id: `event-${++eventID}`, type, data } as OpenCodeEvent
}

test("OpenCode 2.3 direct child answers block completion before their report enters the inbox", () => {
  let now = 0
  const background = createBackgroundObserver({ reportsDirectChildren: true, now: () => now, uncertaintyTimeout: () => 100 })
  const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
  const childTurn = (id: string, modelRan: boolean) => {
    emit("created", id, { parentID: "parent" })
    emit("execution.started", id)
    if (modelRan) {
      emit("step.started", id, { assistantMessageID: "assistant" })
      emit("step.ended", id, { assistantMessageID: "assistant", finish: "stop" })
    }
    emit("execution.succeeded", id)
  }
  childTurn("control-only", false)
  childTurn("ses_aut_fixture_1", true)
  expect(background.outstanding("parent")).toBe(false)
  childTurn("direct", true)
  expect(background.outstanding("parent")).toBe(true)
  const inboxID = "direct-report"
  emit("inbox.enqueued", "parent", { inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "direct" } } } })
  expect(background.outstanding("parent")).toBe(true)
  emit("inbox.delivered", "parent", { inboxID })
  expect(background.outstanding("parent")).toBe(false)

  childTurn("missing-report", true)
  expect(background.outstanding("parent")).toBe(true)
  now = 101
  expect(background.outstanding("parent")).toBe(false)
})

for (const previous of ["unexpired", "expired", "late", "queued"] as const) {
  test(`a ${previous} direct result does not consume the reused child's new result`, () => {
    let now = 0
    const background = createBackgroundObserver({ reportsDirectChildren: true, now: () => now, uncertaintyTimeout: () => 100 })
    const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
    const enqueue = (inboxID: string) => emit("inbox.enqueued", "parent", {
      inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } },
    })

    emit("created", "child", { parentID: "parent" })
    emit("execution.started", "child")
    emit("step.started", "child", { assistantMessageID: "first" })
    emit("step.ended", "child", { assistantMessageID: "first", finish: "stop" })
    emit("execution.succeeded", "child")
    expect(background.outstanding("parent")).toBe(true)
    now = previous === "unexpired" ? 99 : 101
    expect(background.outstanding("parent")).toBe(previous === "unexpired")

    const tool = { id: "reuse", assistantMessageID: "parent-message", executed: true }
    emit("tool.input.started", "parent", { ...tool, name: "subagent" })
    emit("tool.called", "parent", tool)
    emit("execution.started", "child")
    if (previous === "late" || previous === "queued") {
      enqueue("old-result")
      expect(background.outstanding("parent")).toBe(true)
      if (previous === "late") emit("inbox.delivered", "parent", { inboxID: "old-result" })
    }
    emit("execution.succeeded", "child")
    enqueue("new-result")
    emit("tool.success", "parent", { ...tool, metadata: { status: "running", sessionID: "child" } })
    expect(background.outstanding("parent")).toBe(true)
    emit("inbox.delivered", "parent", { inboxID: "new-result" })
    if (previous === "queued") {
      expect(background.outstanding("parent")).toBe(true)
      emit("inbox.delivered", "parent", { inboxID: "old-result" })
    }
    expect(background.outstanding("parent")).toBe(false)
  })
}

test("deleting a child preserves its result already queued in a live parent's inbox", () => {
  const background = createBackgroundObserver()
  const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
  emit("created", "child", { parentID: "parent" })
  emit("execution.started", "child")
  background.idle("parent", [{ kind: "subagent", sessionID: "parent", childSessionID: "child" }])
  emit("execution.succeeded", "child")
  const inboxID = "queued-before-deletion"
  emit("inbox.enqueued", "parent", { inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } } })
  emit("deleted", "child")
  expect(background.outstanding("parent")).toBe(true)
  emit("inbox.delivered", "parent", { inboxID })
  expect(background.outstanding("parent")).toBe(false)
})

test("delivery observed before a fast native tool return satisfies that round only", () => {
  const background = createBackgroundObserver()
  const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
  const invoke = (id: string) => {
    const tool = { id, assistantMessageID: "m", executed: true }
    emit("tool.input.started", "parent", { ...tool, name: "subagent" })
    emit("tool.called", "parent", tool)
    return () => emit("tool.success", "parent", { ...tool, metadata: { status: "running", sessionID: "child" } })
  }
  const first = invoke("first")
  emit("created", "child", { parentID: "parent" })
  emit("execution.started", "child")
  emit("execution.succeeded", "child")
  const item = { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } }
  emit("inbox.enqueued", "parent", { inboxID: "report-1", item })
  emit("inbox.delivered", "parent", { inboxID: "report-1" })
  expect(background.outstanding("parent")).toBe(true)
  first()
  expect(background.outstanding("parent")).toBe(false)

  const second = invoke("second")
  second()
  emit("inbox.enqueued", "parent", { inboxID: "report-1", item })
  emit("inbox.delivered", "parent", { inboxID: "report-1" })
  expect(background.outstanding("parent")).toBe(true)
})

test("overflow defers only the affected session family and expires after the configured wait", () => {
  let now = 0
  const background = createBackgroundObserver({ now: () => now, uncertaintyTimeout: () => 100 })
  const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
  emit("created", "parent", { parentID: "root" })
  emit("created", "child", { parentID: "parent" })
  for (let round = 0; round < 33; round++) {
    emit("execution.started", "child")
    background.idle("parent", [{ kind: "subagent", sessionID: "parent", childSessionID: "child" }])
    emit("execution.succeeded", "child")
  }
  expect(background.outstanding("unrelated")).toBe(false)
  expect(background.outstanding("root")).toBe(true)
  for (let round = 0; round < 32; round++) {
    const inboxID = `overflow-report-${round}`
    emit("inbox.enqueued", "parent", { inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } } })
    emit("inbox.delivered", "parent", { inboxID })
  }
  expect(background.outstanding("parent")).toBe(true)
  now = 101
  expect(background.outstanding("parent")).toBe(false)
  expect(background.outstanding("root")).toBe(false)
})

test("a moved child still waits for its report and deletion releases a separate child's expectation", () => {
  const background = createBackgroundObserver()
  const emit = (type: string, sessionID: string, data = {}) => background.event(event(`session.${type}`, { sessionID, ...data }))
  for (const child of ["moved-child", "deleted-child"]) {
    emit("created", child, { parentID: "parent" })
    emit("execution.started", child)
    background.idle("parent", [{ kind: "subagent", sessionID: "parent", childSessionID: child }])
  }
  background.movedAway("moved-child")
  emit("deleted", "deleted-child")
  expect(background.outstanding("parent")).toBe(true)
  const inboxID = "report-after-move"
  emit("inbox.enqueued", "parent", { inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "moved-child" } } } })
  expect(background.outstanding("parent")).toBe(true)
  emit("inbox.delivered", "parent", { inboxID })
  expect(background.outstanding("parent")).toBe(false)
})

test("failed launches, cancelled reports and fast retries keep later server commands deliverable", async () => {
  const log = join(fixture, "server-terminal-events.log")
  const path = join(fixture, "server-terminal-events.json")
  writeFileSync(path, JSON.stringify({
    deferCompleteUntilChildrenIdle: true, notification: false, sound: false, bell: false,
    command: { enabled: true, path: process.execPath, args: [commandScript, log, "{event}:{sessionID}"] },
    events: { user_message: false, session_started: false, subagent_complete: false, user_cancelled: { command: true } },
  }))
  const previous = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = path
  const location = { directory: fixture }
  const notifier = createV2Notifier({
    session: { get: async ({ sessionID }: { sessionID: string }) => ({ location, title: sessionID }) },
  } as never, location, "command", {}, createBackgroundObserver())
  const emit = (type: string, sessionID: string, data = {}) => {
    const received = event(`session.${type}`, { sessionID, ...data })
    notifier.enqueue(received)
    return received
  }
  const finished = () => existsSync(log) ? readFileSync(log, "utf8").trim().split("\n").sort() : []
  try {
    const tool = { id: "never-started", assistantMessageID: "m", executed: true }
    emit("execution.started", "launch-failure")
    emit("tool.input.started", "launch-failure", { ...tool, name: "subagent" })
    emit("tool.called", "launch-failure", tool)
    emit("created", "never-started-child", { parentID: "launch-failure", location })
    emit("tool.failed", "launch-failure", { ...tool, metadata: { status: "running", sessionID: "never-started-child" } })
    emit("execution.succeeded", "launch-failure")

    emit("execution.started", "cancelled-report")
    emit("inbox.enqueued", "cancelled-report", { inboxID: "removed-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "cancelled-child" } } } })
    emit("execution.succeeded", "cancelled-report")
    emit("inbox.cancelled", "cancelled-report", { inboxID: "removed-report" })

    emit("execution.started", "retry")
    const failure = emit("execution.failed", "retry", { error: {} })
    emit("execution.started", "retry")
    const interrupted = emit("execution.interrupted", "retry", { reason: "user" })
    emit("execution.started", "retry")
    emit("execution.succeeded", "retry")
    notifier.enqueue(failure)
    notifier.enqueue(interrupted)

    for (let n = 0; n < 100 && finished().length < 4; n++) await Bun.sleep(10)
    expect(finished()).toEqual(["complete:launch-failure", "complete:retry", "error:retry", "user_cancelled:retry"])

    emit("execution.started", "cancelled-report")
    emit("execution.succeeded", "cancelled-report")
    for (let n = 0; n < 100 && finished().length < 5; n++) await Bun.sleep(10)
    expect(finished()).toEqual(["complete:cancelled-report", "complete:launch-failure", "complete:retry", "error:retry", "user_cancelled:retry"])
  } finally {
    notifier.dispose()
    if (previous === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = previous
  }
})

test("the server command waits through the outbox gap and a reused child's second round", async () => {
  const log = join(fixture, "server-rounds.log")
  const path = join(fixture, "server-rounds.json")
  writeFileSync(path, JSON.stringify({
    deferCompleteUntilChildrenIdle: true, notification: false, sound: false, bell: false,
    command: { enabled: true, path: process.execPath, args: [commandScript, log, "{event}:{sessionID}"] },
    events: { client_connected: false, session_started: false, subagent_complete: false, user_message: false },
  }))
  const previous = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = path
  const location = { directory: fixture }
  const stream = new TransformStream<OpenCodeEvent>()
  const writer = stream.writable.getWriter()
  let subscribed = false
  const setup = setupV2({
    location,
    session: { get: async ({ sessionID }: { sessionID: string }) => ({ location, title: sessionID, ...(sessionID === "child" ? { parentID: "parent" } : {}) }) },
    event: { subscribe: () => { subscribed = true; return stream.readable } },
  } as never)
  const subscribedBeforeSetup = subscribed
  const cleanup = await setup
  const emit = (type: string, sessionID: string, data = {}) => writer.write(event(`session.${type}`, { sessionID, ...data }))
  const tool = (id: string) => ({ assistantMessageID: "message", id, executed: true })
  const report = (inboxID: string) => emit("inbox.enqueued", "parent", {
    inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } },
  })
  const commands = () => existsSync(log) ? readFileSync(log, "utf8").trim().split("\n") : []
  try {
    expect(subscribedBeforeSetup).toBe(true)
    await emit("created", "child", { location, parentID: "parent" })
    await emit("execution.started", "parent")
    await emit("tool.input.started", "parent", { ...tool("first"), name: "subagent" })
    await emit("tool.called", "parent", tool("first"))
    await emit("execution.started", "child")
    await emit("execution.succeeded", "child")
    await emit("tool.success", "parent", { ...tool("first"), metadata: { sessionID: "child", status: "running" } })
    await emit("execution.succeeded", "parent")
    await Bun.sleep(100)
    expect(commands()).toEqual([])

    await emit("execution.started", "parent")
    await emit("tool.input.started", "parent", { ...tool("second"), name: "subagent" })
    await emit("tool.called", "parent", tool("second"))
    await emit("execution.started", "child")
    await report("first-inbox")
    await emit("inbox.delivered", "parent", { inboxID: "first-inbox" })
    await emit("tool.success", "parent", { ...tool("second"), metadata: { sessionID: "child", status: "running" } })
    await emit("execution.succeeded", "child")
    await emit("execution.succeeded", "parent")
    await Bun.sleep(100)
    expect(commands()).toEqual([])

    await report("second-inbox")
    await emit("execution.started", "parent")
    await emit("inbox.delivered", "parent", { inboxID: "second-inbox" })
    await emit("execution.succeeded", "parent")
    await emit("execution.succeeded", "ordinary")

    const lateChild = "reordered-child", lateRoot = "reordered", inboxID = "reordered-inbox"
    await emit("created", lateChild, { location, parentID: lateRoot })
    await emit("tool.input.started", lateRoot, { ...tool("reordered"), name: "subagent" })
    await emit("tool.called", lateRoot, tool("reordered"))
    await emit("execution.started", lateChild)
    await emit("tool.success", lateRoot, { ...tool("reordered"), metadata: { sessionID: lateChild, status: "running" } })
    await emit("execution.succeeded", lateChild)
    const foreignLocation = { directory: "/other" }
    await writer.write({ ...event("session.inbox.delivered", { sessionID: lateRoot, inboxID }), location: foreignLocation })
    await writer.write({ ...event("session.inbox.enqueued", { sessionID: lateRoot, inboxID,
      item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: lateChild } } },
    }), location: foreignLocation })
    await emit("execution.started", lateRoot)
    await emit("execution.succeeded", lateRoot)

    const surviving = "surviving-root", removed = "removed-child"
    const reportData = { sessionID: removed, inboxID: "deleted-inbox", item: { type: "synthetic",
      payload: { metadata: { source: "subagent", childID: "reporting-grandchild" } } } }
    await emit("created", removed, { location, parentID: surviving })
    await emit("moved", removed, { location: foreignLocation })
    await writer.write({ ...event("session.inbox.enqueued", reportData), location: foreignLocation })
    await emit("execution.started", surviving)
    await emit("execution.succeeded", surviving)
    for (let n = 0; n < 100 && commands().length < 3; n++) await Bun.sleep(10)
    expect(commands().sort()).toEqual(["complete:ordinary", "complete:parent", "complete:reordered"])
    await writer.write({ ...event("session.deleted", { sessionID: removed }), location: foreignLocation })
    await writer.write({ ...event("session.inbox.enqueued", reportData), location: foreignLocation })
    await emit("execution.started", surviving)
    await emit("execution.succeeded", surviving)
    for (let n = 0; n < 100 && commands().length < 4; n++) await Bun.sleep(10)
    expect(commands().sort()).toEqual(["complete:ordinary", "complete:parent", "complete:reordered", "complete:surviving-root"])
  } finally {
    await writer.close()
    await cleanup?.()
    if (previous === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = previous
  }
})

test("a linked child starting in another server location cancels the parent's in-flight completion", async () => {
  const location = { directory: fixture }
  const foreign = { directory: join(fixture, "foreign") }
  const stream = new TransformStream<OpenCodeEvent>()
  const peer = new TransformStream<OpenCodeEvent>()
  const writer = stream.writable.getWriter()
  const peerWriter = peer.writable.getWriter()
  const path = join(fixture, "cross-location-start.json")
  writeFileSync(path, JSON.stringify({ deferCompleteUntilChildrenIdle: true,
    command: { enabled: true, path: process.execPath, args: [commandScript, join(fixture, "cross-location-start.log"), "{event}"] },
    events: { client_connected: false, session_started: false, subagent_complete: false, user_message: false } }))
  const previous = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = path
  let startWait!: (signal: AbortSignal) => void
  const waiting = new Promise<AbortSignal>(resolve => { startWait = resolve })
  let release!: () => void
  const held = new Promise<void>(resolve => { release = resolve })
  const dispose = await setupV2({ location, app: { version: "2.3.2" }, event: { subscribe: () => stream.readable },
    session: {
      get: async () => ({ location }),
      list: async ({ parentID }: { parentID: string }) => ({
        data: parentID === "root" ? [{ id: "child", parentID, time: { busy: 20, idle: 10 } }] : [],
      }),
      wait: async (_input: unknown, { signal }: { signal: AbortSignal }) => { startWait(signal); await held },
    },
  } as never)
  const disposePeer = await setupV2({ location: foreign, event: { subscribe: () => peer.readable }, session: {} } as never)
  try {
    await writer.write({ ...event("session.execution.succeeded", { sessionID: "root" }), location, created: 100 })
    const signal = await waiting
    // A read from the peer's stream after the start is a barrier for fan-out.
    await peerWriter.write(event("session.execution.started", { sessionID: "child" }))
    await peerWriter.write(event("session.execution.started", { sessionID: "unrelated" }))
    expect(signal.aborted).toBe(true)
  } finally {
    release()
    await Promise.all([writer.close(), peerWriter.close()])
    await Promise.all([dispose?.(), disposePeer?.()])
    if (previous === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = previous
  }
})

test("a timed-out permission lookup releases subsequent delivery in the same session", async () => {
  const log = join(fixture, "permission-timeout.log")
  const path = join(fixture, "permission-timeout.json")
  writeFileSync(path, JSON.stringify({ command: { enabled: true, path: process.execPath, args: [commandScript, log, "{event}"] } }))
  const previous = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = path
  let query: AbortSignal | undefined
  const notifier = createV2Notifier({
    session: { get: async () => ({ title: "test" }) },
    permission: { list: (_, options) => {
      query = options?.signal ?? undefined
      return new Promise(() => {})
    } },
  } as never, { directory: fixture }, "command")
  try {
    notifier.enqueue(event("permission.asked", { sessionID: "same", id: "permission-timeout" }))
    notifier.enqueue(event("form.created", { form: { sessionID: "same", id: "after-timeout", metadata: { kind: "question" } } }))
    const deadline = Date.now() + OUTSTANDING_WORK_TIMEOUT_MS + 1500
    while (Date.now() < deadline && (!existsSync(log) || readFileSync(log, "utf8").trim().split("\n").length < 2)) await Bun.sleep(20)
    expect(query?.aborted).toBe(true)
    expect(readFileSync(log, "utf8").trim().split("\n").sort()).toEqual(["permission", "question"])
  } finally {
    notifier.dispose()
    if (previous === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = previous
  }
})

type InboxItem = { type: string; payload?: { metadata?: { source?: string } } }

async function exercise(events: OpenCodeEvent[] | ((notifier: ReturnType<typeof createV2Notifier>) => Promise<void>), options: {
  minDuration?: number
  commandMinDuration?: number
  missing?: boolean
  hangOwnership?: boolean
  child?: boolean
  delivery?: Delivery
  command?: boolean
  permissionPending?: boolean
  messageAge?: number
  messageTimes?: number[]
  unknownElapsed?: boolean
  agentName?: string
  workspaceID?: string
  sessionLocation?: { directory: string; workspaceID?: string }
  readSessionLocation?: () => { directory: string; workspaceID?: string } | undefined
  onGet?: () => void
  onClaim?: () => void | Promise<void>
  silent?: boolean
  deferChildren?: boolean
  managed?: ManagedServiceReadback
  reportsDirectChildren?: boolean
  rpcStatus?: "unavailable" | "failed" | "second-read-unavailable"
  authoritative?: {
    jobs?: readonly object[]
    readActive?: () => readonly string[]
    nativeActivity?: () => unknown
    lastSettled?: (sessionID: string) => number | undefined
    externalActivity?: string[] | (() => string[])
    waitForServerSession?: (sessionID: string, signal?: AbortSignal) => Promise<void>
    responseDelay?: number
    nativeInboxDelay?: number
    nativeInboxResponse?: unknown
    inbox?: readonly InboxItem[]
    readInbox?: (sessionID: string) => readonly InboxItem[]
    listChildren?: (parentID: string, cursor?: string) => {
      data: readonly { id: string; parentID: string; time?: { busy?: number; idle?: number } }[]
      cursor?: { next?: string }
    }
    fail?: boolean
    onCheck?: (sessionID: string) => void
    onCheckInput?: (input: { sessionID: string; descendants?: boolean; status?: string }) => void
    onInbox?: () => void
  }
} = {}) {
  writeFileSync(configPath, JSON.stringify({
    suppressWhenFocused: false, sound: false, bell: false, notificationSystem: "ghostty",
    minDuration: options.minDuration ?? 0,
    deferCompleteUntilChildrenIdle: options.deferChildren ?? false,
    command: { enabled: options.command ?? false, path: process.execPath, args: [commandScript, commandLog, "{event}"], minDuration: options.commandMinDuration ?? 0 },
    events: { session_started: false, client_connected: false, user_message: { notification: true }, complete: options.silent ? false : { notification: true }, subagent_complete: options.agentName ? { notification: true } : false },
    messages: { user_message: "PROBE_USER {turn}", complete: "PROBE_DONE {turn}", error: "PROBE_ERROR {turn}", permission: "PROBE_PERMISSION {turn}", question: "PROBE_QUESTION {turn}", subagent_complete: "PROBE_AGENT {agentName} {turn}" },
  }))
  const oldConfig = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = configPath
  const originalWrite = process.stdout.write
  const writes: string[] = []
  process.stdout.write = ((chunk: unknown, ...args: unknown[]) => {
    writes.push(String(chunk))
    const cb = args.find(x => typeof x === "function") as undefined | (() => void)
    cb?.()
    return true
  }) as typeof process.stdout.write
  const { readActive, listChildren, waitForServerSession } = options.authoritative ?? {}
  const waitForResponse = async () => {
    if (options.authoritative?.responseDelay) await Bun.sleep(options.authoritative.responseDelay)
  }
  let familyReads = 0
  const readFamily = options.authoritative ? async ({ sessionID }: { sessionID: string }) => {
    if (options.rpcStatus === "unavailable") return
    if (options.rpcStatus === "failed") throw new Error("Metadata lookup failed")
    if (options.rpcStatus === "second-read-unavailable" && ++familyReads > 1) return
    await waitForResponse()
    const ids = new Set([sessionID])
    for (const parent of ids) {
      let cursor: string | undefined
      const cursors = new Set<string>()
      do {
        const page = listChildren?.(parent, cursor)
        for (const child of page?.data ?? []) if (child.parentID === parent) ids.add(child.id)
        cursor = page?.cursor?.next
        if (cursor && cursors.has(cursor)) throw new Error("The fixture repeated a cursor")
        if (cursor) cursors.add(cursor)
      } while (cursor)
    }
    const items = [...ids].flatMap(id => {
      options.authoritative?.onInbox?.()
      const rows = options.authoritative?.readInbox?.(id) ?? options.authoritative?.inbox ?? []
      return rows.map((item, index) => ({ sessionID: id, id: `${id}-${index}`,
        report: item.type !== "synthetic" ? 0 : item.payload?.metadata?.source === "subagent" ? 1
          : ["shell", "codemode", "experimental_image_generate"].includes(item.payload?.metadata?.source ?? "") ? 2 : 0 }))
    })
    return {
      sessions: [...ids].map(id => ({ id, idle: options.authoritative?.lastSettled?.(id) ?? null })), items,
      externalActivity: typeof options.authoritative?.externalActivity === "function"
        ? options.authoritative.externalActivity() : options.authoritative?.externalActivity,
    }
  } : undefined
  const notifier = createV2Notifier({
    session: {
      get: async ({ sessionID }: { sessionID: string }) => {
        await waitForResponse()
        options.onGet?.()
        if (options.hangOwnership) return new Promise<never>(() => {})
        if (options.missing) throw new Error("Session not found")
        const location = options.readSessionLocation ? options.readSessionLocation()
          : options.sessionLocation ?? (options.managed ? { directory: fixture } : undefined)
        return { id: "session", title: "test", location, time: {
          idle: options.authoritative?.lastSettled?.(sessionID),
        }, ...(options.child ? { parentID: "parent", agent: options.agentName } : {}) }
      },
      context: async () => options.unknownElapsed ? [] :
        (options.messageTimes ?? [Date.now() - (options.messageAge ?? 1000)]).map((created, index) =>
          ({ id: `message-${index}`, type: "user", text: "hello", time: { created } }) satisfies SessionMessageUser),
      ...(readActive || options.authoritative?.nativeActivity ? {
        active: async () => {
          await waitForResponse()
          return options.authoritative?.nativeActivity ? options.authoritative.nativeActivity()
            : Object.fromEntries(readActive!().map(id => [id, { type: "running" }]))
        },
      } : {}),
      ...(listChildren ? {
        list: async ({ parentID, cursor }: { parentID: string; cursor?: string }) => {
          await waitForResponse()
          return listChildren(parentID, cursor)
        },
      } : {}),
      ...(waitForServerSession ? {
        wait: ({ sessionID }: { sessionID: string }, { signal }: { signal?: AbortSignal }) => waitForServerSession(sessionID, signal),
      } : {}),
      ...(options.rpcStatus ? { inbox: { list: async () => {
        await waitForResponse()
        if (options.authoritative?.nativeInboxDelay) await Bun.sleep(options.authoritative.nativeInboxDelay)
        options.authoritative?.onInbox?.()
        return options.authoritative?.nativeInboxResponse ?? []
      } } } : {}),
    },
    permission: { list: async () => options.permissionPending ? [{ id: "test-permission" }] : [] },
    ...(options.authoritative ? { experimental: { job: { list: async (input: { sessionID: string; descendants?: boolean; status?: string }) => {
      await waitForResponse()
      options.authoritative?.onCheck?.(input.sessionID)
      options.authoritative?.onCheckInput?.(input)
      if (options.authoritative?.fail) throw new Error("job failed")
      return options.authoritative?.jobs ?? []
    } } } } : {}),
  } as never, { directory: fixture, workspaceID: options.workspaceID }, options.delivery ?? "terminal", { claim: async (key, _sessionID, valid) => {
    await options.onClaim?.()
    return valid() && claim(key)
  } },
  waitForServerSession ? createBackgroundObserver({ reportsDirectChildren: options.reportsDirectChildren ?? true }) : undefined, options.managed, readFamily)
  try {
    if (typeof events === "function") await events(notifier)
    else for (const input of events) await notifier.event(input)
    return writes.filter(w => w.includes("PROBE_"))
  } finally {
    notifier.dispose()
    process.stdout.write = originalWrite
    if (oldConfig === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = oldConfig
  }
}

test("a user prompt emits once across enqueue, delivery and replay", async () => {
  const enqueued = event("session.inbox.enqueued", { sessionID: "user-session", inboxID: "u1", item: { type: "user", payload: { text: "hi" }, delivery: "queue" } })
  expect((await exercise([enqueued, event("session.inbox.delivered", { sessionID: "user-session", inboxID: "u1" }), enqueued])).length).toBe(1)
})

test("a tracked child with no enabled server command needs no completion metadata", async () => {
  let reads = 0
  const location = { directory: fixture }
  const sessionID = "disabled-child"
  await exercise(async notifier => {
    const created = event("session.created", { sessionID, parentID: "parent", location })
    await notifier.event(created, notifier.received(created))
    const success = { ...event("session.execution.succeeded", { sessionID }), location }
    await notifier.event(success, notifier.received(success))
  }, { delivery: "command", onGet: () => { reads++ } })
  expect(reads).toBe(0)
})

test("an early inbox acknowledgement does not suppress a local user-message event", async () => {
  const sessionID = "user-location-move", inboxID = "user-reordered"
  const alerts = await exercise(async notifier => {
    notifier.enqueue({ ...event("session.inbox.delivered", { sessionID, inboxID }), location: { directory: "/other" } })
    notifier.enqueue({ ...event("session.inbox.enqueued", { sessionID, inboxID, item: { type: "user" } }), location: { directory: fixture } })
    await Bun.sleep(30)
  })
  expect(alerts).toHaveLength(1)
})

test("a user can enqueue a newer durable inbox item with a cancelled ID", async () => {
  const sessionID = "reused-inbox-root", inboxID = "reused-inbox-item"
  const durable = (type: string, data: object, seq: number) => ({ ...event(type, data), durable: { seq } }) as OpenCodeEvent
  let enqueueAgain!: () => void
  let queued = false
  const alerts = await exercise(async notifier => {
    notifier.enqueue(durable("session.inbox.cancelled", { sessionID, inboxID }, 10))
    enqueueAgain = () => notifier.enqueue(durable("session.inbox.enqueued", { sessionID, inboxID, item: { type: "user" } }, 13))
    notifier.enqueue(event("session.execution.started", { sessionID }))
    notifier.enqueue(event("session.execution.succeeded", { sessionID }))
    await Bun.sleep(65)
  }, { deferChildren: true, onClaim: () => { if (!queued) { queued = true; enqueueAgain() } } })
  expect(queued).toBe(true)
  expect(alerts.filter(alert => alert.includes("PROBE_DONE"))).toHaveLength(0)
  expect(alerts.filter(alert => alert.includes("PROBE_USER"))).toHaveLength(1)
})

test("reusing a child does not discard the preceding child completion", async () => {
  const id = "reused-notification-child"
  const alerts = await exercise(async notifier => {
    notifier.enqueue(event("session.execution.succeeded", { sessionID: id }))
    notifier.enqueue(event("session.execution.started", { sessionID: id }))
    await Bun.sleep(30)
  }, { child: true, agentName: "build" })
  expect(alerts).toHaveLength(1)
  expect(alerts[0]).toContain("PROBE_AGENT build")
})

test("a metadata failure does not cache an untracked child as a root", async () => {
  const sessionID = "untracked-child-retry", location = { directory: fixture }
  const success = () => ({ ...event("session.execution.succeeded", { sessionID }), location })
  let lookups = 0
  const alerts = await exercise(async notifier => {
    notifier.enqueue(success())
    await Bun.sleep(25)
    notifier.enqueue(success())
    notifier.enqueue({ ...event("session.execution.started", { sessionID }), location })
    await Bun.sleep(35)
  }, { child: true, agentName: "build", deferChildren: true, onGet: () => {
    if (++lookups === 1) throw new Error("temporary metadata failure")
  } })
  expect(alerts.filter(alert => alert.includes("PROBE_AGENT"))).toHaveLength(1)
})

test("default root completion survives the next execution start", async () => {
  const id = "default-root-reused"
  const alerts = await exercise(async notifier => {
    notifier.enqueue(event("session.execution.succeeded", { sessionID: id }))
    notifier.enqueue(event("session.execution.started", { sessionID: id }))
    await Bun.sleep(30)
  })
  expect(alerts).toHaveLength(1)
  expect(alerts[0]).toContain("PROBE_DONE")
})

test("completion duration uses the prompt that preceded the successful turn", async () => {
  const success = { ...event("session.execution.succeeded", { sessionID: "duration-reused-root" }), created: 10_000 }
  const alerts = await exercise([success], { minDuration: 5, messageTimes: [1_000, 11_000] })
  expect(alerts).toHaveLength(1)
})

test("superseded successes for an observed root do not require metadata lookups", async () => {
  const id = "superseded-known-root"
  const location = { directory: fixture }
  let lookups = 0
  await exercise(async notifier => {
    notifier.enqueue({ ...event("session.created", { sessionID: id, location }), location })
    for (let i = 0; i < 8; i++) notifier.enqueue({ ...event("session.execution.succeeded", { sessionID: id }), location })
    notifier.enqueue({ ...event("session.execution.started", { sessionID: id }), location })
    await Bun.sleep(30)
  }, { deferChildren: true, onGet: () => { lookups++ } })
  expect(lookups).toBe(0)
})

test("matching foreign activity blocks completion only while the descendant is live", async () => {
  const id = "foreign-active-child"
  let active = [id, "unrelated"]
  const success = () => event("session.execution.succeeded", { sessionID: "foreign-activity-root" })
  const options = { deferChildren: true, authoritative: {
    readActive: () => active,
    externalActivity: [createHash("sha256").update(id).digest("hex")],
  } }
  expect(await exercise([success()], options)).toHaveLength(0)
  active = ["unrelated"]
  expect(await exercise([success()], options)).toHaveLength(1)
})

test("family readback matches a foreign start that arrived before the child was discovered", async () => {
  const root = "newly-linked-root", child = "newly-linked-child"
  const link = createHash("sha256").update(child).digest("hex")
  let reads = 0, start!: () => void
  const alerts = await exercise(async notifier => {
    start = () => notifier.enqueue({ ...event("session.execution.started", { sessionID: child }), location: { directory: "/other" } })
    // Keep a second read necessary without identifying the real child early.
    const success = event("session.execution.succeeded", { sessionID: root })
    await notifier.event(success, notifier.received(success))
  }, { deferChildren: true, authoritative: {
    readActive: () => [],
    externalActivity: () => {
      if (++reads === 1) return ["0".repeat(64)]
      start()
      return [link]
    },
  } })
  expect(reads).toBe(2)
  expect(alerts).toHaveLength(0)
})

test.each([undefined, null, [], { child: null }])("malformed native activity cannot authorize completion (%p)", async value => {
  const alerts = await exercise([event("session.execution.succeeded", { sessionID: "invalid-activity-root" })], {
    deferChildren: true, authoritative: { nativeActivity: () => value },
  })
  expect(alerts).toHaveLength(0)
})

test("a moved child cannot deliver an earlier completion in the former location", async () => {
  const id = "completed-child-moved"
  let move: (() => void) | undefined
  const alerts = await exercise(async notifier => {
    move = () => notifier.enqueue(event("session.moved", { sessionID: id, location: { directory: "/other" } }))
    notifier.enqueue({ ...event("session.execution.succeeded", { sessionID: id }), location: { directory: fixture } })
    await Bun.sleep(30)
  }, { child: true, agentName: "build", onGet: () => move?.() })
  expect(alerts).toHaveLength(0)
})

for (const type of ["permission", "question"]) test(`a queued ${type} must retain session ownership`, async () => {
  const sessionID = `moved-${type}`
  const prompt = () => type === "permission" ? event("permission.asked", { sessionID, id: "test-permission" })
    : event("form.created", { form: { sessionID, id: "test-question", metadata: { kind: "question" } } })
  expect(await exercise([prompt()], { permissionPending: true })).toHaveLength(1)
  let move: (() => void) | undefined
  const alerts = await exercise(async notifier => {
    move = () => notifier.enqueue(event("session.moved", { sessionID, location: { directory: "/other-workspace" } }))
    await notifier.event(prompt())
  }, { permissionPending: true, onGet: () => move?.() })
  expect(alerts).toHaveLength(0)
})

test.each([false, true])("an idle foreign report cancellation affects only its own family (matching digest: %s)", async matching => {
  const foreign = "foreign-cancelled-child"
  let cancel: () => void
  let received!: () => void
  const cancellation = new Promise<void>(resolve => { received = resolve })
  const alerts = await exercise(async notifier => {
    cancel = () => {
      notifier.enqueue(event("session.inbox.cancelled", { sessionID: foreign, inboxID: "cold-foreign-result" }))
      received()
    }
    notifier.enqueue(event("session.execution.succeeded", { sessionID: "foreign-cancellation-root" }))
    await cancellation
    await Bun.sleep(75)
  }, {
    deferChildren: true,
    authoritative: {
      readActive: () => [],
      externalActivity: [createHash("sha256").update(matching ? foreign : "unrelated-child").digest("hex")],
      onCheck: () => cancel(),
    },
  })
  expect(alerts).toHaveLength(matching ? 0 : 1)
})

test.each([["cancellation", false], ["cancellation", true], ["admission", false], ["admission", true],
  ["ordinary cancellation", true], ["execution start", false], ["execution start", true]] as const)(
  "a foreign %s during the claim invalidates only the matching root (%s)", async (kind, matching) => {
  const foreign = "foreign-claim-child"
  let receiveCancellation: () => void
  let received!: () => void
  const cancellation = new Promise<void>(resolve => { received = resolve })
  const alerts = await exercise(async notifier => {
    if (kind === "execution start") notifier.enqueue(event("session.moved", { sessionID: foreign, location: { directory: "/other" } }))
    receiveCancellation = () => {
      if (kind === "execution start") {
        notifier.enqueue(event("session.execution.started", { sessionID: foreign }))
        received()
        return
      }
      const data = { sessionID: foreign, inboxID: "late-foreign-result" }
      if (kind === "ordinary cancellation") notifier.enqueue({
        ...event("session.inbox.enqueued", { ...data, item: { type: "user" } }), location: { directory: "/other" },
      })
      const receipt = kind.endsWith("cancellation") ? event("session.inbox.cancelled", data) : event("session.inbox.enqueued", {
        ...data, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "foreign-grandchild" } } },
      })
      notifier.enqueue({ ...receipt, location: { directory: "/other" } })
      received()
    }
    notifier.enqueue(event("session.execution.succeeded", { sessionID: "foreign-claim-root" }))
    await cancellation
    await Bun.sleep(40)
  }, {
    deferChildren: true, onClaim: () => receiveCancellation(),
    authoritative: { readActive: () => [], externalActivity: [createHash("sha256").update(matching ? foreign : "unrelated-child").digest("hex")] },
  })
  expect(alerts).toHaveLength(matching && kind !== "ordinary cancellation" ? 0 : 1)
})

test.each([["directory", "/other", null], ["workspace", fixture, "other"]] as const)(
  "managed mode rejects a newer root placement in another %s", async (_kind, directory, workspaceID) => {
    const root = "moved-managed-root"
    const alerts = await exercise([event("session.execution.succeeded", { sessionID: root })], {
      deferChildren: true, managed: {
        active: async () => ({}), jobs: async () => [],
        family: async () => ({ sessions: [{ id: root, idle: null, directory, workspaceID }], items: [] }),
      },
    })
    expect(alerts).toHaveLength(0)
  },
)

test.each([["cancelled", true], ["cancelled", false], ["delivered", true], ["delivered", false]] as const)(
  "a %s result (enqueue observed: %s) invalidates a managed completion before its inbox read", async (outcome, seenEnqueue) => {
  const root = "managed-cancel-root"
  let release!: () => void
  let startLookup!: (signal: AbortSignal | undefined) => void
  const holdJobs = new Promise<void>(resolve => { release = resolve })
  const lookup = new Promise<AbortSignal | undefined>(resolve => { startLookup = resolve })
  let readInboxes = 0
  const alerts = await exercise(async notifier => {
    if (seenEnqueue) notifier.enqueue(event("session.inbox.enqueued", { sessionID: root, inboxID: "queued", item: {
      type: "synthetic", payload: { metadata: { source: "subagent" } },
    } }))
    notifier.enqueue(event("session.execution.succeeded", { sessionID: root }))
    const signal = await lookup
    try {
      notifier.enqueue(event(`session.inbox.${outcome}`, { sessionID: root, inboxID: "queued" }))
      expect(signal?.aborted).toBe(true)
      expect(readInboxes).toBe(0)
    } finally {
      release()
    }
    await notifier.event(event("session.execution.succeeded", { sessionID: root }))
  }, {
    deferChildren: true,
    managed: {
      active: async () => ({}),
      jobs: async (_input, options) => {
        startLookup(options?.signal)
        await holdJobs
        options?.signal?.throwIfAborted()
        return []
      },
      family: async () => { readInboxes++; return { sessions: [{ id: root, idle: null, directory: fixture, workspaceID: null }], items: [] } },
    },
  })
  expect(readInboxes).toBe(1)
  expect(alerts).toHaveLength(1)
})

test("a managed completion can use live observation when its relative database cannot be identified", async () => {
  const success = { ...event("session.execution.succeeded", { sessionID: "unverified-database-root" }), created: 100 }
  const alerts = await exercise([success], {
    deferChildren: true,
    authoritative: { listChildren: () => ({ data: [] }), waitForServerSession: async () => {} },
    managed: {
      active: async () => ({}), jobs: async () => [], family: async () => { throw new InboxStoreUnavailable() },
    },
  })
  expect(alerts).toHaveLength(1)
})

test("managed live readback checks the root's location again after a store failure", async () => {
  const root = "managed-root-moved-during-fallback"
  let moved = false
  const alerts = await exercise([event("session.execution.succeeded", { sessionID: root })], {
    deferChildren: true, readSessionLocation: () => ({ directory: moved ? "/other-location" : fixture }),
    authoritative: { listChildren: () => ({ data: [] }), waitForServerSession: async () => {} },
    managed: {
      active: async () => ({}), jobs: async () => [],
      family: async () => { moved = true; throw new InboxStoreUnavailable() },
    },
  })
  expect(alerts).toHaveLength(0)
})

test.each(["local", "moved", "unscoped"])(
  "managed workspace completion uses the native event location when the session API redacts it (%s)", async scenario => {
    const sessionID = "managed-workspace-root"
    const location = { directory: fixture, workspaceID: "fixture-workspace" }
    const completion = event("session.execution.succeeded", { sessionID })
    const success = scenario === "unscoped" ? completion : { ...completion, location }
    let move: () => void
    let readbackCalled = false
    writeFileSync(commandLog, "")
    await exercise(async notifier => {
      move = () => { notifier.received(event("session.moved", { sessionID, location: { ...location, workspaceID: "other-workspace" } })) }
      await notifier.event(success, notifier.received(success))
    }, {
      workspaceID: location.workspaceID, deferChildren: true, command: true, delivery: "command",
      sessionLocation: { directory: fixture },
      authoritative: { listChildren: () => ({ data: [] }), waitForServerSession: async () => {} },
      managed: {
        active: async () => ({}), jobs: async () => [], family: async () => {
          readbackCalled = true
          if (scenario === "moved") move()
          throw new InboxStoreUnavailable()
        },
      },
    })
    expect(readbackCalled).toBe(scenario !== "unscoped")
    if (scenario === "local") await waitForCommands(1)
    expect(readFileSync(commandLog, "utf8")).toBe(scenario === "local" ? "complete\n" : "")
  },
)

test.each([true, false])("managed metadata fallback checks native descendants on older hosts (active: %s)", async active => {
  const root = "older-managed-root"
  const child = "older-managed-child"
  const success = { ...event("session.execution.succeeded", { sessionID: root }), created: 100 }
  const alerts = await exercise([success], {
    deferChildren: true, reportsDirectChildren: false,
    authoritative: { listChildren: parentID => ({ data: parentID === root ? [{ id: child, parentID }] : [] }), waitForServerSession: async () => {} },
    managed: {
      active: async () => active ? { [child]: { type: "running" } } : {},
      jobs: async () => [], family: async () => { throw new InboxStoreUnavailable() },
    },
  })
  expect(alerts).toHaveLength(active ? 0 : 1)
})

test.each([["shell", "root", 1], ["shell", "child", 0], ["subagent", "root", 0]] as const)(
  "managed completion accounts for a running %s job owned by %s", async (kind, sessionID, alerts) => {
    const output = await exercise([event("session.execution.succeeded", { sessionID: "root" })], {
      deferChildren: true,
      managed: {
        active: async () => ({}),
        jobs: async () => [{ kind, sessionID, status: "running" }],
        family: async () => ({ sessions: [{ id: "root", idle: null, directory: fixture, workspaceID: null }], items: [] }),
      },
    })
    expect(output).toHaveLength(alerts)
  },
)

test("the V2 terminal also waits for a running shell Job owned by the root itself", async () => {
  const root = "terminal-shell-root"
  const alerts = await exercise([event("session.execution.succeeded", { sessionID: root })], {
    deferChildren: true,
    authoritative: { readActive: () => [], jobs: [{ kind: "shell", sessionID: root, status: "running" }] },
  })
  expect(alerts).toHaveLength(0)
})

for (const [kind, source] of [["shell", "shell"], ["codemode", "codemode"], ["image", "experimental_image_generate"]])
test.each(["Job", "inbox"])(`a settled descendant ${kind} blocks completion through its %s handoff`, async stage => {
  const root = "shell-handoff-root", child = "shell-handoff-child"
  const alerts = await exercise([event("session.execution.succeeded", { sessionID: root })], {
    deferChildren: true,
    authoritative: {
      readActive: () => [],
      listChildren: parentID => ({ data: parentID === root ? [{ id: child, parentID }] : [] }),
      jobs: stage === "Job" ? [{ kind, sessionID: child, status: "completed" }] : [],
      readInbox: id => stage === "inbox" && id === child ? [{ type: "synthetic", payload: { metadata: { source } } }] : [],
    },
  })
  expect(alerts).toHaveLength(0)
})

test("native local subagent success preserves a report expectation", () => {
  const observer = createBackgroundObserver()
  const tool = { sessionID: "audit-parent", assistantMessageID: "m", id: "native-local" }
  observer.event(event("session.tool.input.started", { ...tool, name: "subagent" }))
  observer.event(event("session.tool.called", { ...tool, executed: false }))
  observer.event(event("session.tool.success", { ...tool, executed: false, metadata: { sessionID: "audit-child", status: "running" } }))
  observer.event(event("session.execution.started", { sessionID: "audit-child" }))
  observer.event(event("session.execution.succeeded", { sessionID: "audit-child" }))
  expect(observer.outstanding("audit-parent")).toBe(true)
})

test("a stale idle snapshot cannot reopen an acknowledged child round", () => {
  let now = 0
  const observer = createBackgroundObserver({ now: () => now, uncertaintyTimeout: () => 100 })
  const root = "audit-root"
  const child = "audit-child"
  const work = [{ sessionID: root, childSessionID: child, kind: "subagent" }]
  observer.event(event("session.created", { sessionID: child, parentID: root }))
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.idle(root, work)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "first", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "first" }))
  expect(observer.outstanding(root)).toBe(false)
  observer.idle(root, work)
  expect(observer.outstanding(root)).toBe(false)
  now = 20
  observer.idle(root, work)
  now = 101
  expect(observer.outstanding(root)).toBe(false)
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.idle(root, work)
  expect(observer.outstanding(root)).toBe(true)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "new-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "new-report" }))
  expect(observer.outstanding(root)).toBe(false)
})

test.each([[false, true], [true, true], [true, false]] as const)(
  "an expired alert does not retire a confirmed Job (after settlement: %s, direct results: %s)", (afterSettlement, reportsDirectChildren) => {
  let now = 0
  const observer = createBackgroundObserver({ reportsDirectChildren, now: () => now, uncertaintyTimeout: () => 100 })
  const root = "root"
  const child = "child"
  const work = [{ sessionID: root, childSessionID: child, kind: "subagent" }]
  observer.event(event("session.created", { sessionID: child, parentID: root }))
  observer.event(event("session.execution.started", { sessionID: child }))
  if (afterSettlement) {
    observer.event(event("session.step.started", { sessionID: child, assistantMessageID: "answer" }))
    observer.event(event("session.step.ended", { sessionID: child, assistantMessageID: "answer", finish: "stop" }))
  }
  if (!afterSettlement) observer.idle(root, work)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  if (afterSettlement) observer.idle(root, work)
  now = 101
  expect(observer.outstanding(root)).toBe(true)
  observer.idle(root, work)
  expect(observer.outstanding(root)).toBe(true)
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "confirmed-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "confirmed-report" }))
  expect(observer.outstanding(root)).toBe(false)
})

test("a root's own shell result during readback does not invalidate its completion", async () => {
  const sessionID = "root-shell-admission", inboxID = "own-shell-result"
  let admit: (() => void) | undefined
  const alerts = await exercise(async notifier => {
    admit = () => {
      notifier.enqueue(event("session.inbox.enqueued", { sessionID, inboxID, item: { type: "synthetic", payload: { metadata: { source: "shell" } } } }))
      notifier.enqueue(event("session.inbox.delivered", { sessionID, inboxID }))
      admit = undefined
    }
    notifier.enqueue(event("session.execution.succeeded", { sessionID }))
    await Bun.sleep(65)
  }, { deferChildren: true, authoritative: { readActive: () => [], onCheck: () => admit?.() } })
  expect(alerts).toHaveLength(1)
})

test("acknowledged idle snapshots leave room for new session families", () => {
  let now = 0
  const observer = createBackgroundObserver({ now: () => now, uncertaintyTimeout: () => 100 })
  for (let index = 0; index < 4096; index++) {
    const child = `child-${index}`
    const inboxID = `report-${index}`
    observer.event(event("session.inbox.enqueued", {
      sessionID: "root", inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
    }))
    observer.event(event("session.inbox.delivered", { sessionID: "root", inboxID }))
    observer.idle("root", [{ sessionID: "root", childSessionID: child, kind: "subagent" }])
  }
  now = 101
  observer.event(event("session.created", { sessionID: "fresh-child", parentID: "fresh-root" }))
  expect(observer.outstanding("fresh-root")).toBe(false)
})

test.each(["unconfirmed", "tool", "idle"])("a reused child's inbox obligation follows the Job evidence (%s)", evidence => {
  const confirmed = evidence !== "unconfirmed"
  let now = 0
  const observer = createBackgroundObserver({ reportsDirectChildren: true, now: () => now, uncertaintyTimeout: () => 100 })
  const root = "root"
  const child = "child"
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "previous", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "previous" }))
  observer.idle(root, [{ sessionID: root, childSessionID: child, kind: "subagent" }])
  const tool = { sessionID: root, assistantMessageID: "message", id: "native-reuse", executed: false }
  if (evidence === "tool") {
    observer.event(event("session.tool.input.started", { ...tool, name: "subagent" }))
    observer.event(event("session.tool.called", tool))
  }
  now = 50
  observer.event(event("session.execution.started", { sessionID: child }))
  if (evidence === "tool") observer.event(event("session.tool.success", { ...tool, metadata: { sessionID: child, status: "running" } }))
  if (evidence === "idle") observer.idle(root, [{ sessionID: root, childSessionID: child, kind: "subagent" }])
  now = 200
  expect(observer.outstanding(root)).toBe(confirmed)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  expect(observer.outstanding(root)).toBe(confirmed)
  now = 500
  expect(observer.outstanding(root)).toBe(confirmed)
})

test("a moved session and its parent hold the queued report until it is acknowledged", () => {
  const observer = createBackgroundObserver()
  const root = "root", child = "child", grandchild = "grandchild"
  observer.event(event("session.created", { sessionID: child, parentID: root }))
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.idle(root, [{ sessionID: root, childSessionID: child, kind: "subagent" }])
  observer.event(event("session.created", { sessionID: grandchild, parentID: child }))
  observer.event(event("session.execution.started", { sessionID: grandchild }))
  observer.idle(child, [{ sessionID: child, childSessionID: grandchild, kind: "subagent" }])
  observer.event(event("session.execution.succeeded", { sessionID: grandchild }))
  observer.event(event("session.inbox.enqueued", {
    sessionID: child, inboxID: "held-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: grandchild } } },
  }))
  expect(observer.outstanding(child)).toBe(true)
  observer.movedAway(child)
  expect(observer.outstanding(child)).toBe(true)
  expect(observer.outstanding(root)).toBe(true)
  observer.event(event("session.inbox.cancelled", { sessionID: child, inboxID: "held-report" }))
  expect(observer.outstanding(child)).toBe(false)
  expect(observer.outstanding(root)).toBe(true)
})

test("a live Job can confirm a child round after it leaves this location", () => {
  const observer = createBackgroundObserver()
  const root = "moved-job-parent", child = "moved-job-child"
  observer.event(event("session.created", { sessionID: child, parentID: root }))
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.movedAway(child)
  observer.idle(root, [{ sessionID: root, childSessionID: child, kind: "subagent" }])
  expect(observer.outstanding(root)).toBe(true)
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "moved-result", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "moved-result" }))
  expect(observer.outstanding(root)).toBe(false)
})

test.each(["error", "settlement"])("a decisive %s aborts sibling standalone work waits", async outcome => {
  let heldSignal: AbortSignal | undefined
  let checked = false
  const adapter = createV2Notifier({
    session: {
      get: async ({ sessionID }: { sessionID: string }) => ({ title: "test", time: { idle: sessionID === "removed" ? 110 : undefined } }),
      list: async ({ parentID }: { parentID: string }) => ({
        data: parentID === "root" ? ["removed", "hung"].map(id => ({ id, parentID, time: { busy: 90 } })) : [],
      }),
      wait: async ({ sessionID }: { sessionID: string }, { signal }: { signal: AbortSignal }) => {
        if (sessionID === "removed") {
          checked = true
          if (outcome === "error") throw new Error("404")
          return
        }
        heldSignal = signal
        return new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }))
      },
    },
  } as never, { directory: fixture }, "terminal", { claim: async () => false }, createBackgroundObserver({ reportsDirectChildren: true }))
  writeFileSync(configPath, JSON.stringify({ deferCompleteUntilChildrenIdle: true }))
  const before = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = configPath
  try {
    const start = performance.now()
    await adapter.event({ ...event("session.execution.succeeded", { sessionID: "root" }), created: 100 })
    expect(checked).toBe(true)
    expect(heldSignal?.aborted).toBe(true)
    expect(performance.now() - start).toBeLessThan(1500)
  } finally {
    adapter.dispose()
    if (before === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = before
  }
})

test.each(["disposed", "invalidated"])("a work scan aborts when its completion is %s", async reason => {
  let readSignal: AbortSignal | undefined
  let entered!: () => void
  const reading = new Promise<void>(resolve => { entered = resolve })
  const adapter = createV2Notifier({
    session: {
      get: async () => ({ title: "test" }),
    },
    experimental: { job: { list: async () => [] } },
  } as never, { directory: fixture }, "terminal", { claim: async () => false }, undefined, undefined, (_, signal) => {
    readSignal = signal
    entered()
    return new Promise(() => {})
  })
  writeFileSync(configPath, JSON.stringify({ deferCompleteUntilChildrenIdle: true }))
  const before = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = configPath
  try {
    const pending = adapter.event(event("session.execution.succeeded", { sessionID: "root" }))
    await reading
    if (reason === "disposed") adapter.dispose()
    else adapter.enqueue(event("session.execution.started", { sessionID: "root" }))
    expect(readSignal?.aborted).toBe(true)
    await pending
  } finally {
    adapter.dispose()
    if (before === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = before
  }
})

test("a quiet family of historical descendants still completes with ordinary remote latency", async () => {
  const options = {
    deferChildren: true,
    authoritative: {
      readActive: () => [],
      listChildren: (parentID: string) => ({ data: parentID === "root"
        ? Array.from({ length: 40 }, (_, index) => ({ id: `idle-${index}`, parentID }))
        : [] }),
      lastSettled: () => 90,
      responseDelay: 0,
    },
  }
  const success = () => ({ ...event("session.execution.succeeded", { sessionID: "root" }), created: 100 })
  expect(await exercise([success()], options)).toHaveLength(1)
  options.authoritative.responseDelay = 150
  expect(await exercise([success()], options)).toHaveLength(1)
})

test("activity readback preserves a grandchild whose intermediate parent was just removed", async () => {
  const input = [
    event("session.created", { sessionID: "middle", parentID: "root" }),
    event("session.created", { sessionID: "leaf", parentID: "middle" }),
    event("session.execution.started", { sessionID: "leaf" }),
    event("session.deleted", { sessionID: "middle" }),
  ]
  const success = () => event("session.execution.succeeded", { sessionID: "root" })
  let active = ["leaf"]
  const options = {
    deferChildren: true,
    authoritative: { readActive: () => active, listChildren: () => ({ data: [] }) },
  }
  expect(await exercise([...input, success()], options)).toHaveLength(0)
  active = []
  expect(await exercise([...input, success()], options)).toHaveLength(1)
})

test.each([50, 500])("an undelivered confirmed Job keeps its own obligation when the child settles again at %d ms", next => {
  let now = 0
  const observer = createBackgroundObserver({ reportsDirectChildren: true, now: () => now, uncertaintyTimeout: () => 100 })
  const root = "root"
  const child = "child"
  const work = [{ sessionID: root, childSessionID: child, kind: "subagent" }]
  observer.event(event("session.created", { sessionID: child, parentID: root }))
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.idle(root, work)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  now = next
  observer.event(event("session.execution.started", { sessionID: child }))
  observer.idle(root, work)
  observer.event(event("session.execution.succeeded", { sessionID: child }))
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "only-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.delivered", { sessionID: root, inboxID: "only-report" }))
  expect(observer.outstanding(root)).toBe(true)
  now = Math.max(now, 101)
  expect(observer.outstanding(root)).toBe(true)
  observer.event(event("session.inbox.enqueued", {
    sessionID: root, inboxID: "older-report", item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: child } } },
  }))
  observer.event(event("session.inbox.cancelled", { sessionID: root, inboxID: "older-report" }))
  expect(observer.outstanding(root)).toBe(false)
})

test("synthetic, compaction and move inbox items are not user messages", async () => {
  const inputs = ["synthetic", "compaction", "move"].flatMap(type => [
    event("session.inbox.enqueued", { sessionID: "s", inboxID: type, item: { type, payload: {} } }),
    event("session.inbox.delivered", { sessionID: "s", inboxID: type }),
  ])
  expect((await exercise(inputs)).length).toBe(0)
})

test("one-second session is suppressed by minDuration=60", async () => {
  expect((await exercise([event("session.execution.succeeded", { sessionID: "duration" })], { minDuration: 60 })).length).toBe(0)
})

test("deleted and unknown children never become top-level completion", async () => {
  expect((await exercise([
    event("session.created", { sessionID: "child", parentID: "parent" }),
    event("session.deleted", { sessionID: "child" }),
    event("session.execution.succeeded", { sessionID: "child" }),
    event("session.execution.succeeded", { sessionID: "unknown" }),
  ], { missing: true })).length).toBe(0)
})

test("v2 execution success produces one completion, even on replay", async () => {
  const finished = event("session.execution.succeeded", { sessionID: "success" })
  expect((await exercise([event("session.execution.started", { sessionID: "success" }), finished, finished])).length).toBe(1)
})

test("terminals on one machine alert once for an event they both receive", async () => {
  const finished = event("session.execution.succeeded", { sessionID: "shared" })
  expect((await exercise([finished])).length).toBe(1)
  expect((await exercise([finished])).length).toBe(0)
  expect((await exercise([event("session.execution.succeeded", { sessionID: "shared" })])).length).toBe(1)
})

test("a terminal with nothing to deliver leaves the event to another terminal", async () => {
  const finished = event("session.execution.succeeded", { sessionID: "silent-first" })
  expect((await exercise([finished], { silent: true })).length).toBe(0)
  expect((await exercise([finished])).length).toBe(1)
})

test("a new run clears error suppression", async () => {
  const output = await exercise([
    event("session.execution.failed", { sessionID: "retry", error: {} }),
    event("session.execution.started", { sessionID: "retry" }),
    event("session.execution.succeeded", { sessionID: "retry" }),
  ])
  expect(output.filter(w => w.includes("PROBE_DONE")).length).toBe(1)
  expect(output.filter(w => w.includes("PROBE_ERROR")).length).toBe(1)
})

test("question forms notify once; other forms and plan switches do not", async () => {
  const question = event("form.created", { form: { id: "question", sessionID: "s", metadata: { kind: "question" } } })
  expect((await exercise([
    question, question,
    event("form.created", { form: { id: "other", sessionID: "s" } }),
    event("session.agent.selected", { sessionID: "s", previous: "plan", agent: "build" }),
  ])).length).toBe(1)
})

test.each(["form.replied", "form.cancelled"])("%s retires a question while its terminal claim waits", async type => {
  const sessionID = "question-session", id = "closing-question"
  let close!: () => void
  let claimed!: () => void
  const waiting = new Promise<void>(resolve => { claimed = resolve })
  const alerts = await exercise(async notifier => {
    close = () => notifier.enqueue(event(type, { sessionID, id }))
    notifier.enqueue(event("form.created", { form: { sessionID, id, metadata: { kind: "question" } } }))
    await waiting
    await Bun.sleep(35)
  }, { onClaim: () => { close(); claimed() } })
  expect(alerts).toHaveLength(0)
})

test("a native permission reply invalidates terminal delivery during its claim", async () => {
  const sessionID = "permission-reply-claim", id = "test-permission"
  let reply!: () => void
  let claimed!: () => void
  const waiting = new Promise<void>(resolve => { claimed = resolve })
  const alerts = await exercise(async notifier => {
    reply = () => notifier.enqueue(event("permission.replied", { sessionID, requestID: id, reply: "once" }))
    notifier.enqueue(event("permission.asked", { sessionID, id }))
    await waiting
    await Bun.sleep(35)
  }, { permissionPending: true, onClaim: () => { reply(); claimed() } })
  expect(alerts).toHaveLength(0)
})

test("a native question may reuse a settled form ID", async () => {
  const sessionID = "reused-form-session", id = "reused-form"
  const question = (created: number) => ({ ...event("form.created", { form: { sessionID, id, metadata: { kind: "question" } } }), created })
  const alerts = await exercise(async notifier => {
    notifier.enqueue({ ...event("form.cancelled", { sessionID, id }), created: 2_000 })
    notifier.enqueue(question(1_000))
    notifier.enqueue(question(2_000))
    await Bun.sleep(35)
  })
  expect(alerts).toHaveLength(1)
})

test("closing a form keeps its question-opened server command", async () => {
  const sessionID = "closed-command-session", id = "closed-command-form"
  writeFileSync(commandLog, "")
  await exercise(async notifier => {
    notifier.enqueue(event("form.created", { form: { sessionID, id, metadata: { kind: "question" } } }))
    notifier.enqueue(event("form.replied", { sessionID, id }))
    await waitForCommands(1)
  }, { delivery: "command", command: true })
  expect(readFileSync(commandLog, "utf8")).toBe("question\n")
})

test("a subagent already running when the plugin loads is excluded from user messages", async () => {
  expect((await exercise([event("session.inbox.enqueued", { sessionID: "child", inboxID: "u1", item: { type: "user" } })], { child: true })).length).toBe(0)
})

test("resolved permissions and non-user interruptions stay silent", async () => {
  expect((await exercise([
    event("permission.asked", { id: "approved", sessionID: "s" }),
    ...["shutdown", "superseded", "inactivity"].map(reason => event("session.execution.interrupted", { sessionID: "s", reason })),
  ])).length).toBe(0)
})

test("events from a different project do not notify", async () => {
  const input = { ...event("session.execution.succeeded", { sessionID: "s" }), location: { directory: "/other-project" } }
  expect((await exercise([input])).length).toBe(0)
})

test("location-less events from another project's session do not notify", async () => {
  const finished = event("session.execution.succeeded", { sessionID: "foreign" })
  expect((await exercise([finished], { sessionLocation: { directory: "/other-project" } })).length).toBe(0)
  expect((await exercise([finished], { sessionLocation: { directory: fixture, workspaceID: "other" } })).length).toBe(0)
})

test("location-less events from this project's session notify", async () => {
  const finished = event("session.execution.succeeded", { sessionID: "own" })
  expect((await exercise([finished], { sessionLocation: { directory: fixture } })).length).toBe(1)
})

test("deleting a foreign session drops its cached ownership", async () => {
  let lookups = 0
  const renamed = () => event("session.renamed", { sessionID: "foreign", title: "x" })
  await exercise([renamed(), event("session.deleted", { sessionID: "foreign" }), renamed()], {
    sessionLocation: { directory: "/other-project" },
    onGet: () => lookups++,
  })
  expect(lookups).toBe(2)
})

test("a failed session lookup fails open", async () => {
  const question = event("form.created", { form: { id: "lookup", sessionID: "gone", metadata: { kind: "question" } } })
  expect((await exercise([question], { missing: true })).length).toBe(1)
})

async function waitForCommands(count: number) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (existsSync(commandLog) && readFileSync(commandLog, "utf8").trim().split("\n").filter(Boolean).length >= count) return
    await Bun.sleep(10)
  }
  throw new Error("custom command did not finish")
}

test("server owns commands; terminal owns popups; both honor duration", async () => {
  writeFileSync(commandLog, "")
  const completed = event("session.execution.succeeded", { sessionID: "ownership" })
  expect((await exercise([completed], { command: true, delivery: "command" })).length).toBe(0)
  await waitForCommands(1)
  expect((await exercise([completed], { command: true, delivery: "terminal" })).length).toBe(1)
  await exercise([event("session.execution.failed", { sessionID: "quick", error: {} })], { command: true, commandMinDuration: 60, delivery: "command" })
  await Bun.sleep(50)
  expect(readFileSync(commandLog, "utf8")).toBe("complete\n")
})


test("v2 subagent names come from session.agent, not the title", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "agent" })], { child: true, agentName: "explore" })
  expect(output.length).toBe(1)
  expect(output[0]).toContain("PROBE_AGENT explore")
})

test("another workspace sharing the directory does not notify", async () => {
  const input = { ...event("session.execution.succeeded", { sessionID: "s" }), location: { directory: fixture, workspaceID: "other" } }
  expect((await exercise([input])).length).toBe(0)
})

test("user-message commands preserve their duration-independent behavior", async () => {
  writeFileSync(commandLog, "")
  await exercise([event("session.inbox.enqueued", { sessionID: "s", inboxID: "message", item: { type: "user" } })], { delivery: "command", command: true, commandMinDuration: 60 })
  await waitForCommands(1)
  expect(readFileSync(commandLog, "utf8")).toBe("user_message\n")
})

test("V2 checks running background Jobs even when it missed child session events", async () => {
  const checked: string[] = []
  const output = await exercise([
    event("session.execution.started", { sessionID: "parent" }),
    event("session.execution.succeeded", { sessionID: "parent" }),
  ], {
    deferChildren: true,
    authoritative: { jobs: [{ status: "running" }], onCheck: id => checked.push(id) },
  })
  expect(checked).toEqual(["parent"])
  expect(output).toHaveLength(0)
})

test("V2 waits when a completed child's synthetic report is still queued for its parent", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "parent" })], {
    deferChildren: true,
    authoritative: {
      jobs: [],
      inbox: [{ type: "synthetic", payload: { metadata: { source: "subagent" } } }],
    },
  })
  expect(output).toHaveLength(0)
})

test("V2 reads descendant inboxes after the outbox is empty, even without child events", async () => {
  const checked: string[] = []
  let queued: readonly InboxItem[] = [{ type: "synthetic", payload: { metadata: { source: "subagent" } } }]
  const options = {
    deferChildren: true,
    authoritative: {
      onCheck: () => checked.push("jobs"),
      listChildren: (parentID: string, cursor?: string) => {
        if (parentID === "root") return cursor
          ? { data: [{ id: "middle", parentID }] }
          : { data: [{ id: "other-child", parentID }], cursor: { next: "second-page" } }
        if (parentID === "middle") return { data: [{ id: "report-owner", parentID }] }
        return { data: [] }
      },
      readInbox: (sessionID: string) => {
        checked.push(sessionID)
        return sessionID === "report-owner" ? queued : []
      },
    },
  }
  const success = () => event("session.execution.succeeded", { sessionID: "root" })
  expect(await exercise([success()], options)).toHaveLength(0)
  expect(checked[0]).toBe("jobs")
  expect(checked).toContain("report-owner")
  queued = [{ type: "synthetic", payload: { metadata: { source: "unrelated-plugin" } } }]
  expect(await exercise([success()], options)).toHaveLength(1)
})

test("V2 treats a non-advancing child list cursor as unknown", async () => {
  const output = await exercise([{ ...event("session.execution.succeeded", { sessionID: "root" }), created: 100 }], {
    deferChildren: true,
    authoritative: {
      listChildren: () => ({ data: [], cursor: { next: "same-page" } }),
      waitForServerSession: async () => {},
    },
  })
  expect(output).toHaveLength(0)
})

test.each(["unavailable", "failed"] as const)("native fallback is used only when metadata RPC is %s", async rpcStatus => {
  let inboxReads = 0
  const success = event("session.execution.succeeded", { sessionID: `legacy-${rpcStatus}` })
  const alerts = await exercise([success], {
    deferChildren: true, rpcStatus,
    authoritative: { listChildren: () => ({ data: [] }), onInbox: () => { inboxReads++ } },
  })
  const expected = rpcStatus === "unavailable" ? 1 : 0
  expect(inboxReads).toBe(expected)
  expect(alerts).toHaveLength(expected)
})

test("the native fallback completes a quiet historical family at ordinary remote latency", async () => {
  const success = { ...event("session.execution.succeeded", { sessionID: "legacy-root" }), created: 100 }
  const alerts = await exercise([success], {
    deferChildren: true, rpcStatus: "unavailable",
    authoritative: {
      readActive: () => [], responseDelay: 150, lastSettled: () => 90,
      listChildren: parentID => ({ data: parentID === "legacy-root"
        ? Array.from({ length: 24 }, (_, index) => ({ id: `legacy-child-${index}`, parentID })) : [] }),
    },
  })
  expect(alerts).toHaveLength(1)
})

test.each([false, true])("a second unavailable metadata read falls back to the native inbox (report: %s)", async report => {
  const root = "second-read-root"
  const alerts = await exercise([event("session.execution.succeeded", { sessionID: root })], {
    deferChildren: true, rpcStatus: "second-read-unavailable",
    authoritative: {
      listChildren: parentID => ({ data: parentID === root ? [{ id: "child", parentID }] : [] }),
      nativeInboxResponse: report ? [{ id: "queued", type: "synthetic", payload: { metadata: { source: "subagent" } } }] : [],
    },
  })
  expect(alerts).toHaveLength(report ? 0 : 1)
})

test.each([["unavailable"], [[null]], [[{ id: "queued", type: "synthetic" }]]] as const)(
  "malformed native inbox metadata keeps completion deferred (%j)", async nativeInboxResponse => {
    const alerts = await exercise([event("session.execution.succeeded", { sessionID: "invalid-native-root" })], {
      deferChildren: true, rpcStatus: "unavailable", authoritative: { nativeInboxResponse },
    })
    expect(alerts).toHaveLength(0)
  },
)

test("a native inbox read uses the full work-check deadline", async () => {
  const success = event("session.execution.succeeded", { sessionID: "legacy-slow-root" })
  const alerts = await exercise([success], {
    deferChildren: true, rpcStatus: "unavailable", authoritative: { nativeInboxDelay: 900 },
  })
  expect(alerts).toHaveLength(1)
})

test("V2 completes a childless session without a second active lookup", async () => {
  let activityReads = 0
  const output = await exercise([event("session.execution.succeeded", { sessionID: "childless-root" })], {
    deferChildren: true,
    authoritative: {
      readActive: () => { activityReads++; return ["unrelated"] },
      listChildren: () => ({ data: [] }),
    },
  })
  expect(output).toHaveLength(1)
  expect(activityReads).toBe(1)
})

test("V2 checks active descendants without treating the root or unrelated sessions as outstanding", async () => {
  let active = ["root", "unrelated", "last-child"]
  const checks: string[] = []
  const options = {
    deferChildren: true,
    authoritative: {
      readActive: () => { checks.push("active"); return active },
      onCheck: () => checks.push("jobs"),
      listChildren: (parentID: string) => {
        const id = parentID === "root" ? "middle" : parentID === "middle" ? "last-child" : undefined
        return { data: id ? [{ id, parentID }] : [] }
      },
    },
  }
  const success = () => event("session.execution.succeeded", { sessionID: "root" })
  expect(await exercise([success()], options)).toHaveLength(0)
  expect(checks).toEqual(["active", "jobs"])
  active = ["root", "unrelated"]
  expect(await exercise([success()], options)).toHaveLength(1)
})

test("V2 rechecks activity if a descendant can take its report during the inbox scan", async () => {
  let processing = false
  const output = await exercise([event("session.execution.succeeded", { sessionID: "root" })], {
    deferChildren: true,
    authoritative: {
      readActive: () => processing ? ["middle"] : [],
      listChildren: parentID => ({ data: parentID === "root" ? [{ id: "middle", parentID }] : [] }),
      readInbox: sessionID => {
        if (sessionID === "root") processing = true
        return []
      },
    },
  })
  expect(output).toHaveLength(0)
})

test("V2 holds completion if a descendant finishes processing entirely between activity snapshots", async () => {
  let middleIdle = 90
  const report = { type: "synthetic", payload: { metadata: { source: "subagent" } } }
  let ownReports: readonly InboxItem[] = []
  const output = await exercise([
    { ...event("session.execution.succeeded", { sessionID: "root" }), created: 100 },
  ], {
    deferChildren: true,
    authoritative: {
      readActive: () => [],
      listChildren: parentID => ({ data: parentID === "root" ? [{ id: "middle", parentID }] : [] }),
      lastSettled: sessionID => sessionID === "middle" ? middleIdle : 100,
      readInbox: sessionID => {
        if (sessionID === "root") return ownReports
        middleIdle = 110
        ownReports = [report]
        return []
      },
    },
  })
  expect(output).toHaveLength(0)
})

test("V2 waits for a later root success when the child and root settle in the same millisecond", async () => {
  const options = {
    deferChildren: true,
    authoritative: {
      listChildren: (parentID: string) => ({ data: parentID === "root" ? [{ id: "child", parentID }] : [] }),
      lastSettled: (sessionID: string) => sessionID === "child" ? 100 : undefined,
    },
  }
  const success = (created: number) => ({ ...event("session.execution.succeeded", { sessionID: "root" }), created })
  expect(await exercise([success(100)], options)).toHaveLength(0)
  expect(await exercise([success(101)], options)).toHaveLength(1)
})

test("V2 server distinguishes a child that settles during its lookup from a stale busy timestamp", async () => {
  let childIdle = 80
  let settling = true
  const checked: string[] = []
  const options = {
    deferChildren: true,
    authoritative: {
      listChildren: (parentID: string) => ({
        data: parentID === "root" ? [{ id: "child", parentID, time: { busy: 90, idle: childIdle } }] : [],
      }),
      lastSettled: (sessionID: string) => sessionID === "child" ? childIdle : undefined,
      waitForServerSession: async (sessionID: string) => {
        checked.push(sessionID)
        if (settling) childIdle = 110
      },
    },
  }
  const success = () => ({ ...event("session.execution.succeeded", { sessionID: "root" }), created: 100 })
  expect(await exercise([success()], options)).toHaveLength(0)
  childIdle = 80
  settling = false
  expect(await exercise([success()], options)).toHaveLength(1)
  expect(checked).toEqual(["child", "child"])
})

test("a server wait that ignores cancellation is shared across successive root turns", async () => {
  let waits = 0
  let rootChecks = 0
  let nativeSignal: AbortSignal | undefined
  const root = "root"
  const success = () => ({ ...event("session.execution.succeeded", { sessionID: root }), created: 120 })
  await exercise([success(), event("session.execution.started", { sessionID: root }), success()], {
    deferChildren: true,
    authoritative: {
      listChildren: parentID => {
        if (parentID === "deleted") throw new Error("Session removed")
        if (parentID === root && ++rootChecks === 2) expect(nativeSignal?.aborted).toBe(true)
        return { data: parentID === root ? [
          { id: "running", parentID, time: { busy: 90 } }, { id: "deleted", parentID },
        ] : [] }
      },
      waitForServerSession: (_sessionID, signal) => {
        waits++
        nativeSignal = signal
        return new Promise(() => {})
      },
    },
  })
  expect(waits).toBe(1)
})

test("V2 fails closed rather than sending a false completion when Job or inbox API fails", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "parent" })], {
    deferChildren: true,
    authoritative: { fail: true },
  })
  expect(output).toHaveLength(0)
})

test("server completions skip Job and inbox lookups when their command is disabled", async () => {
  let lookups = 0
  await exercise([event("session.execution.succeeded", { sessionID: "parent" })], {
    delivery: "command",
    deferChildren: true,
    authoritative: { onCheck: () => { lookups++ }, onInbox: () => { lookups++ } },
  })
  expect(lookups).toBe(0)
})

test.each(["minDuration", "commandMinDuration"] as const)("%s suppresses short server turns before work readback", async required => {
  let lookups = 0
  const options = {
    delivery: "command" as const, command: true, deferChildren: true, [required]: 60,
    authoritative: { jobs: [{ kind: "subagent", status: "running" }], onCheck: () => { lookups++ } },
  }
  const success = () => event("session.execution.succeeded", { sessionID: "parent" })
  await exercise([success()], options)
  expect(lookups).toBe(0)
  await exercise([{ ...success(), created: Date.now() - 119_900 }], { ...options, messageAge: 120_000 })
  expect(lookups).toBe(0)
  await exercise([success()], { ...options, unknownElapsed: true })
  expect(lookups).toBe(1)
})

test("V2 completes normally when its running Jobs and pending subagent reports are empty", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "parent" })], {
    deferChildren: true,
    authoritative: { jobs: [], inbox: [] },
  })
  expect(output.length).toBe(1)
  expect(output[0]).toContain("PROBE_DONE")
})

test("V2 holds completion for a settled but unacknowledged Subagent outbox record", async () => {
  const inputs: { sessionID: string; descendants?: boolean; status?: string }[] = []
  const output = await exercise([event("session.execution.succeeded", { sessionID: "unacknowledged" })], {
    deferChildren: true,
    authoritative: {
      jobs: [{ kind: "subagent", status: "completed" }],
      inbox: [],
      onCheckInput: input => inputs.push(input),
    },
  })
  expect(output).toHaveLength(0)
  // Filtering to "running" here would hide the completed durable outbox row.
  expect(inputs).toEqual([{ sessionID: "unacknowledged", descendants: true }])
})

test("V2 also holds failed/cancelled Subagent outbox reports awaiting delivery", async () => {
  for (const status of ["error", "cancelled"]) {
    const output = await exercise([event("session.execution.succeeded", { sessionID: "outbox-" + status })], {
      deferChildren: true,
      authoritative: { jobs: [{ kind: "subagent", status }], inbox: [] },
    })
    expect(output).toHaveLength(0)
  }
})

test("V2 ignores an unrelated completed non-Subagent Job", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "shell-complete" })], {
    deferChildren: true,
    authoritative: { jobs: [{ kind: "shell", status: "completed" }], inbox: [] },
  })
  expect(output).toHaveLength(1)
})

test("V2 reads Job registry before Inbox to prevent cross-snapshot false completion", async () => {
  const order: string[] = []
  const authoritative = {
    jobs: [] as readonly object[],
    inbox: [] as { type: string; payload?: { metadata?: { source?: string } } }[],
    onCheck: () => {
      order.push("jobs")
      // A synthetic report enters the inbox after the Job snapshot.
      authoritative.inbox = [{ type: "synthetic", payload: { metadata: { source: "subagent" } } }]
    },
    onInbox: () => order.push("inbox"),
  }
  const output = await exercise([event("session.execution.succeeded", { sessionID: "inbox-race" })], {
    deferChildren: true, authoritative,
  })
  expect(order).toEqual(["jobs", "inbox"])
  expect(output).toHaveLength(0)
})

test("foreign session.created is not delivered when the ownership lookup is unavailable", async () => {
  const input = event("session.created", {
    sessionID: "foreign-created",
    location: { directory: "/another-workspace" },
    title: "Foreign session",
  })
  // The event has no envelope, and session.get fails. data.location must win
  // over a fallback decision to announce a session from another workspace.
  const output = await exercise([input], { missing: true })
  expect(output).toHaveLength(0)
})

test("V2 completion fails closed on an unresolved session location, but questions still notify", async () => {
  const complete = event("session.execution.succeeded", { sessionID: "unknown-location" })
  const question = event("form.created", { form: { id: "ownership-unknown", sessionID: "unknown-location", metadata: { kind: "question" } } })
  const output = await exercise([complete, question], { missing: true })
  expect(output.filter(value => value.includes("PROBE_DONE"))).toHaveLength(0)
  expect(output.filter(value => value.includes("PROBE_QUESTION"))).toHaveLength(1)
})

test("an unresponsive session ownership API cannot indefinitely block completion handling", async () => {
  const output = await exercise([
    event("session.execution.succeeded", { sessionID: "unresponsive" }),
  ], { hangOwnership: true })
  // Ownership is unknown, so no speculative "complete" notification. The
  // adapter must nevertheless settle the event within its bounded deadline.
  expect(output).toHaveLength(0)
})
