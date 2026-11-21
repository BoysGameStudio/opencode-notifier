import { afterAll, test, expect } from "bun:test"
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync } from "fs"
import { join } from "path"
import { tmpdir } from "os"
import { createV2Notifier, setupV2, OUTSTANDING_WORK_TIMEOUT_MS } from "./v2"
import { createBackgroundObserver } from "./v2-background"
import type { Delivery } from "./notifier"
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
  const cleanup = await setupV2({
    location,
    session: { get: async ({ sessionID }: { sessionID: string }) => ({ location, title: sessionID, ...(sessionID === "child" ? { parentID: "parent" } : {}) }) },
    event: { subscribe: () => stream.readable },
  } as never)
  const emit = (type: string, sessionID: string, data = {}) => writer.write(event(`session.${type}`, { sessionID, ...data }))
  const tool = (id: string) => ({ assistantMessageID: "message", id, executed: true })
  const report = (inboxID: string) => emit("inbox.enqueued", "parent", {
    inboxID, item: { type: "synthetic", payload: { metadata: { source: "subagent", childID: "child" } } },
  })
  const commands = () => existsSync(log) ? readFileSync(log, "utf8").trim().split("\n") : []
  try {
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
    for (let n = 0; n < 100 && commands().length < 2; n++) await Bun.sleep(10)
    expect(commands().sort()).toEqual(["complete:ordinary", "complete:parent"])
  } finally {
    await writer.close()
    await cleanup?.()
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

async function exercise(events: OpenCodeEvent[], options: {
  minDuration?: number
  commandMinDuration?: number
  missing?: boolean
  hangOwnership?: boolean
  child?: boolean
  delivery?: Delivery
  command?: boolean
  messageAge?: number
  agentName?: string
  sessionLocation?: { directory: string; workspaceID?: string }
  onGet?: () => void
  silent?: boolean
  deferChildren?: boolean
  authoritative?: {
    jobs?: readonly object[]
    inbox?: readonly { type: string; payload?: { metadata?: { source?: string } } }[]
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
    messages: { user_message: "PROBE_USER {turn}", complete: "PROBE_DONE {turn}", error: "PROBE_ERROR {turn}", question: "PROBE_QUESTION {turn}", subagent_complete: "PROBE_AGENT {agentName} {turn}" },
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
  const notifier = createV2Notifier({
    session: {
      get: async () => {
        options.onGet?.()
        if (options.hangOwnership) return new Promise<never>(() => {})
        if (options.missing) throw new Error("Session not found")
        return { id: "session", title: "test", location: options.sessionLocation, ...(options.child ? { parentID: "parent", agent: options.agentName } : {}) }
      },
      context: async () => [{ id: "message", type: "user", text: "hello", time: { created: Date.now() - (options.messageAge ?? 1000) } } satisfies SessionMessageUser],
      ...(options.authoritative ? { inbox: { list: async () => {
        options.authoritative?.onInbox?.()
        if (options.authoritative?.fail) throw new Error("inbox failed")
        return options.authoritative?.inbox ?? []
      } } } : {}),
    },
    permission: { list: async () => [] },
    ...(options.authoritative ? { experimental: { job: { list: async (input: { sessionID: string; descendants?: boolean; status?: string }) => {
      options.authoritative?.onCheck?.(input.sessionID)
      options.authoritative?.onCheckInput?.(input)
      if (options.authoritative?.fail) throw new Error("job failed")
      return options.authoritative?.jobs ?? []
    } } } } : {}),
  } as never, { directory: fixture }, options.delivery ?? "terminal", { claim: async (key, _sessionID, valid) => valid() && claim(key) })
  try {
    for (const input of events) await notifier.event(input)
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

test("V2 fails closed rather than sending a false completion when Job or inbox API fails", async () => {
  const output = await exercise([event("session.execution.succeeded", { sessionID: "parent" })], {
    deferChildren: true,
    authoritative: { fail: true },
  })
  expect(output).toHaveLength(0)
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
