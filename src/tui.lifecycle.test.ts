import { afterAll, expect, test } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import type { OpenCodeEvent } from "@opencode/client"
import type { Plugin } from "@opencode/plugin/tui"
import tui from "./tui"
import { OUTSTANDING_WORK_TIMEOUT_MS } from "./v2"

const root = mkdtempSync(join(tmpdir(), "notifier-lifecycle-test-"))
afterAll(() => rmSync(root, { recursive: true, force: true }))

let nextID = 0
async function until(predicate: () => boolean, explanation: string, attempts = 250) {
  for (let attempt = 0; attempt < attempts && !predicate(); attempt++) await Bun.sleep(5)
  expect(predicate(), explanation).toBe(true)
}

async function harness(
  label: string,
  jobs: (sessionID: string) => Promise<readonly { kind: string; status: string }[]>,
  onGet?: (sessionID: string) => void,
) {
  const path = join(root, `config-${label}.json`)
  writeFileSync(path, JSON.stringify({
    deferCompleteUntilChildrenIdle: true,
    showSessionTitle: true,
    suppressWhenFocused: false,
    notificationSystem: "ghostty",
    sound: false,
    events: {
      client_connected: false, subagent_complete: false, error: false,
      user_cancelled: false, user_message: false, session_started: false,
    },
    messages: { complete: `LIFECYCLE_${label} {sessionTitle}` },
  }))
  const old = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = path
  const write = process.stdout.write
  const writes: string[] = []
  process.stdout.write = ((chunk: unknown, ...args: unknown[]) => {
    writes.push(String(chunk))
    const callback = args.find(arg => typeof arg === "function") as (() => void) | undefined
    callback?.()
    return true
  }) as typeof process.stdout.write
  let listener: Parameters<Plugin.Context["data"]["listen"]>[0] | undefined
  const location = { directory: root }
  try {
    const dispose = await tui.setup({
      location,
      client: {
        session: {
          get: async ({ sessionID }: { sessionID: string }) => {
            onGet?.(sessionID)
            return { id: sessionID, title: sessionID, location }
          },
          inbox: { list: async () => [] },
        },
        experimental: { job: { list: async ({ sessionID }: { sessionID: string }) => jobs(sessionID) } },
        permission: { list: async () => [] },
      },
      data: { listen(callback: typeof listener) { listener = callback; return () => { listener = undefined } } },
    } as never)
    return {
      writes,
      emit(type: string, sessionID: string, data: Record<string, unknown> = {}, id?: string, envelope = true) {
        listener?.({ details: {
          id: id ?? `lifecycle-${label}-${++nextID}`, type,
          ...(envelope ? { location } : {}),
          data: { sessionID, ...data },
        } as OpenCodeEvent })
      },
      async close() {
        await dispose?.()
        process.stdout.write = write
        if (old === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
        else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = old
      },
    }
  } catch (error) {
    process.stdout.write = write
    if (old === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = old
    throw error
  }
}

test("queueing input for a running child does not release its parent's completion", async () => {
  const client = await harness("queued-child-input", async () => [])
  try {
    client.emit("session.created", "child", { location: { directory: root }, parentID: "parent" })
    client.emit("session.execution.started", "child")
    client.emit("session.inbox.enqueued", "child", { inboxID: "more-input", item: { type: "user", payload: { text: "keep working" } } })
    client.emit("session.execution.succeeded", "parent")
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_queued-child-input"))).toBe(false)
    client.emit("session.execution.succeeded", "child")
    client.emit("session.execution.started", "parent")
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_queued-child-input parent")), "Final parent completion did not arrive")
  } finally {
    await client.close()
  }
})

for (const terminal of ["session.deleted", "session.execution.failed", "session.execution.interrupted"]) {
  test(`a ${terminal} received while Job lookup is pending invalidates stale completion`, async () => {
    let entered = false
    let resolveJob!: (value: readonly { kind: string; status: string }[]) => void
    const blocked = new Promise<readonly { kind: string; status: string }[]>(resolve => { resolveJob = resolve })
    const client = await harness(terminal.replace(/\W/g, "-"), async () => {
      entered = true
      return blocked
    })
    try {
      client.emit("session.execution.succeeded", "parent")
      await until(() => entered, "Completion did not reach its Job lookup")
      client.emit(terminal, "parent", terminal === "session.execution.interrupted" ? { reason: "user" } : {})
      resolveJob([])
      await Bun.sleep(120)
      expect(client.writes.some(write => write.includes("LIFECYCLE_"))).toBe(false)
    } finally {
      resolveJob([])
      await client.close()
    }
  })
}

test("a synthetic Subagent report instantly cancels an in-flight parent completion", async () => {
  let entered = false
  let resolveJob!: (value: readonly { kind: string; status: string }[]) => void
  const blocked = new Promise<readonly { kind: string; status: string }[]>(resolve => { resolveJob = resolve })
  const client = await harness("synthetic-report", async () => {
    entered = true
    return blocked
  })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => entered, "Completion did not reach its Job lookup")
    client.emit("session.inbox.enqueued", "parent", {
      inboxID: "result",
      item: {
        type: "synthetic",
        delivery: "steer",
        payload: { text: "Done", metadata: { source: "subagent" } },
      },
    })
    resolveJob([])
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_"))).toBe(false)
    // Once OpenCode restarts and successfully completes the parent turn, it
    // should be allowed to alert exactly once.
    client.emit("session.execution.started", "parent")
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_")), "Final parent completion missing")
    expect(client.writes.filter(write => write.includes("LIFECYCLE_"))).toHaveLength(1)
  } finally {
    resolveJob([])
    await client.close()
  }
})

test("a stalled Job lookup in one session does not block another session's completion", async () => {
  let entered = false
  let release!: (value: readonly { kind: string; status: string }[]) => void
  const blocked = new Promise<readonly { kind: string; status: string }[]>(resolve => { release = resolve })
  const client = await harness("session-isolation", sessionID => {
    if (sessionID !== "slow") return Promise.resolve([])
    entered = true
    return blocked
  })
  try {
    client.emit("session.execution.succeeded", "slow")
    await until(() => entered, "Slow Job query did not start")
    client.emit("session.execution.succeeded", "fast")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_session-isolation fast")),
      "Unrelated session was blocked behind slow Job lookup", 200)
    expect(client.writes.some(write => write.includes("LIFECYCLE_session-isolation slow"))).toBe(false)
  } finally {
    release([])
    await client.close()
  }
})

test("a timed-out background Job check is treated as unknown, never as completed", async () => {
  let entered = false
  const client = await harness("lookup-timeout", async () => {
    entered = true
    return new Promise<never>(() => {})
  })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => entered, "Timeout test never started lookup")
    await Bun.sleep(OUTSTANDING_WORK_TIMEOUT_MS + 150)
    expect(client.writes.some(write => write.includes("LIFECYCLE_"))).toBe(false)
    // The timeout must release its per-session queue so subsequent changes
    // can still be processed.
    client.emit("session.execution.started", "parent")
    await Bun.sleep(50)
  } finally {
    await client.close()
  }
})

test("high-volume text and reasoning deltas do not perform per-event session lookups", async () => {
  let gets = 0
  const client = await harness("stream-filter", async () => [], () => { gets++ })
  try {
    for (let i = 0; i < 2000; i++) {
      client.emit("session.text.delta", "parent", { delta: "x" })
      client.emit("session.reasoning.delta", "parent", { delta: "x" })
    }
    await Bun.sleep(30)
    expect(gets).toBe(0)
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_stream-filter parent")),
      "Relevant completion was delayed by ignored stream events")
  } finally {
    await client.close()
  }
})

test("replayed execution.started event does not invalidate a later legitimate completion", async () => {
  const client = await harness("started-replay", async () => [])
  try {
    const replayedStartID = "replayed-started-id"
    client.emit("session.execution.started", "parent", {}, replayedStartID)
    client.emit("session.execution.succeeded", "parent")
    // Durable stream replay of the exact same started event, after the success
    // has already been received but before it has been processed.
    client.emit("session.execution.started", "parent", {}, replayedStartID)
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_started-replay parent")),
      "Duplicate start improperly invalidated the legitimate completion")
    expect(client.writes.filter(write => write.includes("LIFECYCLE_started-replay parent"))).toHaveLength(1)
  } finally {
    await client.close()
  }
})

test("moving a session out of a workspace invalidates an in-flight completion", async () => {
  let entered = false
  let release!: (value: readonly { kind: string; status: string }[]) => void
  const blocked = new Promise<readonly { kind: string; status: string }[]>(resolve => { release = resolve })
  const client = await harness("workspace-move", async () => {
    entered = true
    return blocked
  })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => entered, "Move test never started lookup")
    // OpenCode routes session.moved to both locations. Its public event has
    // the destination in data.location but often no envelope location.
    client.emit("session.moved", "parent", { location: { directory: "/new-workspace" } })
    release([])
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_workspace-move parent"))).toBe(false)
  } finally {
    release([])
    await client.close()
  }
})

test("a location-less native child-created event immediately protects a pending parent completion", async () => {
  let entered = false
  let release!: (value: readonly { kind: string; status: string }[]) => void
  const blocked = new Promise<readonly { kind: string; status: string }[]>(resolve => { release = resolve })
  const client = await harness("child-created", async () => {
    entered = true
    return blocked
  })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => entered, "Parent Job lookup never started")
    // OpenCode's native child-created event has data.location, not necessarily
    // an envelope location. It must be tracked before its async event runs.
    client.emit("session.created", "child", {
      parentID: "parent",
      location: { directory: root },
      title: "child worker",
    }, undefined, false)
    release([])
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_child-created parent"))).toBe(false)
    // The child can finish, then the resumed parent may give its final answer.
    client.emit("session.execution.succeeded", "child")
    client.emit("session.execution.started", "parent")
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_child-created parent")),
      "Final parent completion missing after native child")
  } finally {
    release([])
    await client.close()
  }
})
