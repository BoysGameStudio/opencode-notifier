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
type InboxItem = { id: string; type: string; payload: { metadata: { source: string } } }
async function until(predicate: () => boolean, explanation: string, attempts = 250) {
  for (let attempt = 0; attempt < attempts && !predicate(); attempt++) await Bun.sleep(5)
  expect(predicate(), explanation).toBe(true)
}

async function harness(
  label: string,
  jobs: (sessionID: string) => Promise<readonly { kind: string; status: string }[]>,
  onGet?: (sessionID: string) => void | Promise<void>,
  authoritativeChildren?: Readonly<Record<string, string>>,
  inbox: (sessionID: string, signal: AbortSignal) => readonly InboxItem[] | Promise<readonly InboxItem[]> = () => [],
  deferCompleteUntilChildrenIdle = true,
  readback?: { rpc: "absent" | "empty" | "invalid"; legacy?: typeof inbox },
) {
  const path = join(root, `config-${label}.json`)
  const config = {
    deferCompleteUntilChildrenIdle,
    showSessionTitle: true,
    suppressWhenFocused: false,
    notificationSystem: "ghostty",
    sound: false,
    events: {
      client_connected: false, subagent_complete: false, error: false,
      user_cancelled: false, user_message: false, session_started: false,
    },
    messages: { complete: `LIFECYCLE_${label} {sessionTitle} {turn}` },
  }
  writeFileSync(path, JSON.stringify(config))
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
  let location = { directory: root }
  try {
    const dispose = await tui.setup({
      get location() { return location },
      client: {
        ...(readback?.rpc === "absent" ? {} : { rpc: () => ({ inboxFamily: async ({ sessionID }: { sessionID: string }, { signal }: { signal: AbortSignal }) => {
          if (readback?.rpc === "empty") return
          if (readback?.rpc === "invalid") return { sessions: [{ id: sessionID, idle: null }],
            items: [{ id: "malformed", sessionID, report: true }] }
          const ids = new Set([sessionID])
          for (const parent of ids) {
            for (const [id, owner] of Object.entries(authoritativeChildren ?? {})) if (owner === parent) ids.add(id)
          }
          const rows = await Promise.all([...ids].map(async id => (await inbox(id, signal)).map(item => ({
            sessionID: id, id: item.id, report: item.type === "synthetic" && item.payload.metadata.source === "subagent" ? 1 : 0,
          }))))
          return { sessions: [...ids].map(id => ({ id, idle: null })), items: rows.flat() }
        } }) }),
        session: {
          get: async ({ sessionID }: { sessionID: string }) => {
            await onGet?.(sessionID)
            return { id: sessionID, title: sessionID, location, parentID: authoritativeChildren?.[sessionID] }
          },
          ...(readback?.legacy ? { inbox: { list: async ({ sessionID }: { sessionID: string }, { signal }: { signal: AbortSignal }) =>
            readback.legacy!(sessionID, signal) } } : {}),
          ...(authoritativeChildren ? {
            active: async () => ({}),
            list: async ({ parentID }: { parentID: string }) => ({
              data: Object.entries(authoritativeChildren).filter(([, parent]) => parent === parentID).map(([id]) => ({ id, parentID })),
            }),
          } : {}),
        },
        experimental: { job: { list: async ({ sessionID }: { sessionID: string }) => jobs(sessionID) } },
        permission: { list: async () => [] },
      },
      data: { listen(callback: typeof listener) { listener = callback; return () => { listener = undefined } } },
    } as never)
    return {
      writes,
      move(directory: string) { location = { directory } },
      setDeferral(enabled: boolean) {
        config.deferCompleteUntilChildrenIdle = enabled
        writeFileSync(path, JSON.stringify(config))
      },
      emit(type: string, sessionID: string, data: Record<string, unknown> = {}, id?: string, envelope = true, created?: number) {
        listener?.({ details: {
          id: id ?? `lifecycle-${label}-${++nextID}`, type,
          ...(created === undefined ? {} : { created }),
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

for (const timing of ["already-running", "starts-later"]) {
  test(`a queued parent completion stays suppressed when a child ${timing}`, async () => {
    let entered = false
    let release!: () => void
    const blocked = new Promise<void>(resolve => { release = resolve })
    const client = await harness(`queued-${timing}`, async () => [], id => {
      if (id === "parent" && !entered) {
        entered = true
        return blocked
      }
    })
    try {
      client.emit("session.created", "child", { location: { directory: root }, parentID: "parent" })
      client.emit("session.execution.started", "parent", {}, undefined, false)
      await until(() => entered, "Parent queue was not waiting on ownership")
      if (timing === "already-running") client.emit("session.execution.started", "child")
      client.emit("session.execution.succeeded", "parent")
      if (timing === "starts-later") client.emit("session.execution.started", "child")
      client.emit("session.execution.succeeded", "child")
      release()
      await Bun.sleep(120)
      expect(client.writes.some(write => write.includes(`LIFECYCLE_queued-${timing}`))).toBe(false)

      client.emit("session.execution.started", "parent")
      client.emit("session.execution.succeeded", "parent")
      await until(() => client.writes.some(write => write.includes(`LIFECYCLE_queued-${timing} parent`)), "Final parent completion did not arrive")
    } finally {
      release()
      await client.close()
    }
  })
}

test("a child moved away does not re-enter local running state through a locationless event", async () => {
  const client = await harness("child-moved-away", async () => [])
  try {
    client.emit("session.created", "child", { location: { directory: root }, parentID: "parent" })
    client.emit("session.execution.started", "child")
    client.emit("session.moved", "child", { location: { directory: "/other-workspace" } }, undefined, false)
    client.emit("session.execution.started", "child", {}, undefined, false)
    client.emit("session.execution.started", "parent")
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_child-moved-away parent")), "Foreign child continued to block the local parent")
  } finally {
    await client.close()
  }
})

test("changing terminal location blocks an in-flight alert before another event arrives", async () => {
  let entered = false
  let release!: () => void
  const lookup = new Promise<void>(resolve => { release = resolve })
  const client = await harness("location-before-claim", async () => {
    entered = true
    await lookup
    return []
  })
  try {
    client.emit("session.execution.succeeded", "old-root")
    await until(() => entered, "The terminal did not begin its completion lookup")
    client.move(join(root, "other-workspace"))
    release()
    await Bun.sleep(250)
    expect(client.writes.some(write => write.includes("old-root"))).toBe(false)
    client.emit("session.execution.succeeded", "new-root")
    await until(() => client.writes.some(write => write.includes("new-root")), "New workspace completion did not arrive")
  } finally {
    release()
    await client.close()
  }
})

test("the terminal keeps event-only completion when no inbox reader is available", async () => {
  const client = await harness("missing-inbox-api", async () => [], undefined, undefined, undefined, true, { rpc: "absent" })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_missing-inbox-api parent")), "Event-only completion did not arrive")
  } finally {
    await client.close()
  }
})

test("an unidentified delivery invalidates event-only completion while session metadata is pending", async () => {
  let entered = false
  let release!: () => void
  const lookup = new Promise<void>(resolve => { release = resolve })
  const client = await harness("event-only-delivery", async () => [], async () => {
    entered = true
    await lookup
  }, undefined, undefined, true, { rpc: "absent" })
  try {
    client.emit("session.execution.succeeded", "parent")
    await until(() => entered, "Old completion did not begin its lookup")
    client.emit("session.inbox.delivered", "parent", { inboxID: "cold-input" })
    release()
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_event-only-delivery parent"))).toBe(false)
    client.emit("session.execution.succeeded", "parent")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_event-only-delivery parent")), "Later successful turn did not notify")
  } finally {
    release()
    await client.close()
  }
})

test.each(["empty", "invalid"] as const)("an %s RPC response does not fall back to native inbox payloads", async rpc => {
  let nativeReads = 0
  const label = `${rpc}-inbox-metadata`
  const client = await harness(label, async () => [], undefined, undefined, undefined, true,
    { rpc, legacy: () => { nativeReads++; return [] } })
  try {
    client.emit("session.execution.succeeded", "parent")
    await Bun.sleep(180)
    expect(nativeReads).toBe(0)
    expect(client.writes.some(write => write.includes(`LIFECYCLE_${label}`))).toBe(false)
  } finally {
    await client.close()
  }
})

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

test("a cancelled queued prompt preserves the preceding completion when deferral is disabled", async () => {
  let entered = false
  let release!: () => void
  const lookup = new Promise<void>(resolve => { release = resolve })
  const client = await harness("default-cancelled-input", async () => [], () => {
    entered = true
    return lookup
  }, undefined, undefined, false)
  try {
    client.emit("session.execution.succeeded", "parent", {}, undefined, false)
    await until(() => entered, "Completion did not start its ownership lookup")
    client.emit("session.inbox.enqueued", "parent", { inboxID: "prompt", item: { type: "user" } })
    client.emit("session.inbox.cancelled", "parent", { inboxID: "prompt" })
    release()
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_default-cancelled-input parent")), "A cancelled prompt suppressed the preceding completion")
  } finally {
    release()
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

test("a report queued in a descendant invalidates the root's in-flight completion", async () => {
  let entered = false
  let unblock!: () => void
  const blocked = new Promise<void>(resolve => { unblock = resolve })
  const client = await harness("descendant-report", async () => {
    entered = true
    await blocked
    return []
  })
  try {
    client.emit("session.created", "middle", { parentID: "root", location: { directory: root } })
    client.emit("session.execution.succeeded", "root")
    await until(() => entered, "Root completion did not reach its lookup")
    client.emit("session.inbox.enqueued", "middle", {
      inboxID: "nested-result",
      item: { type: "synthetic", payload: { metadata: { source: "subagent" } } },
    })
    unblock()
    await Bun.sleep(120)
    expect(client.writes.some(write => write.includes("LIFECYCLE_descendant-report"))).toBe(false)
    client.emit("session.execution.started", "root")
    client.emit("session.execution.succeeded", "root")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_descendant-report root")), "Final root notification missing")
  } finally {
    unblock()
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

test("stalled native inbox reads share capacity with an unrelated family", async () => {
  let reading = 0
  let release!: () => void
  const stalled = new Promise<void>(resolve => { release = resolve })
  const children = Object.fromEntries(Array.from({ length: 8 }, (_, i) => [`busy-${i}`, "slow"]))
  const client = await harness("native-inbox-isolation", async () => [], undefined, children, undefined, true, {
    rpc: "absent",
    async legacy(sessionID) {
      if (sessionID.startsWith("busy-")) {
        reading++
        await stalled
      }
      if (sessionID === "fast") await Bun.sleep(120)
      return []
    },
  })
  try {
    client.emit("session.execution.succeeded", "slow")
    await until(() => reading >= 6, "Slow family did not begin reading its inboxes")
    client.emit("session.execution.succeeded", "fast")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_native-inbox-isolation fast")),
      "Unrelated family was blocked behind stalled inbox reads", 200)
    expect(client.writes.some(write => write.includes("LIFECYCLE_native-inbox-isolation slow"))).toBe(false)
  } finally {
    release()
    await client.close()
  }
})

test("an unobserved child terminal cannot suppress future root successes indefinitely", async () => {
  let reads = 0
  const client = await harness("audit-missed-child-terminal", async () => { reads++; return [] }, undefined, { "audit-child": "audit-root" })
  try {
    client.emit("session.created", "audit-child", { parentID: "audit-root", location: { directory: root } })
    client.emit("session.execution.started", "audit-child")
    // Simulate a stream gap that loses the child's terminal and report events.
    // The server has no jobs or inbox items by the time the root succeeds.
    client.emit("session.execution.started", "audit-root")
    client.emit("session.execution.succeeded", "audit-root")
    await Bun.sleep(180)
    expect(reads).toBeGreaterThan(0)
    expect(client.writes.some(write => write.includes("LIFECYCLE_audit-missed-child-terminal"))).toBe(true)
  } finally {
    await client.close()
  }
})

test.each([true, false])("canceling a subagent report invalidates the root success only with deferral enabled (%s)", async defer => {
  let entered = false
  let release!: () => void
  const blocked = new Promise<void>(resolve => { release = resolve })
  const label = `cancel-report-${defer}`
  const client = await harness(label, async () => {
    entered = true
    await blocked
    return []
  }, undefined, undefined, undefined, defer)
  try {
    client.emit("session.inbox.enqueued", "audit-root", {
      inboxID: "audit-result",
      item: { type: "synthetic", delivery: "queue", payload: { metadata: { source: "subagent" } } },
    })
    client.emit("session.execution.succeeded", "audit-root")
    const notified = () => client.writes.some(write => write.includes(`LIFECYCLE_${label}`))
    if (defer) await until(() => entered, "Old root success did not begin its lookup")
    client.emit("session.inbox.cancelled", "audit-root", { inboxID: "audit-result" })
    release()
    if (!defer) await until(notified, "Cancellation dropped a completion with child deferral disabled")
    else {
      await Bun.sleep(180)
      expect(notified()).toBe(false)
    }
  } finally {
    release()
    await client.close()
  }
})

test.each(["read", "enqueued", "unseen", "user-event", "user-read", "compaction", "system"])
("cancelling a descendant inbox item affects completion according to its source (%s)", async report => {
  let readReport = false
  let queued = true
  let suspendJobs = false
  let readingJobs = false
  let release!: () => void
  const blocked = new Promise<void>(resolve => { release = resolve })
  const label = `descendant-cancel-${report}`
  const client = await harness(label, async () => {
    if (suspendJobs) {
      readingJobs = true
      await blocked
    }
    return []
  }, undefined, { middle: "root" }, sessionID => {
    if (!queued || sessionID !== "middle") return []
    readReport = true
    return [{ id: "report", type: report === "user-read" ? "user" : "synthetic", payload: { metadata: { source: "subagent" } } }]
  })
  const notified = () => client.writes.filter(write => write.includes(`LIFECYCLE_${label}`)).length
  try {
    const nonReport = report.startsWith("user-") || report === "compaction" || report === "system"
    if (report === "read" || report === "user-read") {
      client.emit("session.execution.succeeded", "root")
      await until(() => readReport, "The descendant inbox was not checked")
      if (nonReport) await until(() => notified() === 1, "A queued user prompt blocked the initial success")
    } else if (report !== "unseen") {
      const item = report === "compaction" ? { type: "compaction", payload: {} }
        : report === "user-event" ? { type: "user", payload: { text: "Queued prompt" } }
        : { type: "synthetic", payload: { metadata: { source: report === "system" ? "other" : "subagent" } } }
      client.emit("session.inbox.enqueued", "middle", {
        inboxID: "report",
        item: { ...item, delivery: "queue" },
      })
    }
    suspendJobs = true
    client.emit("session.execution.started", "root")
    client.emit("session.execution.succeeded", "root")
    await until(() => readingJobs, "The next root success did not begin its lookup")
    queued = false
    const before = notified()
    client.emit("session.inbox.cancelled", "middle", { inboxID: "report" }, undefined, false)
    release()
    if (nonReport) {
      await until(() => notified() === before + 1, "Cancelling an unrelated item discarded root completion")
      return
    }
    await Bun.sleep(180)
    expect(notified()).toBe(0)
    client.emit("session.execution.started", "root")
    client.emit("session.execution.succeeded", "root")
    await until(() => notified() === 1, "A new successful turn did not notify")
  } finally {
    release()
    await client.close()
  }
})

test.each(["report", "no-deferral", "start", "own-report"])
("a late activity event respects the current deferral setting and known ancestry (%s)", async activity => {
  const defer = activity === "report" || activity === "start"
  let readingChild = false
  let snapshots = 0
  let release!: () => void
  const held = new Promise<void>(resolve => { release = resolve })
  const label = `late-activity-${activity}`
  const client = await harness(label, async () => [], undefined, { middle: "root" }, async sessionID => {
    if (sessionID === "middle" && ++snapshots === 2) {
      readingChild = true
      await held
    }
    return []
  })
  const notified = () => client.writes.filter(write => write.includes(`LIFECYCLE_${label}`)).length
  try {
    client.emit("session.execution.succeeded", "root", {}, undefined, true, Date.now())
    await until(() => readingChild, "The scan did not reach the final descendant lookup")
    if (!defer) client.setDeferral(false)
    if (activity === "start") client.emit("session.execution.started", "middle")
    else client.emit("session.inbox.enqueued", activity === "own-report" ? "root" : "middle", {
      inboxID: "late",
      item: { type: "synthetic", delivery: "queue", payload: { metadata: { source: "subagent" } } },
    })
    release()
    if (!defer) {
      await until(() => notified() === 1, "Admission dropped root completion after deferral was turned off")
      return
    }
    await Bun.sleep(180)
    expect(notified()).toBe(0)
    client.emit("session.inbox.delivered", "middle", { inboxID: "late" })
    client.emit("session.execution.started", "root")
    client.emit("session.execution.succeeded", "root")
    await until(() => notified() === 1, "A new successful turn did not notify")
  } finally {
    release()
    await client.close()
  }
})

test.each(["deferred", "checking"])("a distinct root success replaces an older %s success whose successor start was missed", async state => {
  let queued = true
  let checks = 0
  let release!: () => void
  const held = new Promise<void>(resolve => { release = resolve })
  const client = await harness("missed-root-start", async () => {
    checks++
    if (checks === 1 && state === "checking") await held
    return queued ? [{ kind: "subagent", status: "running" }] : []
  })
  const notified = () => client.writes.some(write => write.includes("LIFECYCLE_missed-root-start"))
  try {
    client.emit("session.execution.succeeded", "root")
    await until(() => checks === 1, "The initial success did not check outstanding work")
    if (state === "deferred") await Bun.sleep(100)
    expect(notified()).toBe(false)
    queued = false
    client.emit("session.execution.succeeded", "root")
    await until(notified, "The distinct success did not deliver a new completion")
    expect(checks).toBe(2)
  } finally {
    release()
    await client.close()
  }
})

test("a stalled family does not consume the entire inbox deadline of another root", async () => {
  let stalled = 0
  const children = Object.fromEntries(Array.from({ length: 8 }, (_, index) => [`stall-${index}`, "large-root"]))
  const client = await harness("separate-inbox-deadline", async () => [], undefined, children, (id, signal) => {
    if (!id.startsWith("stall-")) return []
    stalled++
    return new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }))
  })
  try {
    client.emit("session.execution.succeeded", "large-root")
    await until(() => stalled === 8, "The first family did not occupy the inbox slots")
    client.emit("session.execution.succeeded", "small-root")
    await until(() => client.writes.some(write => write.includes("LIFECYCLE_separate-inbox-deadline small-root")),
      "The unrelated root lost its completion")
  } finally {
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
