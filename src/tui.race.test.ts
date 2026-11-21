import { afterAll, expect, test } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import type { OpenCodeEvent } from "@opencode/client"
import type { Plugin } from "@opencode/plugin/tui"
import tui from "./tui"

const root = mkdtempSync(join(tmpdir(), "notifier-tui-race-"))
afterAll(() => rmSync(root, { recursive: true, force: true }))

async function until(predicate: () => boolean, message: string) {
  for (let attempt = 0; attempt < 300 && !predicate(); attempt++) await Bun.sleep(5)
  expect(predicate(), message).toBe(true)
}

async function exerciseRace(options: {
  stall: "job" | "ownership"
  foreignStart?: boolean
}) {
  const configPath = join(root, options.stall + (options.foreignStart ? "-foreign" : "") + ".json")
  writeFileSync(configPath, JSON.stringify({
    deferCompleteUntilChildrenIdle: true,
    suppressWhenFocused: false,
    notificationSystem: "ghostty",
    sound: false,
    events: { client_connected: false, subagent_complete: false },
    // Ghostty notification delivery also deduplicates by message text. Each
    // scenario needs its own message, just as its host events need unique IDs.
    messages: { complete: `RACE_DONE_${options.stall}_${options.foreignStart ? "foreign" : "local"}` },
  }))
  const previous = process.env.OPENCODE_NOTIFIER_CONFIG_PATH
  process.env.OPENCODE_NOTIFIER_CONFIG_PATH = configPath
  const originalWrite = process.stdout.write
  const writes: string[] = []
  process.stdout.write = ((chunk: unknown, ...args: unknown[]) => {
    writes.push(String(chunk))
    const callback = args.find(value => typeof value === "function") as (() => void) | undefined
    callback?.()
    return true
  }) as typeof process.stdout.write

  let listener: Parameters<Plugin.Context["data"]["listen"]>[0] | undefined
  const location = { directory: root }
  let jobsChecked = 0
  let gets = 0
  let release!: (result: unknown) => void
  const blocked = new Promise<unknown>(resolve => { release = resolve })
  const trace: string[] = []
  const emit = (id: string, type: string, eventLocation?: { directory: string }) => {
    // The real host generates globally unique event IDs. Distinct tests must
    // not re-use the same delivery-claim key within the shared temp directory.
    const eventID = `${options.stall}:${options.foreignStart ? "foreign" : "local"}:${id}`
    const details = {
      id: eventID, type,
      ...(eventLocation ? { location: eventLocation } : {}),
      data: { sessionID: "parent" },
    } as OpenCodeEvent
    trace.push(`emit:${id}`)
    listener?.({ details })
  }

  let dispose: Awaited<ReturnType<typeof tui.setup>>
  try {
    dispose = await tui.setup({
      location,
      client: {
        session: {
          async get({ sessionID }: { sessionID: string }) {
            gets++
            trace.push(`get#${gets}`)
            if (options.stall === "ownership" && gets === 1) return blocked
            return { id: sessionID, title: "parent task", location }
          },
          inbox: { list: async () => {
            trace.push("inbox")
            return []
          } },
        },
        experimental: { job: {
          async list() {
            jobsChecked++
            trace.push(`job#${jobsChecked}`)
            if (options.stall === "job" && jobsChecked === 1) return blocked
            return []
          },
        } },
        permission: { list: async () => [] },
      },
      data: { listen(callback: typeof listener) { listener = callback; return () => { listener = undefined } } },
    } as never)

    // When eventLocation is omitted the V2 adapter must resolve ownership
    // asynchronously; the separate test holds that lookup before idle().
    emit("old-complete", "session.execution.succeeded", options.stall === "job" ? location : undefined)
    await until(() => options.stall === "job" ? jobsChecked === 1 : gets === 1, "Old completion did not enter its slow lookup")

    // A new start arrives while the old completion is stalled. In the foreign
    // case it belongs to a different workspace and must not cancel our alert.
    emit("parent-resumed", "session.execution.started", options.foreignStart ? { directory: "/foreign-project" } : location)
    release(options.stall === "ownership" ? { id: "parent", title: "parent task", location } : [])
    if (!options.foreignStart) {
      emit("final-complete", "session.execution.succeeded", location)
    }
    for (let attempt = 0; attempt < 300 && !writes.some(write => write.includes("RACE_DONE")); attempt++) await Bun.sleep(5)
    expect(writes.some(write => write.includes("RACE_DONE")),
      `Final completion was not notified: ${trace.join(", ")}; output=${writes.join("").slice(0,300)}`).toBe(true)
    await Bun.sleep(40)
    expect(writes.filter(write => write.includes("RACE_DONE")).length).toBe(1)
  } finally {
    await dispose?.()
    process.stdout.write = originalWrite
    if (previous === undefined) delete process.env.OPENCODE_NOTIFIER_CONFIG_PATH
    else process.env.OPENCODE_NOTIFIER_CONFIG_PATH = previous
  }
}

test("TUI start received during an in-flight Job lookup cancels the stale complete", async () => {
  await exerciseRace({ stall: "job" })
})

test("TUI start received before older complete reaches idle cancels the stale complete", async () => {
  await exerciseRace({ stall: "ownership" })
})

test("a foreign project's start does not cancel an in-flight local completion", async () => {
  await exerciseRace({ stall: "job", foreignStart: true })
})
