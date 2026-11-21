import { expect, test } from "bun:test"
import { once } from "node:events"
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { managedServiceReadback, readLegacyInbox } from "./v2-service-readback"

test("cancelled native inbox calls release callers and retain a bounded transport slot", async () => {
  const waiting: Array<() => void> = []
  let started = 0
  const hang = () => {
    started++
    return new Promise<void>(resolve => waiting.push(resolve))
  }
  const cancelBatch = async (count: number) => {
    const controller = new AbortController()
    const settled = Promise.allSettled(Array.from({ length: count }, () => readLegacyInbox(hang, controller.signal)))
    await Bun.sleep(0)
    controller.abort(new Error("lookup expired"))
    expect((await settled).every(result => result.status === "rejected")).toBe(true)
  }
  try {
    await cancelBatch(8)
    expect(started).toBe(8)
    await expect(readLegacyInbox(async () => "available", AbortSignal.timeout(300))).resolves.toBe("available")
    await cancelBatch(8)
    await cancelBatch(8)
    expect(started).toBe(16)
    waiting.shift()!()
    await expect(readLegacyInbox(async () => "available again", AbortSignal.timeout(300))).resolves.toBe("available again")
  } finally {
    for (const release of waiting) release()
  }
})

test("managed readback reads the family inbox metadata without requesting its payloads", async () => {
  const root = mkdtempSync(join(tmpdir(), "notifier-managed-readback-"))
  const previous = process.env.OPENCODE_DB
  const previousState = process.env.XDG_STATE_HOME
  const previousData = process.env.XDG_DATA_HOME
  const previousConfig = process.env.OPENCODE_CONFIG_DIR
  const previousGlobalConfig = process.env.XDG_CONFIG_HOME
  const file = join(root, "opencode.db")
  process.env.OPENCODE_DB = file
  process.env.XDG_STATE_HOME = root
  const app = { version: "2.3.2", channel: "local" }
  const host = process.platform === "linux" ? "127.0.0.2" : "127.0.0.1"
  const requests: string[] = []
  let activity: unknown = {}
  let jobs: unknown = []
  const server = createServer((request, response) => {
    requests.push(request.url ?? "")
    switch (request.url) {
      case "/api/info": response.end(JSON.stringify({ pid: process.pid, version: app.version })); break
      case "/api/session/active": response.end(JSON.stringify({ data: activity })); break
      case "/api/experimental/job?sessionID=root&descendants=true": response.end(JSON.stringify({ data: jobs })); break
      default: response.writeHead(404).end()
    }
  })
  try {
    const sqlite = "bun:sqlite"
    const { Database } = await import(sqlite)
    const db = new Database(file)
    try {
      db.exec(`
        CREATE TABLE session_v2 (id TEXT PRIMARY KEY, parent_id TEXT, time_idle INTEGER, directory TEXT, workspace_id TEXT);
        CREATE INDEX session_v2_parent_idx ON session_v2(parent_id);
        CREATE TABLE session_inbox (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, payload TEXT);
        INSERT INTO session_v2 VALUES ('root', NULL, NULL, '/main', NULL), ('middle', 'root', 42, '/main', 'ws'),
          ('other', NULL, NULL, '/other', NULL), ('empty', NULL, NULL, '/main', NULL), ('large', NULL, 10, '/main', NULL);
        WITH RECURSIVE sequence(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM sequence WHERE id < 8192)
        INSERT INTO session_v2 SELECT 'idle-' || id, 'large', 10, '/main', NULL FROM sequence;
      `)
      const insert = db.query("INSERT INTO session_inbox VALUES (?, ?, ?, ?)")
      insert.run("prompt", "root", "user", JSON.stringify({ text: "x".repeat(2 * 1024 * 1024) }))
      insert.run("goal", "middle", "synthetic", JSON.stringify({ text: "x".repeat(128 * 1024), metadata: { source: "goal" } }))
      insert.run("report", "middle", "synthetic", '{"metadata":{"source":"subagent"}}')
      insert.run("outside", "other", "synthetic", '{"metadata":{"source":"subagent"}}')
    } finally {
      db.close()
    }
    const before = readFileSync(file)
    server.listen(0, host)
    await once(server, "listening")
    const address = server.address()
    if (!address || typeof address === "string") throw new Error("Missing fixture address")
    const state = join(root, "opencode")
    mkdirSync(state)
    writeFileSync(join(state, "service-local.json"), JSON.stringify({ pid: process.pid, version: app.version, url: `http://${host}:${address.port}` }))
    const readback = await managedServiceReadback(app)
    expect(readback).toBeDefined()
    await expect(readback!.active()).resolves.toEqual({})
    const family = await readback!.family("root")
    expect(family.sessions).toEqual([
      { id: "root", idle: null, directory: "/main", workspaceID: null },
      { id: "middle", idle: 42, directory: "/main", workspaceID: "ws" },
    ])
    expect([...family.items].sort((a, b) => a.id.localeCompare(b.id))).toEqual([
      { sessionID: "middle", id: "goal", report: 0 },
      { sessionID: "root", id: "prompt", report: 0 },
      { sessionID: "middle", id: "report", report: 1 },
    ])
    await expect(readback!.family("empty")).resolves.toEqual({
      sessions: [{ id: "empty", idle: null, directory: "/main", workspaceID: null }], items: [],
    })
    await expect(readback!.family("missing")).rejects.toThrow("the session's inbox database is unavailable")
    await expect(readback!.family("large")).rejects.toThrow("the session's inbox database is unavailable")
    expect(requests).toEqual(["/api/info", "/api/session/active"])
    expect(readFileSync(file).equals(before)).toBe(true)
    const updated = new Database(file)
    try {
      updated.query("INSERT INTO session_inbox VALUES (?, ?, ?, ?)").run("cold-report", "idle-8192", "synthetic", '{"metadata":{"source":"subagent"}}')
    } finally { updated.close() }
    await expect(readback!.family("large")).rejects.toThrow("child reports still pending")

    const nativeHome = join(root, "launch-home")
    const launchState = join(nativeHome, ".local", "state", "opencode")
    mkdirSync(launchState, { recursive: true })
    writeFileSync(join(launchState, "service-local.json"), readFileSync(join(state, "service-local.json")))
    delete process.env.XDG_STATE_HOME
    process.env.OPENCODE_DB = "opencode.db"
    const launchReadback = await managedServiceReadback(app, () => nativeHome)
    expect(launchReadback).toBeDefined()
    await expect(launchReadback!.active()).resolves.toEqual({})
    await expect(launchReadback!.family("root")).rejects.toThrow("the session's inbox database is unavailable")
    process.env.XDG_STATE_HOME = root

    process.env.OPENCODE_DB = "opencode.db"
    process.env.OPENCODE_CONFIG_DIR = root
    process.env.XDG_CONFIG_HOME = join(root, "xdg-config")
    process.env.XDG_DATA_HOME = root
    writeFileSync(join(state, "opencode.db"), before)
    writeFileSync(join(root, "service-local.json"), "{}")
    await expect((await managedServiceReadback(app))!.family("root")).resolves.toEqual(family)
    writeFileSync(join(root, "service-local.json"), JSON.stringify({ env: { XDG_DATA_HOME: root } }))
    const httpOnly = (await managedServiceReadback(app))!
    await expect(httpOnly.family("root")).rejects.toThrow("inbox database is unavailable")
    await expect(httpOnly.active()).resolves.toEqual({})
    await expect(httpOnly.jobs({ sessionID: "root", descendants: true })).resolves.toEqual([])
    for (activity of [null, [], { root: {} }]) {
      await expect(httpOnly.active()).rejects.toThrow("invalid native session activity")
    }
    for (jobs of [null, [{}], [{ sessionID: "root", status: "changed", kind: "subagent" }]]) {
      await expect(httpOnly.jobs({ sessionID: "root", descendants: true })).rejects.toThrow("invalid native Job metadata")
    }
    jobs = [{ sessionID: "root", kind: "subagent", status: "completed" }]
    await expect(httpOnly.jobs({ sessionID: "root", descendants: true })).resolves.toEqual(jobs)

    delete process.env.XDG_DATA_HOME
    writeFileSync(join(root, "service-local.json"), JSON.stringify({ env: { HOME: root } }))
    await expect((await managedServiceReadback(app))!.family("root")).rejects.toThrow("inbox database is unavailable")
    process.env.XDG_DATA_HOME = root
    await expect((await managedServiceReadback(app))!.family("root")).resolves.toEqual(family)
    process.env.OPENCODE_DB = file
    await expect(managedServiceReadback(app)).resolves.toBeDefined()
    app.version = "2.2.7"
    writeFileSync(join(state, "service-local.json"), JSON.stringify({ pid: process.pid, version: app.version, url: `http://${host}:${address.port}` }))
    const older = await managedServiceReadback(app)
    expect(older).toBeDefined()
    await expect(older!.family("root")).resolves.toEqual(family)
    app.version = "2.2.6"
    writeFileSync(join(state, "service-local.json"), JSON.stringify({ pid: process.pid, version: app.version, url: `http://${host}:${address.port}` }))
    activity = { root: { type: "running" } }
    const legacy = (await managedServiceReadback(app))!
    await expect(legacy.active()).resolves.toEqual(activity)
    await expect(legacy.jobs({ sessionID: "root", descendants: true })).resolves.toEqual(jobs)
    await expect(legacy.family("root")).rejects.toThrow("inbox database is unavailable")
  } finally {
    server.closeAllConnections()
    await new Promise<void>(resolve => server.close(() => resolve()))
    if (previous === undefined) delete process.env.OPENCODE_DB
    else process.env.OPENCODE_DB = previous
    if (previousState === undefined) delete process.env.XDG_STATE_HOME
    else process.env.XDG_STATE_HOME = previousState
    if (previousData === undefined) delete process.env.XDG_DATA_HOME
    else process.env.XDG_DATA_HOME = previousData
    if (previousConfig === undefined) delete process.env.OPENCODE_CONFIG_DIR
    else process.env.OPENCODE_CONFIG_DIR = previousConfig
    if (previousGlobalConfig === undefined) delete process.env.XDG_CONFIG_HOME
    else process.env.XDG_CONFIG_HOME = previousGlobalConfig
    rmSync(root, { recursive: true, force: true })
  }
})
