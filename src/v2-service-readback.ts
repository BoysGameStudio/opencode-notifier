import { execFile } from "node:child_process"
import { constants } from "node:fs"
import { readFile, stat } from "node:fs/promises"
import { get as httpGet } from "node:http"
import type { IncomingMessage } from "node:http"
import { isIPv4 } from "node:net"
import { homedir, networkInterfaces, userInfo } from "node:os"
import { isAbsolute, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import { OutstandingWorkError } from "./notifier"
import type { StoredInboxFamily } from "./v2-inbox-worker.js"

type RequestOptions = { signal?: AbortSignal }
const RESPONSE_LIMIT = 1024 * 1024
const INBOX_READ_TIMEOUT_MS = 750
const INBOX_QUEUE_TIMEOUT_MS = 2500
const execute = promisify(execFile)

export class InboxStoreUnavailable extends OutstandingWorkError {
  constructor() { super("the session's inbox database is unavailable") }
}

async function readInboxWithinDeadline<T>(read: (signal: AbortSignal) => Promise<T>, caller?: AbortSignal): Promise<T> {
  const deadline = AbortSignal.timeout(INBOX_READ_TIMEOUT_MS)
  const signal = caller ? AbortSignal.any([caller, deadline]) : deadline
  try {
    return await read(signal)
  } catch (error) {
    signal.throwIfAborted()
    throw error
  }
}

type RetainReader = (until: Promise<void>) => void

function inboxQueue(concurrency: number) {
  let reading = 0
  const queued = new Set<() => void>()
  return async function read<T>(request: (retain: RetainReader) => Promise<T>, signal: AbortSignal): Promise<T> {
    signal.throwIfAborted()
    if (reading < concurrency) reading++
    else {
      if (queued.size >= 32) throw new OutstandingWorkError("inbox reader capacity reached")
      await new Promise<void>((resolve, reject) => {
        function remove() {
          queued.delete(start)
          signal.removeEventListener("abort", abort)
        }
        function start() {
          remove()
          resolve()
        }
        function abort() {
          remove()
          reject(signal.reason)
        }
        queued.add(start)
        signal.addEventListener("abort", abort, { once: true })
      })
    }
    function release() {
      const next = queued.values().next().value
      if (next) next()
      else reading--
    }
    let retained: Promise<void> | undefined
    try {
      signal.throwIfAborted()
      return await request(until => { retained = until })
    } finally {
      if (retained) void retained.then(release, release)
      else release()
    }
  }
}

const readManagedInbox = inboxQueue(1)
const readNativeInbox = inboxQueue(8)
// Cancellation releases the caller slot. The transport slot stays until the
// call settles. Eight spare transport slots keep room for a full set of reads.
const nativeTransport = inboxQueue(16)

export function readLegacyInbox<T>(request: (signal: AbortSignal) => Promise<T>, signal: AbortSignal) {
  return readNativeInbox(() => nativeTransport(async retain => {
    let cancelled!: () => void
    const aborted = new Promise<never>((_, reject) => {
      cancelled = () => reject(signal.reason)
      signal.addEventListener("abort", cancelled, { once: true })
    })
    try {
      const pending = Promise.resolve().then(() => {
        signal.throwIfAborted()
        return request(signal)
      })
      retain(pending.then(() => {}, () => {}))
      return await Promise.race([pending, aborted])
    } finally {
      signal.removeEventListener("abort", cancelled)
    }
  }, signal), signal)
}

async function readInboxMetadata(file: string, root: string, signal: AbortSignal, retain: RetainReader): Promise<StoredInboxFamily> {
  signal.throwIfAborted()
  const module = new URL(import.meta.url)
  const entry = fileURLToPath(new URL("./v2-inbox-worker.js", module))
  // The embedded Bun executable can run the reader directly, without another
  // installed runtime. A child process lets us interrupt a synchronous query.
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^(?:BUN_OPTIONS|NODE_(?:OPTIONS|COMPILE_CACHE|V8_COVERAGE))$/i.test(key)))
  env.NODE_DISABLE_COMPILE_CACHE = "1"
  const args: string[] = []
  if (process.versions.bun) {
    env.BUN_BE_BUN = "1"
    const config = new URL("./v2-inbox.bunfig.toml", module)
    args.push(`--config=${fileURLToPath(config)}`, "--no-env-file", "--no-install")
  }
  try {
    const worker = execute(process.execPath, [...args, entry, file, root], {
      env, signal, maxBuffer: RESPONSE_LIMIT, killSignal: "SIGKILL", windowsHide: true,
    })
    // An aborted execFile can reject before the operating system reaps it.
    retain(new Promise(resolve => worker.child.once("close", () => resolve())))
    const { stdout } = await worker
    const result = JSON.parse(stdout) as StoredInboxFamily | { overflow: true } | null
    if (result === null) throw new InboxStoreUnavailable()
    if ("overflow" in result) throw new OutstandingWorkError("inbox readback exceeded its limit with child reports still pending")
    return result
  } catch (error) {
    signal.throwIfAborted()
    if (error instanceof OutstandingWorkError) throw error
    throw new OutstandingWorkError("inbox metadata could not be read")
  }
}

async function readBoundedJSON(body: AsyncIterable<Buffer | string>): Promise<unknown> {
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of body) {
    const buffer = typeof chunk === "string" ? Buffer.from(chunk) : chunk
    bytes += buffer.length
    if (bytes > RESPONSE_LIMIT) throw new OutstandingWorkError("response exceeded the readback limit")
    chunks.push(buffer)
  }
  return JSON.parse(Buffer.concat(chunks, bytes).toString("utf8"))
}

async function readServiceFile(path: string) {
  const controller = new AbortController()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      (async () => {
        const info = await stat(path)
        if (!info.isFile() || info.size > 16 * 1024 || controller.signal.aborted) return
        const flag = constants.O_RDONLY | (process.platform === "win32" ? 0 : constants.O_NONBLOCK)
        return readFile(path, { encoding: "utf8", flag, signal: controller.signal })
      })(),
      new Promise<undefined>(resolve => {
        timer = setTimeout(() => {
          controller.abort()
          resolve(undefined)
        }, 500)
        timer.unref()
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function isLocalHost(hostname: string) {
  if (hostname === "localhost" || hostname === "[::1]" || (isIPv4(hostname) && hostname.startsWith("127."))) return true
  return Object.values(networkInterfaces()).some(addresses => addresses?.some(({ address, family }) => {
    const host = family === "IPv6" ? `[${address.split("%")[0]}]` : address
    return new URL(`http://${host}`).hostname === hostname
  }))
}

type OpenCodeApp = { version: string; channel: string }

function supportsReadback(app: OpenCodeApp | undefined): app is OpenCodeApp {
  return typeof app?.channel === "string" && /^2\.(?:2\.[67]|3\.\d+)$/.test(app.version)
}

function databasePath(app: OpenCodeApp) {
  if (!/^2\.(?:2\.7|3\.\d+)$/.test(app.version)) return
  // Unlike the Bun executable, Node SEA always runs its embedded entrypoint.
  if (!process.versions.bun && process.getBuiltinModule?.("node:sea")?.isSea?.()) return
  const channel = app.channel.replace(/[^a-zA-Z0-9._-]/g, "-")
  const sharedDatabase = ["latest", "dev", "beta", "next", "prod"].includes(app.channel) ||
    ["1", "true"].includes(process.env.OPENCODE_DISABLE_CHANNEL_DB ?? "")
  const database = process.env.OPENCODE_DB ?? (sharedDatabase ? "opencode.db" : `opencode-${channel}.db`)
  if (database !== ":memory:") return database
}

function familyReader(database: string) {
  const file = resolve(process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"), "opencode", database)
  return (sessionID: string, options?: RequestOptions) => {
    const deadline = AbortSignal.timeout(INBOX_QUEUE_TIMEOUT_MS)
    const signal = options?.signal ? AbortSignal.any([options.signal, deadline]) : deadline
    return readManagedInbox(retain => readInboxWithinDeadline(worker => readInboxMetadata(file, sessionID, worker, retain), signal), signal)
  }
}

export function standaloneFamilyReader(app: OpenCodeApp | undefined) {
  // A CLI standalone server does not apply the managed service's saved environment.
  const args = process.argv.slice(2)
  const database = supportsReadback(app) ? databasePath(app) : undefined
  if (database && args.includes("serve") && !args.some(arg => arg === "--service" || arg.startsWith("--service=")))
    return familyReader(database)
}

function record(value: unknown): Record<string, unknown> | undefined {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as Record<string, unknown>
}

export function nativeActivity(value: unknown) {
  const active = record(value)
  if (!active || Object.entries(active).some(([id, status]) => !id || record(status)?.type !== "running"))
    throw new OutstandingWorkError("invalid native session activity")
  return active
}

export async function managedServiceReadback(app: OpenCodeApp | undefined, accountHome = () => userInfo().homedir) {
  if (!supportsReadback(app)) return
  let database = databasePath(app)
  const channel = app.channel.replace(/[^a-zA-Z0-9._-]/g, "-")
  const filename = ["latest", "dev", "beta", "next"].includes(app.channel)
    ? "service.json"
    : `service-${channel}.json`
  const state = process.env.XDG_STATE_HOME
  const statePaths = [state || join(homedir(), ".local", "state")]
  // Direct serve captures the state root before applying the saved HOME.
  // Explicit XDG_STATE_HOME is preserved by the native service.
  if (!state) try {
    const nativeState = join(accountHome(), ".local", "state")
    if (!statePaths.includes(nativeState)) statePaths.push(nativeState)
  } catch { /* The effective state directory is still a candidate. */ }
  let base: URL | undefined
  let password: string | undefined
  for (const directory of statePaths) {
    try {
      const registration = await readServiceFile(join(directory, "opencode", filename))
      if (registration === undefined) continue
      const info = JSON.parse(registration)
      if (info?.pid !== process.pid || info.version !== app.version || typeof info.url !== "string") continue
      const url = new URL(info.url)
      if (url.protocol !== "http:" || url.username || url.password || url.pathname !== "/" || url.search || url.hash) continue
      if (url.hostname === "0.0.0.0") url.hostname = "127.0.0.1"
      if (url.hostname === "[::]") url.hostname = "[::1]"
      if (!isLocalHost(url.hostname)) continue
      if (directory !== statePaths[0] && database && !isAbsolute(database)) database = undefined
      base = url
      if (typeof info.password === "string") password = info.password
      break
    } catch { /* Continue to the next native state candidate. */ }
  }
  if (!base) return
  if (database && !isAbsolute(database)) {
    try {
      // OpenCode captures the launch configuration and data roots before
      // applying saved environment variables for a direct `serve --service`.
      const configDir = process.env.OPENCODE_CONFIG_DIR || join(process.env.XDG_CONFIG_HOME || join(homedir(), ".config"), "opencode")
      const config = await readServiceFile(join(configDir, filename))
      const saved = config === undefined ? undefined : JSON.parse(config)
      const savedVariables = Object.entries(saved?.env ?? {}).filter(([, value]) => typeof value === "string")
        .map(([key]) => process.platform === "win32" ? key.toUpperCase() : key)
      const overridesRoots = savedVariables.some(key => ["XDG_DATA_HOME", "XDG_CONFIG_HOME", "OPENCODE_CONFIG_DIR"].includes(key))
      const overridesHome = savedVariables.some(key => key === "HOME" || (process.platform === "win32" && key === "USERPROFILE"))
      if (!saved || saved.password !== password || overridesRoots || (!process.env.XDG_DATA_HOME && overridesHome)) database = undefined
    } catch {
      database = undefined
    }
  }
  const authorization = password === undefined ? undefined : "Basic " + Buffer.from(`opencode:${password}`).toString("base64")

  async function get(path: string, signal?: AbortSignal) {
    // Connect to the verified local address directly, even if the host
    // delegates other HTTP traffic to a proxy.
    const response = await new Promise<IncomingMessage>((resolve, reject) => {
      httpGet(new URL(path, base), {
        headers: authorization ? { authorization } : undefined,
        agent: false,
        signal,
      }, resolve).once("error", reject)
    })
    if (response.statusCode !== 200) {
      response.destroy()
      throw new OutstandingWorkError(`native work request returned HTTP ${response.statusCode}`)
    }
    return record(await readBoundedJSON(response))
  }

  let verified = false
  const version = app.version
  async function verify(signal?: AbortSignal) {
    if (verified) return
    const info = await get("/api/info", signal)
    if (info?.pid !== process.pid || info.version !== version) throw new OutstandingWorkError("service identity changed")
    verified = true
  }
  return {
    async active(options?: RequestOptions) {
      await verify(options?.signal)
      return nativeActivity((await get("/api/session/active", options?.signal))?.data)
    },
    async jobs({ sessionID }: { sessionID: string; descendants: true }, options?: RequestOptions) {
      await verify(options?.signal)
      const query = new URLSearchParams({ sessionID, descendants: "true" })
      const jobs = (await get(`/api/experimental/job?${query}`, options?.signal))?.data
      if (!Array.isArray(jobs) || jobs.some(value => {
        const job = record(value)
        return typeof job?.sessionID !== "string" || !job.sessionID || typeof job.kind !== "string" || !["shell", "subagent", "image", "codemode"].includes(job.kind) ||
          typeof job.status !== "string" || !["running", "completed", "error", "cancelled"].includes(job.status)
      })) throw new OutstandingWorkError("invalid native Job metadata")
      return jobs as { sessionID: string; kind: string; status: string }[]
    },
    family: database ? familyReader(database) : async () => { throw new InboxStoreUnavailable() },
  }
}

export type ManagedServiceReadback = NonNullable<Awaited<ReturnType<typeof managedServiceReadback>>>
