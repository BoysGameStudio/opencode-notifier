import { disposeLinuxNotifications } from "./notify"
import type { Plugin, PluginModule } from "@opencode-ai/plugin"
import { loadConfig } from "./config"
import { captureStartupWindowId } from "./focus"
import { createNotifier } from "./notifier"
import { setupV2 } from "./v2"
export { extractAgentNameFromSessionTitle } from "./delivery"
export { PERMISSION_PENDING_GRACE_MS } from "./notifier"

export function isCLIClient(clientEnv?: string): boolean {
  return !clientEnv || clientEnv === "cli"
}

type UnknownRecord = Record<string, unknown>

function asRecord(value: unknown): UnknownRecord | null {
  return value !== null && typeof value === "object" ? (value as UnknownRecord) : null
}

function getNestedRecord(root: unknown, ...path: string[]): UnknownRecord | null {
  let current: unknown = root
  for (const key of path) {
    const record = asRecord(current)
    if (!record || !(key in record)) {
      return null
    }
    current = record[key]
  }
  return asRecord(current)
}

function getStringField(record: UnknownRecord | null, key: string): string | null {
  if (!record) {
    return null
  }
  const value = record[key]
  return typeof value === "string" && value.length > 0 ? value : null
}

function getSessionIDFromEvent(event: unknown): string | null {
  const properties = getNestedRecord(event, "properties")
  return getStringField(properties, "sessionID")
}

export function getPermissionIDFromEvent(event: unknown): string | null {
  const properties = getNestedRecord(event, "properties")
  const id = getStringField(properties, "id")
  if (id) {
    return id
  }
  const request = getNestedRecord(event, "properties", "request")
  return getStringField(request, "id")
}

// True when the request is still awaiting approval. Fails open: any lookup
// failure means "unknown", and unknown must notify rather than stay silent.
export async function isPermissionStillPending(client: unknown, permissionID: string, signal?: AbortSignal): Promise<boolean> {
  try {
    // The v1 SDK client type exposes no permission.list API, so go through
    // the raw HTTP client, with shape checks at this untyped boundary.
    const record = asRecord(client)
    const inner = asRecord(record?._client) ?? getNestedRecord(client, "session", "_client")
    if (!inner || typeof inner.get !== "function") {
      return true
    }
    const listResponse: unknown = await inner.get({ url: "/permission", ...(signal ? { signal } : {}) })
    const body = asRecord(listResponse)?.data ?? listResponse
    const pendingList = Array.isArray(body) ? body : asRecord(body)?.data
    if (!Array.isArray(pendingList)) {
      return true
    }
    return pendingList.some((p: unknown) => asRecord(p)?.id === permissionID)
  } catch {
    return true
  }
}

interface SessionLifecycleInfo {
  id: string | null
  title: string | null
  parentID: string | null
}

function getSessionLifecycleInfo(event: unknown): SessionLifecycleInfo {
  const info = getNestedRecord(event, "properties", "info")
  return {
    id: getStringField(info, "id"),
    title: getStringField(info, "title"),
    parentID: getStringField(info, "parentID"),
  }
}

interface MessageUpdatedInfo {
  role: string | null
  sessionID: string | null
}

function getMessageUpdatedInfo(event: unknown): MessageUpdatedInfo {
  const info = getNestedRecord(event, "properties", "info")
  return {
    role: getStringField(info, "role"),
    sessionID: getStringField(info, "sessionID"),
  }
}

export const NotifierPlugin: Plugin = async ({ client, directory }) => {
  await captureStartupWindowId()
  const clientEnv = process.env.OPENCODE_CLIENT
  if (clientEnv && clientEnv !== "cli" && !loadConfig().enableOnDesktop) return {}
  const notifier = createNotifier({
    async info(id) {
      try {
        const { data } = await client.session.get({ path: { id } })
        if (!data) return { isChild: null, title: null }
        return { isChild: !!data.parentID, title: data.title ?? null }
      } catch {
        return { isChild: null, title: null }
      }
    },
    async elapsed(id, now) {
      try {
        const { data } = await client.session.messages({ path: { id } })
        const times = (data ?? []).flatMap(({ info }) =>
          info.role === "user" && typeof info.time?.created === "number" ? [info.time.created] : [])
        return times.length ? (now - Math.max(...times)) / 1000 : null
      } catch {
        return null
      }
    },
    permissionPending: (_id, requestID, signal) => isPermissionStillPending(client, requestID, signal),
  }, directory)
  const isCLI = isCLIClient(clientEnv)
  type QuestionAlert = { sessionID: string; closed: boolean }
  const questionRequests = new Map<string, { alert?: QuestionAlert; closed: boolean }>()
  const questionCalls = new Map<string, QuestionAlert>()
  const closedPermissions = new Map<string, true>()
  function rememberAlert<T>(map: Map<string, T>, key: string, value: T) {
    map.delete(key)
    map.set(key, value)
    if (map.size > 2048) map.delete(map.keys().next().value!)
  }
  function closeSessionQuestions(sessionID: string) {
    for (const [key, alert] of questionCalls) if (alert.sessionID === sessionID) {
      alert.closed = true
      questionCalls.delete(key)
    }
  }
  function alertPermission(sessionID: string | null, requestID: string | null) {
    const valid = () => !requestID || !closedPermissions.has(requestID)
    void notifier.permission(sessionID, requestID, true, undefined, valid).catch(() => undefined)
  }
  const connected = () => void notifier.notify("client_connected").catch(() => undefined)
  const connectedTimer = isCLI ? undefined : setTimeout(connected, 100)
  if (isCLI) connected()

  return {
    async dispose() {
      disposeLinuxNotifications()
      clearTimeout(connectedTimer)
      notifier.dispose()
      questionRequests.clear()
      questionCalls.clear()
      closedPermissions.clear()
    },
    async event({ event }) {
      try {
        const type = asRecord(event)?.type
        const sessionID = getSessionIDFromEvent(event)
        if (sessionID && (type === "question.asked" || type === "question.replied" || type === "question.rejected")) {
          const properties = getNestedRecord(event, "properties")
          const asked = type === "question.asked"
          const id = getStringField(properties, asked ? "id" : "requestID")
          const callID = asked ? getStringField(getNestedRecord(properties, "tool"), "callID") : null
          if (id) {
            const key = `${sessionID}\0${id}`
            const previous = questionRequests.get(key)
            const alert = callID ? questionCalls.get(`${sessionID}\0${callID}`) : previous?.alert
            const closed = !asked || previous?.closed === true
            if (closed && alert) alert.closed = true
            rememberAlert(questionRequests, key, { alert, closed })
          }
        }
        if (type === "session.created" || type === "session.updated") {
          const info = getSessionLifecycleInfo(event)
          if (type === "session.created") await notifier.created(info.id, info.parentID, info.title)
          else if (info.id) notifier.track(info.id, info.parentID)
        }
        // Stop deleted work while retaining child tombstones for late idle events.
        if (type === "session.deleted") {
          const id = getSessionLifecycleInfo(event).id
          if (id) {
            closeSessionQuestions(id)
            await notifier.stopped(id)
          }
        }
        if (type === "permission.asked") {
          alertPermission(sessionID, getPermissionIDFromEvent(event))
        }
        if (type === "permission.replied") {
          const properties = getNestedRecord(event, "properties")
          const requestID = getStringField(properties, "requestID") ?? getStringField(properties, "permissionID")
          if (requestID) rememberAlert(closedPermissions, requestID, true)
        }
        if (type === "session.idle") {
          if (sessionID) closeSessionQuestions(sessionID)
          await notifier.idle(sessionID, isCLI)
        }
        if (type === "session.status" && getNestedRecord(event, "properties", "status")?.type === "busy" && sessionID) {
          notifier.busy(sessionID)
        }
        if (type === "session.error") {
          const cancelled = getNestedRecord(event, "properties", "error")?.name === "MessageAbortedError"
          if (sessionID) closeSessionQuestions(sessionID)
          await notifier.failed(sessionID, cancelled ? "user_cancelled" : "error")
        }
        if (type === "message.updated") {
          const info = getMessageUpdatedInfo(event)
          if (info.role === "user") await notifier.userMessage(info.sessionID)
        }
      } catch {
        // Notification failures must not interrupt the host.
      }
    },
    "permission.ask": async (input) => {
      try {
        const request = asRecord(input)
        alertPermission(getStringField(request, "sessionID"), getStringField(request, "id"))
      } catch {}
    },
    "tool.execute.before": async (input) => {
      try {
        if (input.tool === "question" || input.tool === "plan_exit") {
          const sessionID = getStringField(asRecord(input), "sessionID")
          const callID = getStringField(asRecord(input), "callID")
          const alert = input.tool === "question" && sessionID && callID ? { sessionID, closed: false } : undefined
          const question = alert ? `${sessionID}\0${callID}` : undefined
          if (question && alert) rememberAlert(questionCalls, question, alert)
          await notifier.notify(input.tool, sessionID, {
            key: question ? `question:${question}` : undefined, dispatchIndependent: true, valid: () => !alert?.closed,
          })
        }
      } catch {}
    },
  }
}

export default {
  id: "opencode-notifier",
  server: NotifierPlugin,
  setup: setupV2,
} satisfies PluginModule & { setup: typeof setupV2 }
