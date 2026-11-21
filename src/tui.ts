import type { Plugin } from "@opencode/plugin/tui"
import { captureStartupWindowId } from "./focus"
import { disposeLinuxNotifications } from "./notify"
import { createV2Notifier, inboxRpc, notificationEvent } from "./v2"
import type { InboxFamily } from "./v2-inbox-worker.js"
import { OutstandingWorkError } from "./notifier"
import { createTerminalClients } from "./terminal-clients"

function inboxFamily(value: unknown, sessionID: string): InboxFamily {
  const invalid = () => new OutstandingWorkError("invalid server inbox metadata")
  const family = value as InboxFamily | undefined
  if (!family || !Array.isArray(family.sessions) || !Array.isArray(family.items)) throw invalid()
  const sessions = new Set<string>()
  for (const session of family.sessions) {
    if (!session || typeof session.id !== "string" || !session.id || sessions.has(session.id) ||
        (session.idle !== null && (!Number.isSafeInteger(session.idle) || session.idle < 0))) throw invalid()
    sessions.add(session.id)
  }
  if (!sessions.has(sessionID) || family.items.some(item => !item || typeof item.id !== "string" || !item.id ||
      !sessions.has(item.sessionID) || (item.report !== 0 && item.report !== 1 && item.report !== 2))) throw invalid()
  if (family.externalActivity !== undefined && (!Array.isArray(family.externalActivity) ||
      family.externalActivity.some(id => typeof id !== "string" || !/^[0-9a-f]{64}$/.test(id)))) throw invalid()
  if (family.externalWork !== undefined && typeof family.externalWork !== "boolean") throw invalid()
  return family
}

const setup: Plugin.Definition["setup"] = async (ctx) => {
  let location = ctx.location ?? ctx.data.location.default()
  await captureStartupWindowId()
  const clients = createTerminalClients(() => {
    let sessions: string[] | null = null
    try {
      const route = ctx.ui?.router?.current?.()
      const tabs = ctx.ui?.tabs?.list?.()
      if (tabs) sessions = tabs.map(tab => tab.sessionID)
      if (route?.type === "session") {
        // A child route also owns its family, so publish its root alongside it.
        (sessions ??= []).push(route.sessionID)
        sessions.push(ctx.data.session.root(route.sessionID) || route.sessionID)
      }
    } catch {}
    return { location: ctx.location ?? ctx.data.location.default(), sessions }
  }, sessionID => ctx.data.session.root(sessionID))
  await clients.refresh()
  const hooks = {
    claim: clients.claim,
    focusSession(sessionID: string) {
      if (disposed) return
      let root = sessionID
      try { root = ctx.data.session.root(sessionID) || sessionID } catch {}
      if (!ctx.ui?.tabs?.focus?.(root) || root !== sessionID) ctx.ui?.router?.navigate?.({ type: "session", sessionID })
    },
  }
  const remote = typeof ctx.client.rpc === "function" ? ctx.client.rpc(inboxRpc) : undefined
  const create = (owner: typeof location) => createV2Notifier(ctx.client, owner, "terminal", {
    ...hooks,
    current: () => {
      const current = ctx.location ?? ctx.data.location.default()
      return current.directory === owner.directory && current.workspaceID === owner.workspaceID
    },
  }, undefined, undefined, remote ? async (input, signal) => {
    try {
      const result = await remote.inboxFamily({ ...input, workspaceID: owner.workspaceID }, { location: owner, signal })
      if (result && typeof result === "object" && "availability" in result &&
          (result.availability === "unsupported-location" || result.availability === "unsupported-store")) return
      return inboxFamily(result, input.sessionID)
    } catch (error) {
      const type = (error as { type?: string } | undefined)?.type
      if (type !== "rpc.unavailable" && type !== "rpc.method_not_found") throw error
    }
  } : undefined)
  let notifier = create(location)
  let disposed = false
  const stop = ctx.data.listen(({ details }) => {
    if (disposed || !notificationEvent(details)) return
    const current = ctx.location ?? ctx.data.location.default()
    if (current.directory !== location.directory || current.workspaceID !== location.workspaceID) {
      notifier.dispose()
      location = current
      notifier = create(location)
      void clients.refresh().catch(() => undefined)
    }
    notifier.enqueue(details)
  })
  void notifier.connected().catch(() => undefined)
  return () => {
    disposed = true
    clients.dispose()
    disposeLinuxNotifications()
    stop()
    notifier.dispose()
  }
}

export default {
  id: "opencode-notifier",
  // V1 discovers ./tui too. Its server entrypoint already handles delivery.
  tui: async () => {},
  setup,
}
