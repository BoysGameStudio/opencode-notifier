import type { Plugin } from "@opencode/plugin/tui"
import { captureStartupWindowId } from "./focus"
import { disposeLinuxNotifications } from "./notify"
import { createV2Notifier } from "./v2"
import { createTerminalClients } from "./terminal-clients"

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
  let notifier = createV2Notifier(ctx.client, location, "terminal", hooks)
  let disposed = false
  // Ownership and Job checks are asynchronous, so lifecycle events are still
  // processed in their arrival order. But execution.started MUST invalidate any
  // older completion synchronously on receipt, not when it reaches this queue.
  let eventQueue: Promise<void> = Promise.resolve()
  const stop = ctx.data.listen(({ details }) => {
    if (disposed) return
    const current = ctx.location ?? ctx.data.location.default()
    if (current.directory !== location.directory || current.workspaceID !== location.workspaceID) {
      notifier.dispose()
      location = current
      notifier = createV2Notifier(ctx.client, location, "terminal", hooks)
    }
    const target = notifier
    const receipt = target.received(details)
    eventQueue = eventQueue.then(async () => {
      if (disposed) return
      // Presence refresh is best-effort; a temporary failure must not lose events.
      try { await clients.refresh() } catch {}
      await target.event(details, receipt)
    }).catch(() => undefined)
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
