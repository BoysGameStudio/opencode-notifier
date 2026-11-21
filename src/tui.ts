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
  // Only events relevant to notifier delivery or lifecycle tracking need an
  // asynchronous handler. In particular, never queue high-volume text/reasoning
  // deltas. Each session has a separate serial queue so a hung metadata lookup
  // cannot block permission or completion alerts for unrelated sessions.
  const relevant = new Set<string>([
    "session.created", "session.deleted", "session.moved",
    "session.execution.started", "session.execution.succeeded",
    "session.execution.failed", "session.execution.interrupted",
    "session.inbox.enqueued", "permission.asked", "form.created",
  ])
  const queues = new Map<string, Promise<void>>()
  const stop = ctx.data.listen(({ details }) => {
    if (disposed || !relevant.has(details.type)) return
    if (details.type === "session.inbox.enqueued" &&
        details.data.item.type !== "user" &&
        !(details.data.item.type === "synthetic" && details.data.item.payload.metadata?.source === "subagent")) return
    const current = ctx.location ?? ctx.data.location.default()
    if (current.directory !== location.directory || current.workspaceID !== location.workspaceID) {
      notifier.dispose()
      location = current
      notifier = createV2Notifier(ctx.client, location, "terminal", hooks)
      void clients.refresh().catch(() => undefined)
    }
    const target = notifier
    const receipt = target.received(details)
    const sessionID = details.type === "form.created"
      ? details.data.form.sessionID
      : "sessionID" in details.data ? details.data.sessionID : undefined
    const queueKey = sessionID ?? "__global__"
    const previous = queues.get(queueKey) ?? Promise.resolve()
    const task = previous.then(async () => {
      if (disposed) return
      await target.event(details, receipt)
    }).catch(() => undefined).finally(() => {
      if (queues.get(queueKey) === task) queues.delete(queueKey)
    })
    queues.set(queueKey, task)
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
