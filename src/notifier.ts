import { basename } from "path"
import { loadConfig } from "./config"
import type { EventType, NotifierConfig } from "./config"
import { claimDelivery } from "./claim"
import { deliversAny, extractAgentNameFromSessionTitle, handleEvent, shouldResolveSessionContextForEvent } from "./delivery"
import { debugFocusState as debug } from "./focus"
import { shouldSuppressPermissionAlert, prunePermissionAlertState } from "./permission-dedupe"

// Allow immediate auto-approval to settle before checking the pending list.
export const PERMISSION_PENDING_GRACE_MS = 300
const IDLE_COMPLETE_DELAY_MS = 350
// OpenCode resumes a parent with its children's results right after the last one stops. 2 s leaves
// room for the parent's busy event while still alerting soon for a parent that never resumes.
export const CHILDREN_SETTLE_MS = 2000
// Preserve recent native child ancestry across the five-minute idle cleanup:
// OpenCode allows follow-up work in an existing Subagent session much later.
const MAX_CACHED_CHILD_PARENTS = 4096

export interface SessionInfo {
  // A failed lookup must never turn a deleted child into a parent.
  isChild: boolean | null
  title: string | null
  agentName?: string | null
}

export interface SessionAccess {
  info(sessionID: string): Promise<SessionInfo>
  elapsed(sessionID: string, now: number): Promise<number | null>
  permissionPending(sessionID: string | null, permissionID: string): Promise<boolean>
  // V2 TUI: include server-side background Jobs and queued subagent reports.
  // Absent on V1 and on server-only V2 delivery.
  hasOutstandingWork?(sessionID: string): Promise<boolean>
  hasObservedOutstandingWork?(sessionID: string): boolean
  // V2 server events know parent/child lifecycle even without the Job API.
  waitForParentResume?: boolean
}

export type Delivery = "all" | "terminal" | "command"

export interface DeliveryHooks {
  // valid() is checked right before the claim is recorded, so a canceled alert leaves its key free for a retry.
  claim?: (key: string, sessionID: string | null, valid: () => boolean) => Promise<boolean>
  focusSession?: (sessionID: string) => void
}

// key identifies the host event behind an alert. Terminals on one machine share it, so only one delivers.
interface NotifyOptions {
  title?: string | null
  now?: number
  key?: string
  // Checked after lookups and again around the claim. False skips the alert. Must not change state.
  valid?: () => boolean
  // Runs once the alert passed every check and is about to be delivered.
  commit?: () => void
}

// Each host instance owns its timers and session state. The adapters only translate API data.
export function createNotifier(access: SessionAccess, directory: string, delivery: Delivery = "all", hooks: DeliveryHooks = {}) {
  const children = new Set<string>()
  const sequences = new Map<string, number>()
  const errors = new Set<string>()
  let nextSequence = 0
  const touched = new Map<string, number>()
  const idleTimers = new Map<string, ReturnType<typeof setTimeout>>()
  const parents = new Map<string, string>()
  const running = new Set<string>()
  // timer drops the alert at deferredCompleteTimeout; grace runs while no descendant is running. The
  // entry stays until delivery, so dropping it or clearing grace cancels an alert already in flight.
  const pending = new Map<string, {
    title: string | null
    key?: string
    timer: ReturnType<typeof setTimeout>
    grace?: ReturnType<typeof setTimeout>
  }>()
  let disposed = false

  function config(): NotifierConfig {
    const value = loadConfig()
    if (delivery === "all") return value
    const local = structuredClone(value)
    if (delivery === "command") {
      local.suppressWhenFocused = false
      for (const event of Object.values(local.events)) {
        event.notification = false
        event.sound = false
        event.bell = false
      }
    } else {
      local.command.enabled = false
    }
    return local
  }

  function invalidate(sessionID: string): number {
    touched.set(sessionID, Date.now())
    const sequence = ++nextSequence
    sequences.set(sessionID, sequence)
    clearTimeout(idleTimers.get(sessionID))
    idleTimers.delete(sessionID)
    drop(sessionID)
    return sequence
  }

  function drop(sessionID: string) {
    const deferred = pending.get(sessionID)
    if (!deferred) return
    clearTimeout(deferred.timer)
    clearTimeout(deferred.grace)
    pending.delete(sessionID)
  }

  async function notify(event: EventType, sessionID: string | null = null, { title, now = Date.now(), key, valid = () => true, commit }: NotifyOptions = {}) {
    if (disposed) return
    const current = config()
    // Do not count a server event that has no command to deliver.
    if (delivery === "command" && (!current.command.enabled || !current.command.path || !current.events[event].command)) return
    const lifecycleEvent = event === "session_started" || event === "user_message"
    let elapsed: number | null = null
    if (!lifecycleEvent && sessionID && (current.minDuration > 0 || (current.command.enabled && (current.command.minDuration ?? 0) > 0))) {
      elapsed = await access.elapsed(sessionID, now)
    }
    let sessionTitle = title ?? null
    let agentName: string | null = null
    if (sessionID && ((!sessionTitle && current.showSessionTitle) || shouldResolveSessionContextForEvent(current, event))) {
      const info = await access.info(sessionID)
      sessionTitle ??= info.title
      agentName = info.agentName ?? null
    }
    if (disposed || !valid()) return
    if (key && delivery === "terminal") {
      if (!deliversAny(current, event)) {
        debug(`${event}: no channel enabled in this terminal, leaving the event to other terminals`)
        return
      }
      const identity = `${directory}\0${key}`
      if (!(hooks.claim ? await hooks.claim(identity, sessionID, () => !disposed && valid()) : claimDelivery(identity))) return
    }
    if (disposed || !valid()) return
    commit?.()
    const project = directory ? (current.showFullPath ? directory : basename(directory)) : null
    await handleEvent(current, event, project, elapsed, sessionTitle, sessionID, agentName ?? extractAgentNameFromSessionTitle(sessionTitle), hooks.focusSession)
  }

  // A new child can start after a parent has checked Job/inbox state but
  // before its notification delivery claim settles. In strict V2 mode,
  // invalidate all known ancestors so that in-flight completion cannot pass
  // the final sequence check, even if the child finishes very quickly.
  function strictParentTracking() {
    return !!(access.waitForParentResume || access.hasOutstandingWork) && config().deferCompleteUntilChildrenIdle
  }

  function invalidateAncestors(sessionID: string) {
    if (!strictParentTracking()) return
    const seen = new Set<string>([sessionID])
    let ancestor = parents.get(sessionID)
    while (ancestor && !seen.has(ancestor)) {
      seen.add(ancestor)
      invalidate(ancestor)
      ancestor = parents.get(ancestor)
    }
  }

  // Keep idle child parent links until explicitly deleted or displaced by newer
  // relationships. Never evict running children or ancestors of running descendants.
  function trimParentLinks() {
    if (parents.size <= MAX_CACHED_CHILD_PARENTS) return
    for (const [id] of parents) {
      if (parents.size <= MAX_CACHED_CHILD_PARENTS) break
      if (running.has(id) || hasRunningChildren(id)) continue
      parents.delete(id)
      children.delete(id)
    }
  }

  function track(sessionID: string, parentID?: string | null) {
    touched.set(sessionID, Date.now())
    if (parentID) {
      const previous = parents.get(sessionID)
      children.add(sessionID)
      parents.delete(sessionID)
      parents.set(sessionID, parentID)
      // Ignore duplicate creation events; they are common when the V2 TUI
      // tracks a child synchronously and later processes the queued event.
      if (previous !== parentID) invalidateAncestors(sessionID)
      // A session can be running before its parent is known.
      holdGrace()
      if (strictParentTracking()) trimParentLinks()
    }
  }

  function hasRunningChildren(parentID: string): boolean {
    for (const id of running) {
      const seen = new Set<string>([id])
      let ancestor = parents.get(id)
      while (ancestor && !seen.has(ancestor)) {
        if (ancestor === parentID) return true
        seen.add(ancestor)
        ancestor = parents.get(ancestor)
      }
    }
    return false
  }

  // A parent whose descendants all stopped alerts after a grace, unless it resumes first. Its next
  // busy drops the alert and its next idle decides again.
  function startGrace() {
    // The V2 TUI can check authoritative outstanding work. After children stop,
    // keep the alert pending until the parent's NEXT successful execution.
    // A fixed grace cannot guarantee the parent has resumed.
    if ((access.waitForParentResume || access.hasOutstandingWork) && config().deferCompleteUntilChildrenIdle) return
    for (const [id, deferred] of pending) {
      if (deferred.grace || hasRunningChildren(id)) continue
      const grace = setTimeout(() => {
        const valid = () => pending.get(id) === deferred && deferred.grace === grace
        void notify("complete", id, { title: deferred.title, key: deferred.key, valid, commit: () => drop(id) }).catch(() => undefined)
      }, CHILDREN_SETTLE_MS)
      grace.unref()
      deferred.grace = grace
    }
  }

  // A descendant that starts during grace holds the alert until it stops again.
  function holdGrace() {
    for (const [id, deferred] of pending) {
      if (!deferred.grace || !hasRunningChildren(id)) continue
      clearTimeout(deferred.grace)
      deferred.grace = undefined
    }
  }

  async function complete(sessionID: string, sequence: number, now: number, key?: string, blockedAtReceipt = false) {
    if (disposed || sequences.get(sessionID) !== sequence) return
    if (errors.delete(sessionID)) return
    if (children.has(sessionID)) {
      await notify("subagent_complete", sessionID, { now, key, valid: () => sequences.get(sessionID) === sequence })
      return
    }
    const info = await access.info(sessionID)
    if (disposed || sequences.get(sessionID) !== sequence || errors.delete(sessionID)) return
    if (info.isChild === null) return
    if (info.isChild) children.add(sessionID)
    const current = config()
    let outstanding = blockedAtReceipt
    if (!outstanding && !info.isChild && current.deferCompleteUntilChildrenIdle && access.hasOutstandingWork) {
      try {
        outstanding = await access.hasOutstandingWork(sessionID)
      } catch {
        // Losing the Job/inbox API must not turn uncertainty into a false "complete".
        outstanding = true
      }
    }
    // A parent may resume while metadata or outstanding work is being queried.
    if (disposed || sequences.get(sessionID) !== sequence) return
    if (!info.isChild && current.deferCompleteUntilChildrenIdle && (hasRunningChildren(sessionID) || outstanding || access.hasObservedOutstandingWork?.(sessionID))) {
      // Expiry drops the pending alert; it must not claim completion while work is still active.
      const timer = setTimeout(() => drop(sessionID), current.deferredCompleteTimeout)
      timer.unref()
      pending.set(sessionID, { title: info.title, key, timer })
      return
    }
    await notify(info.isChild ? "subagent_complete" : "complete", sessionID, {
      title: info.title, now, key,
      // Lookups and delivery claims can race with the next execution.started event.
      valid: () => sequences.get(sessionID) === sequence &&
        (info.isChild || !current.deferCompleteUntilChildrenIdle || !access.hasObservedOutstandingWork?.(sessionID)),
    })
  }

  // Bound tombstones and run state without discarding a live lookup or pending idle.
  const cleanup = setInterval(() => {
    const cutoff = Date.now() - 5 * 60_000
    prunePermissionAlertState(cutoff)
    for (const [id, lastSeen] of touched) {
      if (lastSeen >= cutoff || idleTimers.has(id) || running.has(id) || pending.has(id) || hasRunningChildren(id)) continue
      touched.delete(id)
      children.delete(id)
      sequences.delete(id)
      errors.delete(id)
      // V2 native Subagents may restart after hours of idle time, without
      // another session.created event. Retain bounded ancestry for that case.
      if (!strictParentTracking()) parents.delete(id)
    }
    if (strictParentTracking()) trimParentLinks()
  }, 5 * 60_000)
  cleanup.unref()

  return {
    notify,
    track,
    invalidate,
    settled(id: string) {
      running.delete(id)
      return { sequence: sequences.get(id) ?? invalidate(id), runningChildren: strictParentTracking() && hasRunningChildren(id) }
    },
    completionCurrent(id: string, sequence: number) {
      return sequences.get(id) === sequence
    },
    async created(id: string | null, parentID: string | null, title: string | null, key?: string) {
      if (id) track(id, parentID)
      if (id && parentID) {
        running.add(id)
        holdGrace()
      }
      if (!parentID) await notify("session_started", id, { title, key })
    },
    busy(id: string) {
      invalidate(id)
      errors.delete(id)
      running.add(id)
      // Refresh the LRU position of a reused native child after a long idle.
      const parent = parents.get(id)
      if (parent) {
        parents.delete(id)
        parents.set(id, parent)
      }
      // An existing child may start a new round after its parent has already
      // passed the background Job check. Cancel that parent's in-flight alert.
      invalidateAncestors(id)
      holdGrace()
    },
    async idle(id: string | null, immediate = true, key?: string, blockedAtReceipt = false) {
      if (disposed) return
      if (!id) return notify("complete")
      // Its busy would have dropped the alert, so this is a repeated idle. Keep the current wait.
      if (pending.has(id)) return
      const sequence = invalidate(id)
      running.delete(id)
      const now = Date.now()
      if (immediate) {
        await complete(id, sequence, now, key, blockedAtReceipt)
        startGrace()
        return
      }
      idleTimers.set(id, setTimeout(() => {
        idleTimers.delete(id)
        void complete(id, sequence, now, key, blockedAtReceipt).then(startGrace).catch(() => undefined)
      }, IDLE_COMPLETE_DELAY_MS))
    },
    stopped(id: string) {
      invalidate(id)
      running.delete(id)
      startGrace()
    },
    deleted(id: string) {
      invalidate(id)
      invalidateAncestors(id)
      running.delete(id)
      // Descendants may still be draining after an intermediate child is
      // deleted. Preserve its ancestor link until they all stop.
      if (!hasRunningChildren(id)) {
        children.delete(id)
        parents.delete(id)
      }
      startGrace()
    },
    movedAway(id: string) {
      invalidate(id)
      invalidateAncestors(id)
      running.delete(id)
    },
    async failed(id: string | null, event: "error" | "user_cancelled", key?: string) {
      if (id) {
        invalidate(id)
        errors.add(id)
        running.delete(id)
      }
      await notify(event, id, { key })
      startGrace()
    },
    async permission(id: string | null, requestID: string | null, legacyHook = false, key?: string) {
      if (requestID) {
        await new Promise(resolve => setTimeout(resolve, PERMISSION_PENDING_GRACE_MS))
        if (disposed || !(await access.permissionPending(id, requestID))) return
      }
      if (disposed) return
      // V1 has two permission sources. V2 adapters dedupe by request identity.
      if (legacyHook && shouldSuppressPermissionAlert(id)) return
      await notify("permission", id, { key })
    },
    async userMessage(id: string | null, verifyParent = false, key?: string) {
      if (id && children.has(id)) return
      if (id && verifyParent) {
        const info = await access.info(id)
        if (info.isChild !== false) return
      }
      await notify("user_message", id, { key })
    },
    dispose() {
      disposed = true
      clearInterval(cleanup)
      for (const timer of idleTimers.values()) clearTimeout(timer)
      idleTimers.clear()
      children.clear()
      sequences.clear()
      errors.clear()
      touched.clear()
      for (const id of [...pending.keys()]) drop(id)
      parents.clear()
      running.clear()
    },
  }
}
