import type { OpenCodeEvent } from "@opencode/client"
import { loadConfig } from "./config"

export type IdleBackground = { kind: string; sessionID: string; childSessionID?: string }

type Round = {
  started: boolean
  ended?: number
  expected: boolean
  inboxID?: string
  resolved: boolean
  observedFor?: Set<string>
  startSequence?: number
  assistantStep?: string
  assistantCompleted?: boolean
  inferredUntil?: number
}

const HISTORY_LIMIT = 4096
const backgroundEvents = new Set<OpenCodeEvent["type"]>([
  "session.created", "session.deleted", "session.execution.started", "session.execution.succeeded",
  "session.execution.failed", "session.execution.interrupted", "session.tool.input.started", "session.tool.called",
  "session.tool.success", "session.tool.failed", "session.inbox.enqueued", "session.inbox.delivered", "session.inbox.cancelled",
])

// The server context exposes live events and idle snapshots, but no Job or
// inbox readback. Keep the expectation alive across child settlement and
// correlate actual inbox delivery by its ID, independently of child activity.
export function createBackgroundObserver({ reportsDirectChildren = false, now = Date.now, uncertaintyTimeout = () => loadConfig().deferredCompleteTimeout } = {}) {
  const parents = new Map<string, string>()
  const rounds = new Map<string, Round[]>()
  const inputs = new Set<string>()
  const calls = new Map<string, { parent: string; began: number }>()
  const inbox = new Map<string, { parent: string; round: Round }>()
  const uncertainUntil = new Map<string, number>()
  let order = 0

  function pruneUncertainty(time: number) {
    for (const [owner, deadline] of uncertainUntil) if (deadline <= time) uncertainUntil.delete(owner)
  }

  function markUncertain(owner: string) {
    const time = now()
    pruneUncertainty(time)
    uncertainUntil.set(owner, time + uncertaintyTimeout())
  }

  function rememberParent(child: string, parent: string) {
    if (!parents.has(child) && parents.size >= HISTORY_LIMIT) {
      const time = now()
      const ancestors = new Set(parents.values())
      const firstCall = new Map<string, number>()
      for (const call of calls.values()) firstCall.set(call.parent, Math.min(firstCall.get(call.parent) ?? Infinity, call.began))
      for (const [id, owner] of parents) {
        const began = firstCall.get(owner) ?? Infinity
        if (ancestors.has(id) || rounds.get(id)?.some(round => round.ended === undefined || round.ended > began || hasPendingResult(round, time))) continue
        parents.delete(id)
        rounds.delete(id)
        break
      }
    }
    return remember(parents, child, parent, parent)
  }

  function remember<K, V>(map: Map<K, V>, key: K, value: V, owner: string) {
    if (!map.has(key) && map.size >= HISTORY_LIMIT) {
      markUncertain(owner)
      return false
    }
    map.delete(key)
    map.set(key, value)
    return true
  }

  function add(child: string, parent: string, started: boolean) {
    const history = rounds.get(child) ?? []
    const round: Round = { started, expected: false, resolved: false }
    if (history.length >= 32) {
      const time = now()
      const retired = history.findIndex(item => item.resolved || (item.ended !== undefined && !hasPendingResult(item, time)))
      if (retired === -1) {
        markUncertain(parent)
        return
      }
      history.splice(retired, 1)
    }
    if (!remember(rounds, child, history, parent)) return
    history.push(round)
    return round
  }

  function expect(parent: string, child: string, began: number) {
    if (!rememberParent(child, parent)) return
    const history = rounds.get(child)
    // A tool can return after its child and even after the inbox delivery.
    // A call into an already running child joins that round. Evidence from a
    // round that ended before this call cannot satisfy its result.
    const round = history?.find(item => item.ended === undefined || item.ended > began) ?? add(child, parent, false)
    if (round) round.expected = true
    return round
  }

  function belongsTo(child: string, parent: string) {
    const seen = new Set<string>()
    for (let id: string | undefined = child; id && !seen.has(id); id = parents.get(id)) {
      if (id === parent) return true
      seen.add(id)
    }
    return false
  }

  function hasPendingResult(round: Round, time: number) {
    return !round.resolved && (round.expected || !!round.inboxID || (round.inferredUntil ?? 0) > time)
  }

  return {
    accepts(event: OpenCodeEvent) {
      if (event.type === "session.step.started" || event.type === "session.step.ended") {
        return reportsDirectChildren && parents.has(event.data.sessionID)
      }
      return backgroundEvents.has(event.type)
    },
    movedAway(sessionID: string) {
      const round = rounds.get(sessionID)?.at(-1)
      // The result still belongs to the parent, although this location may
      // no longer see the child's terminal event.
      if (round && round.ended === undefined) round.ended = ++order
    },
    idle(sessionID: string, background: readonly IdleBackground[]) {
      for (const work of background) {
        if (work.kind !== "subagent" || typeof work.childSessionID !== "string") continue
        let round = rounds.get(work.childSessionID)?.at(-1)
        if (round?.ended !== undefined) {
          // The idle snapshot may have been taken before the terminal event.
          if (!round.resolved) round.expected = true
        } else {
          round = expect(work.sessionID, work.childSessionID, order)
        }
        if (!round && !belongsTo(work.sessionID, sessionID)) markUncertain(sessionID)
        if (round && !round.resolved && !belongsTo(work.sessionID, sessionID)) {
          (round.observedFor ??= new Set()).add(sessionID)
        }
      }
    },
    event(event: OpenCodeEvent) {
      const at = ++order
      switch (event.type) {
        case "session.created":
          if (event.data.parentID) rememberParent(event.data.sessionID, event.data.parentID)
          break
        case "session.execution.started": {
          const id = event.data.sessionID
          const parent = parents.get(id)
          if (!parent) break
          const last = rounds.get(id)?.at(-1)
          const round = last && !last.started && last.ended === undefined ? last : add(id, parent, true)
          if (round) {
            round.started = true
            round.startSequence = event.durable?.seq
          }
          break
        }
        case "session.step.started":
        case "session.step.ended": {
          if (!reportsDirectChildren) break
          const round = rounds.get(event.data.sessionID)?.at(-1)
          if (!round?.started || round.ended !== undefined) break
          if (round.startSequence !== undefined && event.durable?.seq <= round.startSequence) break
          if (event.type === "session.step.started") round.assistantStep = event.data.assistantMessageID
          else if (round.assistantStep === event.data.assistantMessageID && event.data.finish !== "error" && event.data.finish !== "content-filter") {
            round.assistantCompleted = true
          }
          break
        }
        case "session.execution.succeeded":
        case "session.execution.failed":
        case "session.execution.interrupted": {
          const child = event.data.sessionID
          const last = rounds.get(child)?.at(-1)
          if (!last || last.ended !== undefined) break
          last.ended = at
          // OpenCode 2.3 reports direct child turns too. Automation children
          // run quietly, and successful control-only turns produce no report.
          const expectsReport = event.type === "session.execution.succeeded" ? last.assistantCompleted
            : event.type === "session.execution.failed" || event.data.reason !== "shutdown"
          if (reportsDirectChildren && last.started && !child.startsWith("ses_aut_") && expectsReport && !last.expected) {
            last.inferredUntil = now() + uncertaintyTimeout()
          }
          break
        }
        case "session.tool.input.started": {
          const { sessionID, assistantMessageID, id, name } = event.data
          if (name !== "subagent") break
          if (inputs.size < HISTORY_LIMIT) inputs.add(`${sessionID}\0${assistantMessageID}\0${id}`)
          else markUncertain(sessionID)
          break
        }
        case "session.tool.called": {
          const { sessionID, assistantMessageID, id, executed } = event.data
          const key = `${sessionID}\0${assistantMessageID}\0${id}`
          if (inputs.delete(key) && executed) remember(calls, key, { parent: sessionID, began: at }, sessionID)
          break
        }
        case "session.tool.success":
        case "session.tool.failed": {
          const { sessionID, assistantMessageID, id, metadata } = event.data
          const key = `${sessionID}\0${assistantMessageID}\0${id}`
          const call = calls.get(key)
          inputs.delete(key)
          calls.delete(key)
          // Failed tools can retain progress metadata written before launch.
          if (event.type === "session.tool.success" && call && metadata?.status === "running" && typeof metadata.sessionID === "string") {
            expect(sessionID, metadata.sessionID, call.began)
          }
          break
        }
        case "session.inbox.enqueued": {
          const { sessionID, inboxID, item } = event.data
          if (item.type !== "synthetic" || item.payload.metadata?.source !== "subagent") break
          const child = item.payload.metadata.childID
          if (typeof child !== "string") break
          const history = rounds.get(child)
          if (inbox.has(inboxID) || history?.some(round => round.inboxID === inboxID)) break
          if (!rememberParent(child, sessionID)) break
          // Child settlement precedes its report. Use the oldest unmatched
          // settlement, so an older report cannot release a later execution.
          const round = history?.find(item => !item.resolved && (item.expected || item.inferredUntil !== undefined) && item.ended !== undefined && !item.inboxID)
            ?? history?.filter(item => item.ended !== undefined && !item.inboxID).at(-1)
            ?? history?.find(item => item.expected && !item.started && !item.inboxID)
            ?? add(child, sessionID, false)
          if (!round) break
          round.ended ??= at
          round.inboxID = inboxID
          // The retained round bounds the index entry until it is resolved.
          inbox.set(inboxID, { parent: sessionID, round })
          break
        }
        case "session.inbox.delivered":
        case "session.inbox.cancelled": {
          const { inboxID, sessionID } = event.data
          const entry = inbox.get(inboxID)
          if (entry?.parent !== sessionID) break
          entry.round.resolved = true
          inbox.delete(inboxID)
          if (event.type === "session.inbox.cancelled") return sessionID
          break
        }
        case "session.deleted": {
          const id = event.data.sessionID
          uncertainUntil.delete(id)
          for (const [child, history] of rounds) {
            if (child !== id && parents.get(child) !== id) continue
            for (const round of history) {
              // Removing the child does not remove a report already admitted
              // to a different, still-existing parent's inbox.
              const queued = round.inboxID ? inbox.get(round.inboxID) : undefined
              if (queued && queued.parent !== id) continue
              round.ended ??= at
              round.resolved = true
              if (round.inboxID) inbox.delete(round.inboxID)
            }
          }
          for (const [key, call] of calls) if (call.parent === id) calls.delete(key)
          for (const key of inputs) if (key.startsWith(`${id}\0`)) inputs.delete(key)
          break
        }
      }
    },
    outstanding(sessionID: string) {
      const time = now()
      pruneUncertainty(time)
      for (const owner of uncertainUntil.keys()) if (belongsTo(owner, sessionID)) return true
      for (const call of calls.values()) if (belongsTo(call.parent, sessionID)) return true
      for (const [child, history] of rounds) {
        const parent = parents.get(child)
        if (history.some(round => hasPendingResult(round, time) &&
          (round.observedFor?.has(sessionID) || parent && belongsTo(parent, sessionID)))) return true
      }
      return false
    },
  }
}
