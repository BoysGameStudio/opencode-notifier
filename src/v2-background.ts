import type { OpenCodeEvent } from "@opencode/client"

export type IdleBackground = { kind: string; sessionID: string; childSessionID?: string }

type Round = {
  began: number
  started: boolean
  ended?: number
  expected: boolean
  inboxID?: string
  delivered: boolean
  observedFor?: Set<string>
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
export function createBackgroundObserver() {
  const parents = new Map<string, string>()
  const rounds = new Map<string, Round[]>()
  const inputs = new Set<string>()
  const calls = new Map<string, { parent: string; began: number }>()
  const inbox = new Map<string, { child: string; round: Round }>()
  let order = 0
  let saturated = false

  function rememberParent(child: string, parent: string) {
    if (!parents.has(child) && parents.size >= HISTORY_LIMIT) {
      const ancestors = new Set(parents.values())
      const firstCall = new Map<string, number>()
      for (const call of calls.values()) firstCall.set(call.parent, Math.min(firstCall.get(call.parent) ?? Infinity, call.began))
      for (const [id, owner] of parents) {
        const began = firstCall.get(owner) ?? Infinity
        if (ancestors.has(id) || rounds.get(id)?.some(round => round.ended === undefined || round.ended > began ||
          (!round.delivered && (round.expected || round.inboxID)))) continue
        parents.delete(id)
        rounds.delete(id)
        break
      }
    }
    remember(parents, child, parent)
  }

  function remember<K, V>(map: Map<K, V>, key: K, value: V) {
    if (!map.has(key) && map.size >= HISTORY_LIMIT) {
      saturated = true
      return false
    }
    map.delete(key)
    map.set(key, value)
    return true
  }

  function add(child: string, began: number, started: boolean) {
    const history = rounds.get(child) ?? []
    const round: Round = { began, started, expected: false, delivered: false }
    if (history.length >= 32) {
      const retired = history.findIndex(item => item.delivered || (!item.expected && !item.inboxID))
      if (retired === -1) {
        saturated = true
        return round
      }
      history.splice(retired, 1)
    }
    if (remember(rounds, child, history)) history.push(round)
    return round
  }

  function expect(parent: string, child: string, began: number) {
    rememberParent(child, parent)
    const history = rounds.get(child)
    // A tool can return after its child and even after the inbox delivery.
    // A call into an already running child joins that round. Evidence from a
    // round that ended before this call cannot satisfy its result.
    const round = history?.find(item => item.ended === undefined || item.ended > began) ?? add(child, began, false)
    round.expected = true
  }

  function belongsTo(child: string, parent: string) {
    const seen = new Set<string>()
    for (let id: string | undefined = child; id && !seen.has(id); id = parents.get(id)) {
      if (id === parent) return true
      seen.add(id)
    }
    return false
  }

  return {
    accepts(event: OpenCodeEvent) { return backgroundEvents.has(event.type) },
    idle(sessionID: string, background: readonly IdleBackground[]) {
      for (const work of background) {
        if (work.kind !== "subagent" || typeof work.childSessionID !== "string") continue
        const active = rounds.get(work.childSessionID)?.at(-1)
        if (active?.ended !== undefined) {
          // The idle snapshot may have been taken before the terminal event.
          if (!active.delivered) active.expected = true
        } else {
          expect(work.sessionID, work.childSessionID, order)
        }
        const round = rounds.get(work.childSessionID)?.at(-1)
        if (round && !round.delivered && !belongsTo(work.sessionID, sessionID)) {
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
          if (!parents.has(id)) break
          const last = rounds.get(id)?.at(-1)
          if (last && !last.started && last.ended === undefined) last.started = true
          else add(id, at, true)
          break
        }
        case "session.execution.succeeded":
        case "session.execution.failed":
        case "session.execution.interrupted": {
          const child = event.data.sessionID
          const last = rounds.get(child)?.at(-1)
          if (last && last.ended === undefined) last.ended = at
          break
        }
        case "session.tool.input.started": {
          const { sessionID, assistantMessageID, id, name } = event.data
          if (name !== "subagent") break
          if (inputs.size < HISTORY_LIMIT) inputs.add(`${sessionID}\0${assistantMessageID}\0${id}`)
          else saturated = true
          break
        }
        case "session.tool.called": {
          const { sessionID, assistantMessageID, id, executed } = event.data
          const key = `${sessionID}\0${assistantMessageID}\0${id}`
          if (inputs.delete(key) && executed) remember(calls, key, { parent: sessionID, began: at })
          break
        }
        case "session.tool.success":
        case "session.tool.failed": {
          const { sessionID, assistantMessageID, id, metadata } = event.data
          const key = `${sessionID}\0${assistantMessageID}\0${id}`
          const call = calls.get(key)
          inputs.delete(key)
          calls.delete(key)
          if (call && metadata?.status === "running" && typeof metadata.sessionID === "string") {
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
          rememberParent(child, sessionID)
          // Child settlement precedes its report. Use the oldest unmatched
          // settlement, so an older report cannot release a later execution.
          const round = history?.find(item => item.expected && item.ended !== undefined && !item.inboxID)
            ?? history?.filter(item => item.ended !== undefined && !item.inboxID).at(-1)
            ?? history?.find(item => item.expected && !item.started && !item.inboxID)
            ?? add(child, at, false)
          round.ended ??= at
          round.inboxID = inboxID
          remember(inbox, inboxID, { child, round })
          break
        }
        case "session.inbox.delivered":
        case "session.inbox.cancelled": {
          const entry = inbox.get(event.data.inboxID)
          if (!entry || parents.get(entry.child) !== event.data.sessionID) break
          if (event.type === "session.inbox.delivered") entry.round.delivered = true
          else entry.round.expected = true
          inbox.delete(event.data.inboxID)
          break
        }
        case "session.deleted": {
          const id = event.data.sessionID
          for (const [key, call] of calls) if (call.parent === id) calls.delete(key)
          for (const key of inputs) if (key.startsWith(`${id}\0`)) inputs.delete(key)
          if (!parents.has(id)) {
            for (const [child] of parents) {
              if (belongsTo(child, id) && !(rounds.get(child)?.some(round => round.expected && !round.delivered))) rounds.delete(child)
            }
          }
          break
        }
      }
    },
    outstanding(sessionID: string) {
      if (saturated) return true
      for (const call of calls.values()) if (belongsTo(call.parent, sessionID)) return true
      for (const [child, history] of rounds) {
        const parent = parents.get(child)
        if (history.some(round => !round.delivered && (round.expected || round.inboxID) &&
          (round.observedFor?.has(sessionID) || parent && belongsTo(parent, sessionID)))) return true
      }
      return false
    },
  }
}
