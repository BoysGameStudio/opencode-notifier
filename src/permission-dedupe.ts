const PERMISSION_DEDUPE_WINDOW_MS = 1000

const sessionLastPermissionAt = new Map<string, number>()
const requestLastPermissionAt = new Map<string, number>()
let globalLastPermissionAt = 0
let unattributedPermissionAt: number | undefined

export function shouldSuppressPermissionAlert(sessionID: string | null, now: number = Date.now(), requestID?: string | null): boolean {
  if (requestID) {
    const previous = requestLastPermissionAt.get(requestID)
    if (previous !== undefined && now - previous < PERMISSION_DEDUPE_WINDOW_MS) return true
    const sessionFallback = sessionID ? sessionLastPermissionAt.get(sessionID) : undefined
    // Match an ID-less alert to at most one identified request. Two distinct
    // identified requests may still be active within the same time window.
    const fallback = sessionFallback !== undefined && now - sessionFallback < PERMISSION_DEDUPE_WINDOW_MS
      ? "session" : unattributedPermissionAt !== undefined && now - unattributedPermissionAt < PERMISSION_DEDUPE_WINDOW_MS ? "global" : undefined
    if (fallback === "session" && sessionID) sessionLastPermissionAt.delete(sessionID)
    if (fallback === "global") unattributedPermissionAt = undefined
    requestLastPermissionAt.delete(requestID)
    requestLastPermissionAt.set(requestID, now)
    if (requestLastPermissionAt.size > 2048) requestLastPermissionAt.delete(requestLastPermissionAt.keys().next().value!)
    globalLastPermissionAt = now
    return !!fallback
  }
  const sessionLastAt = sessionID ? sessionLastPermissionAt.get(sessionID) : undefined
  const latestSeen = Math.max(globalLastPermissionAt, sessionLastAt ?? 0)
  const isDuplicate = latestSeen > 0 && now - latestSeen < PERMISSION_DEDUPE_WINDOW_MS

  if (isDuplicate) {
    return true
  }

  globalLastPermissionAt = now
  if (sessionID) {
    sessionLastPermissionAt.set(sessionID, now)
    if (sessionLastPermissionAt.size > 2048) sessionLastPermissionAt.delete(sessionLastPermissionAt.keys().next().value!)
  } else unattributedPermissionAt = now

  return false
}

export function prunePermissionAlertState(cutoffMs: number): void {
  for (const [sessionID, timestamp] of sessionLastPermissionAt) {
    if (timestamp < cutoffMs) {
      sessionLastPermissionAt.delete(sessionID)
    }
  }
  for (const [request, timestamp] of requestLastPermissionAt) if (timestamp < cutoffMs) requestLastPermissionAt.delete(request)
  if (unattributedPermissionAt !== undefined && unattributedPermissionAt < cutoffMs) unattributedPermissionAt = undefined

  if (globalLastPermissionAt < cutoffMs) {
    globalLastPermissionAt = 0
  }
}

export function resetPermissionAlertState(): void {
  sessionLastPermissionAt.clear()
  requestLastPermissionAt.clear()
  globalLastPermissionAt = 0
  unattributedPermissionAt = undefined
}
