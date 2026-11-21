import { describe, test, expect, beforeEach } from "bun:test"
import { shouldSuppressPermissionAlert, resetPermissionAlertState, prunePermissionAlertState } from "./permission-dedupe"

describe("permission dedupe", () => {
  beforeEach(() => {
    resetPermissionAlertState()
  })

  test("suppresses duplicate events in dedupe window for same session", () => {
    expect(shouldSuppressPermissionAlert("session-a", 1000)).toBe(false)
    expect(shouldSuppressPermissionAlert("session-a", 1500)).toBe(true)
    expect(shouldSuppressPermissionAlert("session-a", 2101)).toBe(false)
  })

  test("suppresses immediate hook fallback when event already fired", () => {
    expect(shouldSuppressPermissionAlert("session-a", 1000)).toBe(false)
    expect(shouldSuppressPermissionAlert(null, 1200)).toBe(true)
  })

  test("distinct known requests do not consume each other's dedupe window", () => {
    expect(shouldSuppressPermissionAlert("session-a", 1000, "first")).toBe(false)
    expect(shouldSuppressPermissionAlert("session-a", 1100, "first")).toBe(true)
    expect(shouldSuppressPermissionAlert("session-a", 1200, "second")).toBe(false)
    expect(shouldSuppressPermissionAlert(null, 1300)).toBe(true)
  })

  test("an identified request remains the same when one source omits the session", () => {
    expect(shouldSuppressPermissionAlert(null, 1000, "request")).toBe(false)
    expect(shouldSuppressPermissionAlert("session-a", 1100, "request")).toBe(true)
  })

  test.each(["session-a", null])("an early ID-less alert matches one later request (%s)", session => {
    expect(shouldSuppressPermissionAlert(session, 1000)).toBe(false)
    expect(shouldSuppressPermissionAlert("session-a", 1300, "first")).toBe(true)
    expect(shouldSuppressPermissionAlert("session-a", 1350, "first")).toBe(true)
    expect(shouldSuppressPermissionAlert("session-a", 1400, "second")).toBe(false)
  })

  test("pruning clears stale entries", () => {
    expect(shouldSuppressPermissionAlert("session-a", 1000)).toBe(false)
    prunePermissionAlertState(3000)
    expect(shouldSuppressPermissionAlert("session-a", 3001)).toBe(false)
  })
})
