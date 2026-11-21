import { describe, test, expect, beforeEach } from "bun:test"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { claimSoundSlot, playSound, resetSoundState } from "./sound"

describe("claimSoundSlot", () => {
  beforeEach(() => {
    resetSoundState()
  })

  test("first sound claims the slot", () => {
    expect(claimSoundSlot("complete", 10000)).toBe(true)
  })

  test("stacked distinct events within the window are coalesced (#52)", () => {
    expect(claimSoundSlot("permission", 10000)).toBe(true)
    expect(claimSoundSlot("complete", 10400)).toBe(false)
    expect(claimSoundSlot("error", 10800)).toBe(false)
  })

  test("repeat of the same event within the window is dropped", () => {
    expect(claimSoundSlot("complete", 10000)).toBe(true)
    expect(claimSoundSlot("complete", 10500)).toBe(false)
  })

  test("sounds after the window play again", () => {
    expect(claimSoundSlot("permission", 10000)).toBe(true)
    expect(claimSoundSlot("complete", 11000)).toBe(true)
  })

  test("event with no sound file does not consume the slot", async () => {
    // plan_exit has no bundled wav; with no custom path nothing can play.
    await playSound("plan_exit", null, 1)
    expect(claimSoundSlot("complete")).toBe(true)
  })

  test("muted event does not consume the slot", async () => {
    await playSound("complete", null, 0)
    expect(claimSoundSlot("permission")).toBe(true)
  })
})

test.skipIf(process.platform !== "linux")("sound playback bounds concurrent children and stops a timed-out player", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-sound-"))
  try {
    const completed = join(directory, "played")
    writeFileSync(join(directory, "paplay"), `#!${process.execPath}\nawait Bun.sleep(500)\nawait Bun.write(${JSON.stringify(completed)}, "played")\n`, { mode: 0o755 })
    const child = Bun.spawn([process.execPath, "--eval", `
      const { existsSync } = await import("node:fs")
      const deadlines = []
      const originalTimer = globalThis.setTimeout
      globalThis.setTimeout = (callback, duration, ...args) => {
        if (duration !== 30000) return originalTimer(callback, duration, ...args)
        deadlines.push(callback)
        return originalTimer(() => {}, 60000).unref()
      }
      const { playSound } = await import(${JSON.stringify(new URL("./sound.ts", import.meta.url).href)})
      let now = 1000
      Date.now = () => (now += 1001)
      const pending = Array.from({ length: 8 }, () => playSound("complete", null, 1))
      console.log(deadlines.length)
      for (const expire of [...deadlines]) expire()
      await Promise.all(pending)
      console.log(deadlines.length, existsSync(${JSON.stringify(completed)}))
    `], { env: { ...process.env, PATH: directory }, stdout: "pipe", stderr: "pipe" })
    const output = await new Response(child.stdout).text()
    const errors = await new Response(child.stderr).text()
    expect(await child.exited, errors).toBe(0)
    expect(output.trim()).toBe("4\n4 false")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})
