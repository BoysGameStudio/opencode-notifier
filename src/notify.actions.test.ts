import { expect, test } from "bun:test"
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import isWsl from "is-wsl"

// WSL reports linux but delivers through WindowsToaster, which would pop real toasts.
const linuxDesktop = process.platform === "linux" && !isWsl

const fixture = join(import.meta.dir, "../test/fixtures/notification-daemon.ts")
const notify = join(import.meta.dir, "notify.ts")

async function run(script: string) {
  const child = Bun.spawn(["dbus-run-session", "--", process.execPath, "--eval", script], { stdout: "pipe", stderr: "pipe" })
  const stdout = await new Response(child.stdout).text()
  const stderr = await new Response(child.stderr).text()
  expect(await child.exited, stderr).toBe(0)
  return stdout.trim()
}

test.skipIf(!linuxDesktop)("the Linux popup backlog is bounded and keeps the newest queued duplicate", async () => {
  const output = await run(`
    const { queueLinuxNotification, disposeLinuxNotifications } = await import(${JSON.stringify(join(import.meta.dir, "notify-linux.ts"))})
    let release
    const head = queueLinuxNotification(() => new Promise(resolve => { release = resolve }), false)
    const overflow = Array.from({ length: 40 }, (_, index) => queueLinuxNotification(async () => index, -1))
    const stale = queueLinuxNotification(async () => "stale", "skipped", "duplicated")
    const fresh = queueLinuxNotification(async () => "fresh", "skipped", "duplicated")
    release(true)
    await head
    console.log((await Promise.all(overflow)).filter(index => index >= 0).length, await stale, await fresh)
    disposeLinuxNotifications()
  `)
  expect(output).toBe("31 skipped fresh")
})

test.skipIf(!linuxDesktop)("plain Linux fallbacks have a separate concurrency limit", async () => {
  const output = await run(`
    const { queueLinuxNotification, disposeLinuxNotifications } = await import(${JSON.stringify(join(import.meta.dir, "notify-linux.ts"))})
    let release, active = 0, max = 0
    const held = new Promise(resolve => { release = resolve })
    const requests = Array.from({ length: 20 }, () => queueLinuxNotification(async () => {
      max = Math.max(++active, max)
      await held
      active--
    }, undefined, undefined, true))
    const expired = queueLinuxNotification(async () => "displayed", "expired", undefined, true, performance.now() - 11000)
    console.log(await queueLinuxNotification(async () => "next popup", "skipped"), active)
    release()
    await Promise.all(requests)
    console.log(max, await expired)
    disposeLinuxNotifications()
  `)
  expect(output).toBe("next popup 4\n4 expired")
})

test.skipIf(!linuxDesktop)("popup retries waiting on a duplicate stay subject to queue capacity", async () => {
  const output = await run(`
    const { queueLinuxNotification } = await import(${JSON.stringify(join(import.meta.dir, "notify-linux.ts"))})
    let release
    const held = new Promise(resolve => { release = resolve })
    const duplicates = Array.from({ length: 40 }, (_, index) =>
      queueLinuxNotification(async () => index, -1, undefined, false, performance.now(), held))
    const ready = await queueLinuxNotification(async () => "ready", "skipped")
    release()
    console.log(ready, (await Promise.all(duplicates)).filter(index => index >= 0).length)
  `)
  expect(output).toBe("ready 31")
})

test.skipIf(!linuxDesktop)("a popup-only prompt that loses its claim does not advance the notification counter", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-popup-counter-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, notification: true, suppressWhenFocused: false, linux: { grouping: true } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { readFileSync } = await import("node:fs")
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { createNotifier } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      const notifier = createNotifier({ info: async () => ({ title: "fixture" }) }, ${JSON.stringify(directory)}, "terminal",
        { claim: async key => key.endsWith("accepted") })
      await channel.sendNotification("fixture", "earlier popup", 1, undefined, "osascript", true, () => {})
      await notifier.notify("question", "session", { key: "skipped" })
      await notifier.notify("question", "session", { key: "accepted" })
      console.log(JSON.parse(readFileSync(${JSON.stringify(join(directory, "opencode-notifier-state.json"))}, "utf8")).turn, daemon.requests.length)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("1 2")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("a permission that expires during a pending check does not claim the popup", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-expired-permission-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, suppressWhenFocused: false }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { createNotifier } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      let now = 0, claims = 0
      Object.defineProperty(performance, "now", { value: () => now })
      const notifier = createNotifier({ info: async () => ({ title: "fixture" }) }, ${JSON.stringify(directory)}, "terminal", {
        claim: async (_key, _session, valid) => { if (!valid()) return false; claims++; return true },
      })
      await notifier.notify("permission", "session", { key: "approval", stillPending: async () => { now = 10001; return true } })
      console.log(claims)
      notifier.dispose()
    `)
    expect(output).toBe("0")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const prompt of [true, false]) test.skipIf(!linuxDesktop)(`the Linux queue preserves distinct events with identical text (prompt: ${prompt})`, async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    const sending = channel.sendNotification("held confirmation", "earlier popup", 1, undefined, "osascript", true, () => {})
    const deadline = Date.now() + 1000
    while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
    if (!daemon.confirmHeldNotification) throw new Error("The earlier popup did not start")
    let valid = true
    const first = channel.sendNotification("first", "permission required", 1, undefined, "osascript", true, () => {},
      undefined, undefined, () => true, undefined, { key: "still-needed", prompt: ${JSON.stringify(prompt)} })
    const second = channel.sendNotification("second", "permission required", 1, undefined, "osascript", true, () => {},
      undefined, undefined, () => valid, undefined, { key: "already-answered", prompt: ${JSON.stringify(prompt)} })
    valid = false
    daemon.confirmHeldNotification()
    await Promise.all([sending, first, second])
    console.log(daemon.requests.length)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("2")
})

test.skipIf(!linuxDesktop)("persistent sender routes one click and retires a replaced callback", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let first = 0, second = 0
    await channel.sendNotification("fixture", "first", 1, undefined, "osascript", true, () => first++)
    await channel.sendNotification("fixture", "second", 1, undefined, "osascript", true, () => { second++; throw Error("callback") })
    const [id] = daemon.visible.keys()
    daemon.notifications.ActionInvoked(id + 1, "focus-terminal")
    daemon.notifications.ActionInvoked(id, "other")
    daemon.notifications.ActionInvoked(id, "focus-terminal")
    daemon.notifications.ActionInvoked(id, "focus-terminal")
    await Bun.sleep(50)
    console.log(first, second, daemon.visible.size, daemon.requests[1].replaces === id)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0 1 1 true")
})

test.skipIf(!linuxDesktop)("ungrouped popups share a persistent sender and keep independent actions", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let first = 0, second = 0
    await Promise.all([
      channel.sendNotification("fixture", "first", 1, undefined, "osascript", false, () => first++),
      channel.sendNotification("fixture", "second", 1, undefined, "osascript", false, () => second++),
    ])
    const [a, b] = daemon.requests
    daemon.notifications.CloseNotification(a.id)
    daemon.notifications.ActionInvoked(a.id, "focus-terminal")
    daemon.notifications.ActionInvoked(b.id, "focus-terminal")
    await Bun.sleep(50)
    console.log(first, second, a.sender === b.sender, a.id !== b.id)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0 1 true true")
})

test.skipIf(!linuxDesktop)("disposal closes the sender and clears actions, then a fresh connection can deliver", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let clicks = 0
    await channel.sendNotification("fixture", "first", 1, undefined, "osascript", false, () => clicks++)
    channel.disposeLinuxNotifications()
    await Bun.sleep(50)
    const old = daemon.requests[0]
    console.log(daemon.visible.get(old.id).actions.length)
    daemon.notifications.ActionInvoked(old.id, "focus-terminal")
    await channel.sendNotification("fixture", "second", 1, undefined, "osascript", false, () => clicks++)
    const fresh = daemon.requests[1]
    daemon.notifications.ActionInvoked(fresh.id, "focus-terminal")
    await Bun.sleep(50)
    console.log(clicks, old.sender !== fresh.sender)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0\n1 true")
})


test.skipIf(!linuxDesktop)("signals arriving before the Notify reply are routed after handler registration", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let clicks = 0
    await channel.sendNotification("fixture", "early", 1, undefined, "osascript", false, () => clicks++)
    await channel.sendNotification("fixture", "closed", 1, undefined, "osascript", false, () => clicks++)
    console.log(clicks)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("1")
})

test.skipIf(!linuxDesktop)("the first actionable popup starts an inactive daemon through bus activation", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-activation-"))
  try {
    // A private bus whose only service is the fixture, so activation never reaches a real daemon.
    writeFileSync(join(directory, "daemon.ts"), `await import(${JSON.stringify(fixture)})\n`)
    writeFileSync(join(directory, "org.freedesktop.Notifications.service"),
      `[D-BUS Service]\nName=org.freedesktop.Notifications\nExec=${process.execPath} ${join(directory, "daemon.ts")}\n`)
    writeFileSync(join(directory, "bus.conf"), `<busconfig><type>session</type><listen>unix:dir=${directory}</listen><auth>EXTERNAL</auth><servicedir>${directory}</servicedir><policy context="default"><allow send_destination="*" eavesdrop="true"/><allow eavesdrop="true"/><allow own="*"/></policy></busconfig>`)
    const child = Bun.spawn(["dbus-run-session", "--config-file", join(directory, "bus.conf"), "--", process.execPath, "--eval", `
      const channel = await import(${JSON.stringify(notify)})
      let clicks = 0
      await channel.sendNotification("fixture", "early", 1, undefined, "osascript", false, () => clicks++)
      console.log(clicks)
      channel.disposeLinuxNotifications()
    `], { stdout: "pipe", stderr: "pipe" })
    const stdout = await new Response(child.stdout).text()
    const stderr = await new Response(child.stderr).text()
    expect(await child.exited, stderr).toBe(0)
    expect(stdout.trim()).toBe("1")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
}, 10000)

test.skipIf(!linuxDesktop)("daemon restart retires old IDs and callbacks before the next delivery", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let first = 0, second = 0
    await channel.sendNotification("fixture", "before restart", 1, undefined, "osascript", true, () => first++)
    await daemon.restartDaemon()
    await Bun.sleep(30)
    await channel.sendNotification("fixture", "after restart", 1, undefined, "osascript", true, () => second++)
    const request = daemon.requests.at(-1)
    daemon.notifications.ActionInvoked(request.id, "focus-terminal")
    await Bun.sleep(30)
    console.log(first, second, request.replaces)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0 1 0")
})

test.skipIf(!linuxDesktop)("handler capacity closes the oldest popup before releasing its action", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    for (let i = 0; i < 128; i++) {
      await channel.sendNotification("fixture", "popup " + i, 1, undefined, "osascript", i === 127, () => {})
    }
    await channel.sendNotification("fixture", "claimed elsewhere", 1, undefined, "osascript", false, () => {},
      undefined, undefined, () => true, async () => false)
    await channel.sendNotification("fixture", "popup 127", 1, undefined, "osascript", false, () => {})
    console.log(daemon.visible.size, daemon.visible.has(daemon.requests[0].id))
    await channel.sendNotification("fixture", "popup 128", 1, undefined, "osascript", false, () => {})
    console.log(daemon.visible.size, daemon.visible.has(daemon.requests[0].id), [...daemon.visible.values()].every(popup => popup.actions.length === 2))
    await channel.sendNotification("foreign replacement", "popup 129", 1, undefined, "osascript", true, () => {})
    console.log(daemon.visible.size, daemon.visible.has(daemon.requests[1].id), daemon.requests.at(-1).replaces)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("128 true\n128 false true\n128 false 0")
})

test.skipIf(!linuxDesktop)("a failed stale action popup does not debounce a fresh completion", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let current = true
    const send = (title, eligible) => channel.sendNotification(title, "identical message", 1, undefined, "osascript", false,
      () => {}, undefined, undefined, eligible)
    const old = send("held failure", () => current)
    const deadline = Date.now() + 1000
    while (Date.now() < deadline && !daemon.rejectHeldNotification) await Bun.sleep(5)
    if (!daemon.rejectHeldNotification) throw new Error("The D-Bus attempt did not start")
    current = false
    const fresh = send("fresh completion", () => true)
    daemon.rejectHeldNotification()
    await Promise.all([old, fresh])
    console.log(daemon.requests.length, daemon.visible.size)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("1 1")
})

test.skipIf(!linuxDesktop)("a stale D-Bus reply preserves its replacement ID without debouncing the new completion", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let current = true
    const send = (title, eligible) => channel.sendNotification(title, "identical message", 1, undefined, "osascript", true,
      () => {}, undefined, undefined, eligible)
    const old = send("held confirmation", () => current)
    const deadline = Date.now() + 1000
    while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
    if (!daemon.confirmHeldNotification) throw new Error("The D-Bus attempt did not start")
    current = false
    const fresh = send("fresh completion", () => true)
    daemon.confirmHeldNotification()
    await Promise.all([old, fresh])
    const [first, second] = daemon.requests
    console.log(daemon.requests.length, daemon.visible.size, second.replaces === first.id)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("2 1 true")
})

for (const plainSucceeds of [true, false]) test.skipIf(!linuxDesktop)(`an unrelated popup can deliver while an actionable request waits for plain delivery (success: ${plainSucceeds})`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-plain-action-order-"))
  const started = join(directory, "plain-started"), release = join(directory, "release-plain")
  try {
    writeFileSync(join(directory, "notify-send"), `#!${process.execPath}
import { existsSync, writeFileSync } from "fs"
if (process.argv[2] === "--version") { console.log("notify-send 0.7.9"); process.exit(0) }
if (process.argv.at(-1) !== "identical popup") process.exit(0)
writeFileSync(${JSON.stringify(started)}, "")
const deadline = Date.now() + 1200
while (Date.now() < deadline && !existsSync(${JSON.stringify(release)})) await Bun.sleep(5)
process.exit(${plainSucceeds ? 0 : 1})
`, { mode: 0o755 })
    const output = await run(`
      const { existsSync, writeFileSync } = await import("fs")
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      process.env.PATH = ${JSON.stringify(directory)}
      const plain = channel.sendNotification("fixture", "identical popup", 1)
      const deadline = Date.now() + 1000
      while (Date.now() < deadline && !existsSync(${JSON.stringify(started)})) await Bun.sleep(5)
      if (!existsSync(${JSON.stringify(started)})) throw new Error("The plain attempt did not start")
      const duplicate = channel.sendNotification("fixture", "identical popup", 1, undefined, "osascript", true, () => {})
      await channel.sendNotification("fixture", "unrelated prompt", 1, undefined, "osascript", ${!plainSucceeds}, ${plainSucceeds ? "() => {}" : "undefined"})
      console.log(daemon.requests.length)
      writeFileSync(${JSON.stringify(release)}, "")
      await Promise.all([plain, duplicate])
      console.log(daemon.requests.length)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe(plainSucceeds ? "1\n1" : "0\n1")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("GNOME rejection creates a fresh popup with the selected action behavior", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-gnome-replacement-"))
  const log = join(directory, "notify-args")
  try {
    writeFileSync(join(directory, "notify-send"), `#!${process.execPath}
import { appendFileSync } from "fs"
const args = process.argv.slice(2)
if (args[0] === "--version") { console.log("notify-send 0.8.0"); process.exit(0) }
appendFileSync(${JSON.stringify(log)}, JSON.stringify(args) + "\\n")
if (args.includes("--replace-id")) {
  console.error("Failed to show notification: GDBus.Error:org.freedesktop.DBus.Error.InvalidArgs: Invalid notification ID")
  process.exit(1)
}
console.log("55")
`, { mode: 0o755 })
    const output = await run(`
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      process.env.PATH = ${JSON.stringify(directory)}
      await channel.sendNotification("fixture", "original popup", 1)
      await channel.sendNotification("fixture", "plain popup", 1)
      let clicked = 0
      await channel.sendNotification("foreign replacement", "new action popup", 1, undefined, "osascript", true, () => clicked++)
      const [shown] = daemon.requests
      daemon.notifications.ActionInvoked(shown.id, "focus-terminal")
      await Bun.sleep(30)
      console.log(shown.replaces, shown.actions.length, clicked)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("0 2 1")
    const sent = readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line) as string[])
    expect(sent).toHaveLength(3)
    expect(sent[0].at(-1)).toBe("original popup")
    expect(sent[1][sent[1].indexOf("--replace-id") + 1]).toBe("55")
    expect(sent[2]).not.toContain("--replace-id")
    expect(sent[2].at(-1)).toBe("plain popup")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("a plain grouped replacement retires the action through its owning D-Bus sender", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let clicked = 0
    await channel.sendNotification("fixture", "action popup", 1, undefined, "osascript", true, () => clicked++)
    await channel.sendNotification("fixture", "plain popup", 1)
    const [before, after] = daemon.requests
    daemon.notifications.ActionInvoked(before.id, "focus-terminal")
    await Bun.sleep(30)
    console.log(clicked, daemon.visible.size, before.sender === after.sender, before.id === after.replaces, after.actions.length)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0 1 true true 0")
})

test.skipIf(!linuxDesktop)("a buffered click after a plain replacement cannot run the old action", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let clicks = 0
    await channel.sendNotification("fixture", "old action", 1, undefined, "osascript", true, () => clicks++)
    await channel.sendNotification("fixture", "early", 1)
    console.log(clicks)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0")
})

test.skipIf(!linuxDesktop)("a queued prompt loses eligibility before attempting the delivery claim", async () => {
  const output = await run(`
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(notify)})
    let eligible = true, claims = 0
    const sending = channel.sendNotification("held confirmation", "first popup", 1, undefined, "osascript", true, () => {})
    const deadline = Date.now() + 1000
    while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
    if (!daemon.confirmHeldNotification) throw new Error("The D-Bus attempt did not start")
    const queued = channel.sendNotification("fixture", "pending permission", 1, undefined, "osascript", true, () => {},
      undefined, undefined, () => eligible, async () => (++claims, true))
    eligible = false
    daemon.confirmHeldNotification()
    await Promise.all([sending, queued])
    console.log(claims, daemon.requests.length)
    channel.disposeLinuxNotifications()
    daemon.stopDaemon()
  `)
  expect(output).toBe("0 1")
})

test.skipIf(!linuxDesktop)("a D-Bus fallback rechecks whether the permission is still waiting", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-fallback-permission-"))
  try {
    const output = await run(`
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      process.env.PATH = ${JSON.stringify(directory)}
      let pending = true, checks = 0
      const popup = channel.sendNotification("held failure", "pending permission", 1, undefined, "osascript", false,
        () => {}, undefined, undefined, () => true, async () => (++checks, pending))
      const deadline = Date.now() + 1000
      while (Date.now() < deadline && !daemon.rejectHeldNotification) await Bun.sleep(5)
      if (!daemon.rejectHeldNotification) throw new Error("The D-Bus attempt did not start")
      pending = false
      daemon.rejectHeldNotification()
      await popup
      console.log(checks, daemon.requests.length)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("2 0")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("the V1 permission hook returns while its pending-list request times out", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-v1-permission-timeout-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, notification: true, events: { client_connected: false } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { NotifierPlugin } = await import(${JSON.stringify(join(import.meta.dir, "index.ts"))})
      let signal
      const client = { _client: { get: options => { signal ??= options.signal; return new Promise(() => {}) } } }
      const plugin = await NotifierPlugin({ client, directory: ${JSON.stringify(directory)} })
      const hook = plugin["permission.ask"]({ id: "permission", sessionID: "v1-session" })
      const returned = await Promise.race([hook.then(() => true), Bun.sleep(150).then(() => false)])
      const deadline = Date.now() + 1500
      while (Date.now() < deadline && !signal?.aborted) await Bun.sleep(5)
      await plugin.dispose()
      console.log(returned, signal?.aborted)
    `)
    expect(output).toBe("true true")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const replyID of ["requestID", "permissionID"]) test.skipIf(!linuxDesktop)(`a V1 permission reply (${replyID}) during the final focus check cancels the popup`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-permission-replied-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, onClickCommand: { enabled: true, path: "unused" },
      events: { client_connected: false } }))
    const output = await run(`
      process.env.OPENCODE_CLIENT = "cli"
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { mock } = await import("bun:test")
      let release, startedProbe
      const held = new Promise(resolve => { release = resolve })
      const started = new Promise(resolve => { startedProbe = resolve })
      let probes = 0
      mock.module(${JSON.stringify(join(import.meta.dir, "focus.ts"))}, () => ({
        captureStartupWindowId: async () => {}, debugFocusState: () => {}, focusTerminal: async () => {},
        isTerminalJumpBackSupported: () => false,
        isTerminalFocused: async () => {
          if (++probes === 2) { startedProbe(); await held }
          return false
        },
      }))
      const daemon = await import(${JSON.stringify(fixture)})
      const { NotifierPlugin } = await import(${JSON.stringify(join(import.meta.dir, "index.ts"))})
      const sessionID = "session", requestID = "approval"
      const client = { _client: { get: async () => [{ id: requestID }] } }
      const plugin = await NotifierPlugin({ client, directory: ${JSON.stringify(directory)} })
      await plugin.event({ event: { type: "permission.asked", properties: { sessionID, id: requestID } } })
      await started
      await plugin.event({ event: { type: "permission.replied", properties: { sessionID, [${JSON.stringify(replyID)}]: requestID } } })
      release()
      await Bun.sleep(80)
      console.log(daemon.requests.length, probes)
      await plugin.dispose()
      daemon.stopDaemon()
    `)
    expect(output).toBe("0 2")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const queuedEvent of ["question", "complete"]) test.skipIf(!linuxDesktop)(`a queued Linux ${queuedEvent} rechecks focus after its sound plays`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-queuing-focus-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: true, notification: true, suppressWhenFocused: true, linux: { grouping: true } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { mock } = await import("bun:test")
      let focused = false, play
      const sound = new Promise(resolve => { play = resolve })
      mock.module(${JSON.stringify(join(import.meta.dir, "focus.ts"))}, () => ({
        debugFocusState: () => {}, focusTerminal: async () => {}, isTerminalFocused: () => focused, isTerminalJumpBackSupported: () => false,
      }))
      mock.module(${JSON.stringify(join(import.meta.dir, "sound.ts"))}, () => ({ playSound: async () => play() }))
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { createNotifier } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      const notifier = createNotifier({ info: async () => ({ title: "fixture" }) }, ${JSON.stringify(directory)}, "terminal", { claim: async () => true })
      const earlier = channel.sendNotification("held confirmation", "earlier popup", 1, undefined, "osascript", true, () => {})
      const deadline = Date.now() + 1000
      while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
      if (!daemon.confirmHeldNotification) throw new Error("The earlier popup did not start")
      const sending = notifier.notify(${JSON.stringify(queuedEvent)}, "session", { key: "queued" })
      await sound
      focused = true
      daemon.confirmHeldNotification()
      await Promise.all([earlier, sending])
      console.log(daemon.requests.length)
      notifier.dispose()
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("1")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const older of ["complete", "permission", "permission-grace"]) test.skipIf(!linuxDesktop)(`a delayed ${older} cannot replace a newer question`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-popup-order-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, showSessionTitle: true, linux: { grouping: true }, onClickCommand: { enabled: true, path: "unused" } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { mock } = await import("bun:test")
      const older = ${JSON.stringify(older)}
      let release, probeStarted
      const held = new Promise(resolve => { release = resolve })
      const started = new Promise(resolve => { probeStarted = resolve })
      let probes = 0
      mock.module(${JSON.stringify(join(import.meta.dir, "focus.ts"))}, () => ({
        debugFocusState: () => {}, focusTerminal: async () => {}, isTerminalJumpBackSupported: () => false,
        isTerminalFocused: async () => {
          if (++probes === 1 && older === "complete") { probeStarted(); await held }
          return false
        },
      }))
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { createNotifier } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      const notifier = createNotifier({ info: async session => {
        if (session === "old-session" && older === "permission") { probeStarted(); await held }
        return { title: session }
      }, permissionPending: async () => true }, ${JSON.stringify(directory)}, "terminal", { claim: async () => true })
      const sending = older === "permission-grace" ? notifier.permission("old-session", "old-permission")
        : notifier.notify(older, "old-session", { key: older })
      if (older !== "permission-grace") await started
      await notifier.notify("question", "new-session", { key: "question" })
      const shownFirst = daemon.requests.length
      release()
      await sending
      console.log(shownFirst, daemon.requests.length, probes, daemon.requests.every(request => request.replaces === 0))
      notifier.dispose()
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe(older === "complete" ? "1 1 2 true" : "1 2 2 true")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("V1 retires a deferred completion before its sound finishes", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-deferred-sound-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ deferCompleteUntilChildrenIdle: true, deferredCompleteTimeout: 2400, sound: true, notification: true,
      suppressWhenFocused: false, events: { subagent_complete: false } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { mock } = await import("bun:test")
      let started, release, canSend, plays = 0
      const playing = new Promise(resolve => { started = resolve })
      const held = new Promise(resolve => { release = resolve })
      mock.module(${JSON.stringify(join(import.meta.dir, "sound.ts"))}, () => ({ playSound: async () => {
        plays++
        started()
        await held
      } }))
      mock.module(${JSON.stringify(notify)}, () => ({
        usesLinuxDesktopNotifications: () => true,
        sendNotification: async (...args) => { canSend = args[9]; await held },
      }))
      const { createNotifier, CHILDREN_SETTLE_MS } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      const notifier = createNotifier({ info: async () => ({ title: "fixture", isChild: false }) }, ${JSON.stringify(directory)})
      notifier.track("child", "root")
      notifier.busy("child")
      await notifier.idle("root", true)
      notifier.stopped("child")
      await playing
      await Bun.sleep(500)
      const initiallyEligible = canSend()
      notifier.busy("child")
      const stillEligible = canSend()
      notifier.stopped("child")
      await Bun.sleep(CHILDREN_SETTLE_MS + 50)
      console.log(plays, initiallyEligible, stillEligible, canSend())
      release()
      notifier.dispose()
    `)
    expect(output).toBe("1 true false false")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
}, 7000)

test.skipIf(!linuxDesktop)("V2 retains prompt order while an earlier permission waits for routing", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-prompt-routing-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, suppressWhenFocused: false, linux: { grouping: true },
      onClickCommand: { enabled: true, path: "unused" } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { createV2Notifier } = await import(${JSON.stringify(join(import.meta.dir, "v2.ts"))})
      const location = { directory: ${JSON.stringify(directory)} }
      let ready, release
      const started = new Promise(resolve => { ready = resolve })
      const held = new Promise(resolve => { release = resolve })
      const notifier = createV2Notifier({
        session: { get: async ({ sessionID }) => {
          if (sessionID === "old") { ready(); await held }
          return { location }
        } },
        permission: { list: async () => [{ id: "approval" }] },
      }, location, "terminal", { claim: async () => true })
      const old = { id: "old", type: "permission.asked", data: { sessionID: "old", id: "approval" } }
      const next = { id: "new", location, type: "form.created", data: { form: { id: "question", sessionID: "new", metadata: { kind: "question" } } } }
      const sending = notifier.event(old, notifier.received(old))
      await started
      await notifier.event(next, notifier.received(next))
      const firstDeadline = Date.now() + 1000
      while (!daemon.requests.length && Date.now() < firstDeadline) await Bun.sleep(5)
      if (!daemon.requests.length) throw new Error("The newer question did not show")
      release()
      await sending
      const deadline = Date.now() + 1000
      while (daemon.requests.length < 2 && Date.now() < deadline) await Bun.sleep(5)
      console.log(daemon.requests.length, daemon.requests.every(request => request.replaces === 0))
      notifier.dispose()
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("2 true")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const settlement of [undefined, "question.replied", "question.rejected", "session.idle", "session.deleted"]) test.skipIf(!linuxDesktop)(
  `a V1 question does not wait for the Linux queue and respects its settlement (${settlement ?? "open"})`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-v1-tool-queue-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound: false, notification: true, suppressWhenFocused: false,
      linux: { grouping: true }, events: { client_connected: false, complete: false } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { NotifierPlugin } = await import(${JSON.stringify(join(import.meta.dir, "index.ts"))})
      const plugin = await NotifierPlugin({ client: {}, directory: ${JSON.stringify(directory)} })
      const sending = channel.sendNotification("held confirmation", "earlier popup", 1, undefined, "osascript", true, () => {})
      const deadline = Date.now() + 1000
      while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
      if (!daemon.confirmHeldNotification) throw new Error("The earlier popup did not start")
      const hook = plugin["tool.execute.before"]({ tool: "question", sessionID: "v1-session", callID: "call" })
      const didNotWait = await Promise.race([hook.then(() => true), Bun.sleep(250).then(() => false)])
      await plugin.event({ event: { type: "question.asked", properties: { sessionID: "v1-session", id: "request", tool: { callID: "call" } } } })
      const settlement = ${JSON.stringify(settlement ?? null)}
      if (settlement) await plugin.event({ event: { type: settlement,
        properties: { sessionID: "v1-session", requestID: "request", info: { id: "v1-session" } } } })
      daemon.confirmHeldNotification()
      await Promise.all([sending, hook])
      await Bun.sleep(100)
      console.log(didNotWait, daemon.requests.length)
      await plugin.dispose()
      daemon.stopDaemon()
    `)
    expect(output).toBe(settlement ? "true 1" : "true 2")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const [sound, timeout] of [[false, false], [true, false], [true, true]]) test.skipIf(!linuxDesktop)(
  `a queued Linux permission uses a bounded pending check (sound: ${sound}, timeout: ${timeout})`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-queued-permission-"))
  try {
    const config = join(directory, "notifier.json")
    writeFileSync(config, JSON.stringify({ sound, bell: false, notification: true, suppressWhenFocused: false, linux: { grouping: true },
      events: { permission: { notification: true, sound, bell: false, command: false } } }))
    const output = await run(`
      process.env.OPENCODE_NOTIFIER_CONFIG_PATH = ${JSON.stringify(config)}
      const { mock } = await import("bun:test")
      let sounds = 0
      mock.module(${JSON.stringify(join(import.meta.dir, "sound.ts"))}, () => ({ playSound: async () => { sounds++ } }))
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      const { createNotifier } = await import(${JSON.stringify(join(import.meta.dir, "notifier.ts"))})
      let pending = true, checks = 0, claims = 0, pendingSignal
      const notifier = createNotifier({ info: async () => ({ title: "fixture" }),
        permissionPending: async (_session, _request, signal) => {
          if (++checks === 2 && ${JSON.stringify(timeout)}) {
            pendingSignal = signal
            return new Promise(() => {})
          }
          return pending
        }, elapsed: async () => null }, ${JSON.stringify(directory)}, "terminal",
        { claim: async () => (++claims, true) })
      const sending = channel.sendNotification("held confirmation", "earlier popup", 1, undefined, "osascript", true, () => {})
      const deadline = Date.now() + 2000
      while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
      if (!daemon.confirmHeldNotification) throw new Error("The D-Bus attempt did not start")
      const permission = notifier.permission("session", "permission", false, "permission-key")
      while (Date.now() < deadline && !checks) await Bun.sleep(5)
      pending = false
      daemon.confirmHeldNotification()
      await Promise.all([sending, permission])
      console.log(checks, claims, daemon.requests.length, sounds, pendingSignal?.aborted ?? false)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe(timeout ? "2 1 2 1 true" : sound ? "2 1 1 1 false" : "2 0 1 0 false")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

for (const body of ["plain popup", "hang"]) test.skipIf(!linuxDesktop)(`a failed plain replacement preserves unrelated D-Bus actions (${body})`, async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-replacement-error-"))
  try {
    const output = await run(`
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      process.env.PATH = ${JSON.stringify(directory)}
      let grouped = 0, unrelated = 0
      await channel.sendNotification("fixture", "grouped action", 1, undefined, "osascript", true, () => grouped++)
      await channel.sendNotification("fixture", "unrelated action", 1, undefined, "osascript", false, () => unrelated++)
      await channel.sendNotification(${JSON.stringify(body === "hang" ? "fixture" : "rejected replacement")}, ${JSON.stringify(body)}, 1)
      for (const request of daemon.requests) daemon.notifications.ActionInvoked(request.id, "focus-terminal")
      await Bun.sleep(30)
      console.log(grouped, unrelated, daemon.visible.size)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe(body === "hang" ? "0 1 2" : "1 1 2")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop)("a timed-out actionable replacement cannot run the previous session's click", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-action-timeout-"))
  try {
    const output = await run(`
      const daemon = await import(${JSON.stringify(fixture)})
      const channel = await import(${JSON.stringify(notify)})
      process.env.PATH = ${JSON.stringify(directory)}
      let previous = 0, replacement = 0, unrelated = 0
      await channel.sendNotification("fixture", "previous session", 1, undefined, "osascript", true, () => previous++)
      await channel.sendNotification("fixture", "unrelated popup", 1, undefined, "osascript", false, () => unrelated++)
      const sending = channel.sendNotification("held confirmation", "new session", 1, undefined, "osascript", true, () => replacement++)
      const deadline = Date.now() + 1000
      while (Date.now() < deadline && !daemon.confirmHeldNotification) await Bun.sleep(5)
      if (!daemon.confirmHeldNotification) throw new Error("The replacement was not displayed")
      daemon.notifications.ActionInvoked(daemon.requests[0].id, "focus-terminal")
      daemon.notifications.ActionInvoked(daemon.requests[1].id, "focus-terminal")
      await sending
      console.log(previous, replacement, unrelated)
      channel.disposeLinuxNotifications()
      daemon.stopDaemon()
    `)
    expect(output).toBe("0 0 1")
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test.skipIf(!linuxDesktop || !Bun.which("node"))("pending delivery completes on Node but its idle sender does not keep the client alive", async () => {
  const directory = mkdtempSync(join(tmpdir(), "notifier-node-runtime-"))
  try {
    const result = await Bun.build({ entrypoints: [notify], target: "node", outdir: directory, naming: "notify.mjs", external: ["@particle/dbus-next", "node-notifier"] })
    expect(result.success).toBe(true)
    symlinkSync(join(import.meta.dir, "../node_modules"), join(directory, "node_modules"))
    const output = await run(`
      const daemon = await import(${JSON.stringify(fixture)})
      const script = 'const channel = await import(' + ${JSON.stringify(JSON.stringify(join(directory, "notify.mjs")))} + '); await channel.sendNotification("fixture", "exiting client", 1, undefined, "osascript", false, () => {}); console.log("done")'
      const client = Bun.spawn([${JSON.stringify(Bun.which("node"))}, "--input-type=module", "--eval", script], { stdout: "pipe", stderr: "pipe" })
      const timer = setTimeout(() => client.kill(), 1000)
      const code = await client.exited
      clearTimeout(timer)
      console.log(code, (await new Response(client.stdout).text()).trim())
      daemon.stopDaemon()
    `)
    expect(output).toBe("0 done")
  } finally {
    rmSync(directory, {recursive:true,force:true})
  }
})
