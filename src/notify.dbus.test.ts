import { afterAll, expect, test } from "bun:test"
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import isWsl from "is-wsl"

// WSL reports linux but delivers through WindowsToaster, which would pop real toasts.
const linuxDesktop = process.platform === "linux" && !isWsl

const directory = mkdtempSync(join(tmpdir(), "notifier-dbus-failure-"))
afterAll(() => rmSync(directory, { recursive: true, force: true }))

test.skipIf(!linuxDesktop)("a broken D-Bus connection fails open with a plain popup", async () => {
  const log = join(directory, "notify-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}\nimport { writeFileSync } from "fs"\nwriteFileSync(${JSON.stringify(log)}, JSON.stringify(process.argv.slice(2)))\nconsole.log("1")\n`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification, disposeLinuxNotifications } = await import(${JSON.stringify(modulePath)})
    await sendNotification("fixture", "fallback", 1, undefined, "osascript", false, () => { throw Error("no click") })
    disposeLinuxNotifications()
    console.log("done")
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "unix:path=/does-not-exist/notifier-fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const output = await new Response(child.stdout).text()
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(output.trim()).toBe("done")
  const args = JSON.parse(readFileSync(log, "utf8"))
  expect(args).toContain("fallback")
  expect(args).not.toContain("--action")
})


test.skipIf(!linuxDesktop)("a hung daemon is bounded and a disposed delivery cannot create a late popup", async () => {
  const log = join(directory, "hung-notify-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}\nimport { appendFileSync } from "fs"\nappendFileSync(${JSON.stringify(log)}, "plain\\n")\nconsole.log("1")\n`)
  chmodSync(executable, 0o755)
  const fixture = join(import.meta.dir, "../test/fixtures/notification-daemon.ts")
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn(["dbus-run-session", "--", process.execPath, "--eval", `
    const daemon = await import(${JSON.stringify(fixture)})
    const channel = await import(${JSON.stringify(modulePath)})
    const start = Date.now()
    await channel.sendNotification("fixture", "hang", 1, undefined, "osascript", false, () => {})
    console.log(Date.now() - start < 3000)
    await Bun.sleep(1100)
    const sending = channel.sendNotification("fixture", "hang", 1, undefined, "osascript", false, () => {})
    await Bun.sleep(50)
    channel.disposeLinuxNotifications()
    await sending
    daemon.stopDaemon()
  `], {
    env: { ...process.env, PATH: directory + ":" + process.env.PATH }, stdout: "pipe", stderr: "pipe",
  })
  const output = await new Response(child.stdout).text()
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(output.trim()).toBe("true")
  expect(readFileSync(log, "utf8")).toBe("plain\n")
}, 10000)

test.skipIf(!linuxDesktop)("a broken D-Bus connection with grouping still pops on notify-send older than 0.8", async () => {
  const log = join(directory, "old-notify-args")
  const executable = join(directory, "notify-send")
  // 0.7 has no --print-id or --replace-id and rejects unknown flags.
  writeFileSync(executable, `#!${process.execPath}\nimport { writeFileSync } from "fs"\nconst args = process.argv.slice(2)\nif (args[0] === "--version") { console.log("notify-send 0.7.9"); process.exit(0) }\nif (args.some(arg => arg === "--print-id" || arg === "--replace-id")) process.exit(1)\nwriteFileSync(${JSON.stringify(log)}, JSON.stringify(args))\n`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification, disposeLinuxNotifications } = await import(${JSON.stringify(modulePath)})
    await sendNotification("fixture", "old fallback", 1, undefined, "osascript", true, () => {})
    disposeLinuxNotifications()
  `], {
    env: { ...process.env, PATH: directory + ":" + process.env.PATH, DBUS_SESSION_BUS_ADDRESS: "unix:path=/does-not-exist/notifier-fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(JSON.parse(readFileSync(log, "utf8"))).toContain("old fallback")
})

test.skipIf(!linuxDesktop)("plain Linux popups use the OpenCode app name", async () => {
  const log = join(directory, "app-name-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}\nimport { writeFileSync } from "fs"\nwriteFileSync(${JSON.stringify(log)}, JSON.stringify(process.argv.slice(2)))\n`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    await sendNotification("OpenCode", "app-name test", 1, undefined, "osascript", false)
  `], {
    env: { ...process.env, PATH: directory + ":" + process.env.PATH, DBUS_SESSION_BUS_ADDRESS: "fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  const args: string[] = JSON.parse(readFileSync(log, "utf8"))
  expect(args[args.indexOf("--app-name") + 1]).toBe("opencode")
})

for (const grouping of [false, true]) test.skipIf(!linuxDesktop)(`an ungrouped fallback does not hold the next action popup (grouping requested: ${grouping})`, async () => {
  const log = join(directory, `ungrouped-notify-args-${grouping}`)
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync, readFileSync } from "fs"
const log = ${JSON.stringify(log)}
if (process.argv[2] === "--version") { console.log("notify-send 0.7.9"); process.exit(0) }
if (process.argv.at(-1) === "slow ungrouped popup") {
  appendFileSync(log, "slow started\\n")
  const deadline = Date.now() + 1200
  while (Date.now() < deadline && !readFileSync(log, "utf8").includes("fast sent")) await Bun.sleep(10)
  appendFileSync(log, "slow finished\\n")
} else appendFileSync(log, "fast sent\\n")
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { existsSync } = await import("fs")
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    const send = message => sendNotification("fixture", message, 1, undefined, "osascript", ${grouping}, () => {})
    const slow = send("slow ungrouped popup")
    const deadline = Date.now() + 2000
    while (Date.now() < deadline && !existsSync(${JSON.stringify(log)})) await Bun.sleep(5)
    await Promise.all([slow, send("fast ungrouped popup")])
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "unix:path=/does-not-exist/notifier-fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(readFileSync(log, "utf8").trim().split("\n")).toEqual(["slow started", "fast sent", "slow finished"])
})

test.skipIf(!linuxDesktop)("invalidation during Linux capability detection does not debounce the next popup", async () => {
  const log = join(directory, "late-popup-args")
  const probing = join(directory, "capability-probe-started")
  const resume = join(directory, "capability-probe-resume")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync, existsSync, writeFileSync } from "fs"
if (process.argv[2] === "--version") {
  writeFileSync(${JSON.stringify(probing)}, "")
  const deadline = Date.now() + 800
  while (Date.now() < deadline && !existsSync(${JSON.stringify(resume)})) await Bun.sleep(5)
  console.log("notify-send 0.8.0")
} else appendFileSync(${JSON.stringify(log)}, process.argv.at(-2) + "\\n")
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { existsSync, writeFileSync } = await import("fs")
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    const send = (title, eligible) => sendNotification(title, "same message", 1, undefined, "osascript", true, undefined, undefined, undefined, eligible)
    let current = true
    const old = send("stale", () => current)
    const deadline = Date.now() + 1200
    while (!existsSync(${JSON.stringify(probing)}) && Date.now() < deadline) await Bun.sleep(5)
    if (!existsSync(${JSON.stringify(probing)})) throw new Error("The capability probe did not start")
    current = false
    writeFileSync(${JSON.stringify(resume)}, "")
    await Promise.all([old, send("fresh", () => true)])
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(readFileSync(log, "utf8")).toBe("fresh\n")
})

test.skipIf(!linuxDesktop)("a duplicate submitted during a slow grouped dispatch remains debounced", async () => {
  const log = join(directory, "duplicate-popup-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync } from "fs"
if (process.argv[2] === "--version") { console.log("notify-send 0.8.0"); process.exit(0) }
appendFileSync(${JSON.stringify(log)}, process.argv.at(-2) + "\\n")
if (process.argv.at(-2) === "original") await Bun.sleep(1150)
console.log("55")
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    await Promise.all([
      sendNotification("original", "identical popup", 1),
      sendNotification("duplicate", "identical popup", 1),
    ])
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(readFileSync(log, "utf8")).toBe("original\n")
})

test.skipIf(!linuxDesktop)("grouped requests keep their original order when a repeated message follows a failed attempt", async () => {
  const log = join(directory, "grouped-order-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync } from "fs"
if (process.argv[2] === "--version") { console.log("notify-send 0.8.0"); process.exit(0) }
const title = process.argv.at(-2)
appendFileSync(${JSON.stringify(log)}, title + "\\n")
if (title === "failed") { await Bun.sleep(50); process.exit(1) }
console.log("55")
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    await Promise.all([
      sendNotification("failed", "repeated", 1),
      sendNotification("second", "repeated", 1),
      sendNotification("latest", "different", 1),
    ])
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(readFileSync(log, "utf8")).toBe("failed\nsecond\nlatest\n")
})

test.skipIf(!linuxDesktop)("a disposed plain send cannot delay or debounce the next generation", async () => {
  const log = join(directory, "generation-popup-args")
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync, readFileSync } from "fs"
const log = ${JSON.stringify(log)}
const title = process.argv.at(-2)
appendFileSync(log, title + "\\n")
if (title === "old") {
  const deadline = Date.now() + 1200
  while (Date.now() < deadline && !readFileSync(log, "utf8").includes("fresh")) await Bun.sleep(5)
  appendFileSync(log, "old finished\\n")
}
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { existsSync } = await import("fs")
    const { sendNotification, disposeLinuxNotifications } = await import(${JSON.stringify(modulePath)})
    const send = title => sendNotification(title, "same message", 1, undefined, "osascript", false)
    const old = send("old")
    const deadline = Date.now() + 1200
    while (Date.now() < deadline && !existsSync(${JSON.stringify(log)})) await Bun.sleep(5)
    if (!existsSync(${JSON.stringify(log)})) throw new Error("The old send did not start")
    disposeLinuxNotifications()
    await Promise.all([old, send("fresh")])
    await send("duplicate")
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  expect(readFileSync(log, "utf8")).toBe("old\nfresh\nold finished\n")
})

for (const firstAction of [false, true]) test.skipIf(!linuxDesktop)(`concurrent grouped popups retain order with first action ${firstAction}`, async () => {
  const log = join(directory, `grouped-notify-args-${firstAction}`)
  const executable = join(directory, "notify-send")
  writeFileSync(executable, `#!${process.execPath}
import { appendFileSync } from "fs"
const args = process.argv.slice(2)
if (args[0] === "--version") { console.log("notify-send 0.8.0"); process.exit(0) }
appendFileSync(${JSON.stringify(log)}, JSON.stringify(args) + "\\n")
if (args.includes("first fixture popup")) await Bun.sleep(100)
console.log("55")
`)
  chmodSync(executable, 0o755)
  const modulePath = join(import.meta.dir, "notify.ts")
  const child = Bun.spawn([process.execPath, "--eval", `
    const { sendNotification } = await import(${JSON.stringify(modulePath)})
    let eligible = true
    const first = sendNotification("fixture", "first fixture popup", 1, undefined, "osascript", true, ${firstAction ? "() => {}" : "undefined"})
    const stale = sendNotification("fixture", "second fixture popup", 1, undefined, "osascript", true, () => {}, undefined, undefined, () => eligible)
    eligible = false
    await Promise.all([
      first, stale,
      sendNotification("fixture", "second fixture popup", 1, undefined, "osascript", true),
    ])
  `], {
    env: { ...process.env, PATH: directory, DBUS_SESSION_BUS_ADDRESS: "unix:path=/does-not-exist/notifier-fixture" },
    stdout: "pipe", stderr: "pipe",
  })
  const error = await new Response(child.stderr).text()
  expect(await child.exited, error).toBe(0)
  const sent = readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line) as string[])
  expect(sent).toHaveLength(2)
  const [first, second] = sent
  expect(first.at(-1)).toBe("first fixture popup")
  expect(first).not.toContain("--replace-id")
  expect(second.at(-1)).toBe("second fixture popup")
  expect(second[second.indexOf("--replace-id") + 1]).toBe("55")
})
