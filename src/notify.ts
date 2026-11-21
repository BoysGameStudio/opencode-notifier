import os from "os"
import { execFile } from "child_process"
import { getLinuxNotificationID, hasLinuxDBusReplacement, linuxDeliveryGuard, LINUX_NOTIFICATION_WAIT_MS, nextLinuxNotificationOrder, queueLinuxNotification, rememberLinuxNotificationID, sendLinuxDBusNotification } from "./notify-linux"
export { disposeLinuxNotifications } from "./notify-linux"
import notifier from "node-notifier"
import isWsl from "is-wsl"
import { ensureSnoreToastExecutable } from "./snoretoast"

const DEBOUNCE_MS = 1000

const platform = os.type()

let platformNotifier: { notify(notification: notifier.Notification, callback: notifier.NotificationCallback): unknown } = notifier

if (platform === "Windows_NT" || isWsl) {
  const { WindowsToaster } = notifier
  // npm ships the snoreToast executable without +x, which WSL needs to launch it.
  if (isWsl) ensureSnoreToastExecutable()
  platformNotifier = new WindowsToaster({ withFallback: false })
} else if (platform !== "Darwin") {
  platformNotifier = notifier
}

export type NotificationAction = "focus" | "close"

const LINUX_FOCUS_ACTION_KEY = "focus-terminal"
const LINUX_FOCUS_ACTION_LABEL = "Jump to terminal"

const lastNotifications = new Map<string, { requestedAt: number; channelCurrent?: () => boolean }>()
const linuxDeliveries = new Map<string, { result: Promise<void>; channelCurrent: () => boolean }>()
let lastGroupedDelivery: { order: number; channelCurrent: () => boolean } | undefined

let linuxNotifySendSupportsReplace: boolean | null = null

export function usesLinuxDesktopNotifications(system: string): boolean {
  return system !== "ghostty" && (platform === "Linux" || /BSD$/.test(platform)) && !isWsl
}

function sanitizeGhosttyField(value: string): string {
  return value.replace(/[;\u0000-\u001f\u007f-\u009f]/g, "")
}

export function formatGhosttyNotificationSequence(
  title: string,
  message: string,
  env: NodeJS.ProcessEnv = process.env
): string {
  const escapedTitle = sanitizeGhosttyField(title)
  const escapedMessage = sanitizeGhosttyField(message)
  const payload = `\x1b]9;${escapedTitle}: ${escapedMessage}\x07`

  if (env.TMUX) {
    return `\x1bPtmux;\x1b${payload}\x1b\\`
  }

  return payload
}

function runNotifySend(args: string[], timeout: number): Promise<{ stdout: string; invalidID?: boolean } | undefined> {
  return new Promise(resolve => {
    const deadline = setTimeout(() => resolve(undefined), timeout)
    deadline.unref()
    execFile("notify-send", args, { timeout, killSignal: "SIGKILL" }, (error, stdout, stderr) => {
      clearTimeout(deadline)
      if (!error) resolve({ stdout })
      else if (error.code === 1 && stderr.includes("org.freedesktop.DBus.Error.InvalidArgs: Invalid notification ID")) resolve({ stdout, invalidID: true })
      else resolve(undefined)
    })
  })
}

async function detectNotifySendCapabilities() {
  const output = await runNotifySend(["--version"], 1000)
  const match = output?.stdout.match(/(\d+)\.(\d+)/)
  if (!match) return false
  return Number(match[1]) > 0 || Number(match[2]) >= 8
}

async function sendPlainLinuxNotification(
  title: string,
  message: string,
  timeout: number,
  iconPath: string | undefined,
  grouping: boolean,
  current: () => boolean,
  channelCurrent: () => boolean,
  permit?: () => Promise<boolean>,
): Promise<boolean> {
  const args = ["--app-name", "opencode", "--expire-time", String(timeout * 1000)]
  if (iconPath) args.push("--icon", iconPath)
  const lastID = grouping ? getLinuxNotificationID() : null
  if (grouping) args.push("--print-id")
  args.push("--", title, message)
  let output = await runNotifySend(lastID === null ? args : ["--replace-id", String(lastID), ...args], 2000)
  // GNOME can reject a replacement owned by a different process.
  // An explicit ID error proves the first attempt did not show a popup.
  if (lastID !== null && output?.invalidID && current() && (!permit || await permit()) && current()) output = await runNotifySend(args, 2000)
  if (grouping && channelCurrent() && output && !output.invalidID) {
    const id = Number(output.stdout.trim())
    if (Number.isSafeInteger(id) && id > 0) rememberLinuxNotificationID(id)
  }
  return !!output && !output.invalidID
}

function pendingLinuxMessage(message: string) {
  const previous = linuxDeliveries.get(message)
  return previous?.channelCurrent() ? previous.result : undefined
}

function sendLinuxMessage<T>(message: string, channelCurrent: () => boolean, send: () => Promise<T>) {
  const result = send()
  const delivery = { result: result.then(() => undefined, () => undefined), channelCurrent }
  linuxDeliveries.set(message, delivery)
  return result.finally(() => {
    if (linuxDeliveries.get(message) === delivery) linuxDeliveries.delete(message)
  })
}

export function parseNotifySendOutputLine(
  line: string
): { type: "id"; id: number } | { type: "action"; action: NotificationAction } | null {
  const trimmed = line.trim()
  if (!trimmed) {
    return null
  }

  if (/^\d+$/.test(trimmed)) {
    const id = parseInt(trimmed, 10)
    if (!isNaN(id)) {
      return { type: "id", id }
    }
  }

  if (trimmed === LINUX_FOCUS_ACTION_KEY) {
    return { type: "action", action: "focus" }
  }

  if (trimmed === "close") {
    return { type: "action", action: "close" }
  }

  return null
}

export interface WindowsNotificationOptions {
  title: string
  message: string
  timeout: number
  icon: string | undefined
  appName: string
}

export function buildWindowsNotificationOptions(
  title: string,
  message: string,
  timeout: number,
  iconPath?: string,
  windowsAppID?: string
): WindowsNotificationOptions {
  return {
    title: title,
    message: message,
    timeout: timeout,
    icon: iconPath,
    // node-notifier's WindowsToaster only maps `appName` (not the Linux
    // "app-name" key) to SnoreToast's -appID. Without it, toasts fall back
    // to the generic "SnoreToast" label (#114).
    appName: windowsAppID ?? "opencode",
  }
}

export function buildOsascriptNotificationArgs(title: string, message: string): string[] {
  return [
    "-e",
    "on run argv\n display notification (item 1 of argv) with title (item 2 of argv)\nend run",
    message,
    title,
  ]
}

export async function sendNotification(
  title: string,
  message: string,
  timeout: number,
  iconPath?: string,
  notificationSystem: "osascript" | "node-notifier" | "ghostty" = "osascript",
  linuxGrouping: boolean = true,
  onClick?: () => void,
  windowsAppID?: string,
  clickActionLabel: string = LINUX_FOCUS_ACTION_LABEL,
  eligible: () => boolean = () => true,
  beforeSend?: (current: () => boolean) => Promise<boolean>,
  delivery?: { key?: string; order?: number; prompt: boolean; render?: () => { title: string; message: string } },
): Promise<void> {
  if (!eligible()) return
  const linuxDesktop = usesLinuxDesktopNotifications(notificationSystem)
  const identity = linuxDesktop && delivery?.prompt && delivery.key !== undefined ? `prompt\0${delivery.key}` : message
  const requestedAt = Date.now()
  const debounced = () => {
    const previous = lastNotifications.get(identity)
    return previous && (previous.channelCurrent?.() ?? true) && requestedAt - previous.requestedAt < DEBOUNCE_MS
  }
  if (debounced()) return
  const delivered = (channelCurrent?: () => boolean) => {
    if (!(channelCurrent?.() ?? true)) return
    lastNotifications.delete(identity)
    lastNotifications.set(identity, { requestedAt, channelCurrent })
    if (lastNotifications.size > 1024) lastNotifications.delete(lastNotifications.keys().next().value!)
  }

  if (linuxDesktop) {
    if (!process.env.DBUS_SESSION_BUS_ADDRESS) return
    const channelCurrent = linuxDeliveryGuard()
    const queuedAt = performance.now()
    const render = delivery?.render ?? (() => ({ title, message }))
    const order = delivery?.order ?? nextLinuxNotificationOrder()
    const mayReplace = () => linuxGrouping && (!lastGroupedDelivery?.channelCurrent() || lastGroupedDelivery.order <= order)
    const current = () => channelCurrent() && eligible() && !debounced() && performance.now() - queuedAt <= LINUX_NOTIFICATION_WAIT_MS &&
      (!linuxGrouping || delivery?.prompt || mayReplace())
    const permit = beforeSend && (() => beforeSend(current))
    const displayed = (replaces: boolean) => {
      if (!channelCurrent()) return
      if (replaces && (!lastGroupedDelivery?.channelCurrent() || lastGroupedDelivery.order < order))
        lastGroupedDelivery = { order, channelCurrent }
      if (eligible()) delivered(channelCurrent)
    }
    const plain = async (replace: boolean) => {
      if (!current() || (permit && !(await permit()))) return
      if (!current()) return
      const { title, message } = render()
      if (await sendPlainLinuxNotification(title, message, timeout, iconPath, replace, current, channelCurrent, permit)) displayed(replace)
    }
    const queuedSend = async (): Promise<"done" | "plain" | { pending: Promise<void> }> => {
      if (!current()) return "done"
      const pending = pendingLinuxMessage(identity)
      if (pending) return { pending }
      return sendLinuxMessage(identity, channelCurrent, async () => {
        // An older, still-open prompt remains actionable alongside the
        // newer popup. It must never replace that popup.
        const replace = mayReplace()
        if (onClick || (replace && hasLinuxDBusReplacement())) {
          const result = await sendLinuxDBusNotification(render, timeout, iconPath, replace, onClick, clickActionLabel, current, permit)
          if (result === "shown") displayed(replace)
          if (result !== "failed") return "done"
        }
        if (!replace) return "plain"
        if (current()) linuxNotifySendSupportsReplace ??= await detectNotifySendCapabilities()
        if (!linuxNotifySendSupportsReplace) return "plain"
        await plain(true)
        return "done"
      })
    }
    if (linuxGrouping || onClick) {
      const group = linuxGrouping ? (delivery ? delivery.key : identity) : undefined
      let result = await queueLinuxNotification(queuedSend, "done", group, false, queuedAt)
      // A slow plain popup must not occupy the replacement queue. If a newer
      // grouped popup succeeds first, the older request must not replace it.
      while (typeof result === "object" && current()) {
        result = await queueLinuxNotification(queuedSend, "done", group, false, queuedAt, result.pending)
      }
      if (result !== "plain") return
    }
    let waitFor: Promise<void> | undefined
    while (current()) {
      const waiting = await queueLinuxNotification(async () => {
        if (!current()) return
        const pending = pendingLinuxMessage(identity)
        if (pending) return { pending }
        await sendLinuxMessage(identity, channelCurrent, () => plain(false))
      }, undefined, undefined, true, queuedAt, waitFor)
      if (!waiting) return
      waitFor = waiting.pending
    }
    return
  }

  delivered()

  if (notificationSystem === "ghostty") {
    return new Promise((resolve) => {
      const sequence = formatGhosttyNotificationSequence(title, message)
      process.stdout.write(sequence, () => {
        resolve()
      })
    })
  }

  if (platform === "Darwin") {
    if (notificationSystem === "node-notifier") {
      return new Promise((resolve) => {
        const notificationOptions = {
          title: title,
          message: message,
          timeout: timeout,
          icon: iconPath,
        }

        notifier.notify(
          notificationOptions,
          (_error, response) => {
            if (onClick && response === "activate") {
              try { onClick() } catch {}
            }
            resolve()
          }
        )
      })
    }

    return new Promise((resolve) => {
      execFile("osascript", buildOsascriptNotificationArgs(title, message), () => {
        resolve()
      })
    })
  }

  return new Promise((resolve) => {
    const notificationOptions = buildWindowsNotificationOptions(title, message, timeout, iconPath, windowsAppID)

    platformNotifier.notify(
      notificationOptions,
      (_error, response, metadata) => {
        if (onClick && (response === "activate" || metadata?.activationType === "default" || metadata?.activationType === "activated")) {
          try { onClick() } catch {}
        }
        resolve()
      }
    )
  })
}
