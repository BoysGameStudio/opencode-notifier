import { readFileSync, writeFileSync } from "fs"
import {
  isEventSoundEnabled,
  isEventNotificationEnabled,
  isEventCommandEnabled,
  isEventBellEnabled,
  getMessage,
  getSoundPath,
  getSoundVolume,
  getIconPath,
  interpolateMessage,
  getStatePath,
  CHANNELS,
} from "./config"
import type { Channel, EventType, MessageContext, NotifierConfig } from "./config"
import { sendNotification, usesLinuxDesktopNotifications } from "./notify"
import { nextLinuxNotificationOrder } from "./notify-linux"
import { playSound } from "./sound"
import { ringBell } from "./bell"
import { runCommand } from "./command"
import { isTerminalFocused, focusTerminal, isTerminalJumpBackSupported, debugFocusState } from "./focus"

let globalTurnCount: number | null = null

function loadTurnCount(): number {
  try {
    const content = readFileSync(getStatePath(), "utf-8")
    const state = JSON.parse(content)
    if (typeof state.turn === "number" && Number.isFinite(state.turn) && state.turn >= 0) {
      return state.turn
    }
  } catch {}
  return 0
}

function saveTurnCount(count: number): void {
  try {
    writeFileSync(getStatePath(), JSON.stringify({ turn: count }))
  } catch {}
}

function incrementTurnCount(): number {
  if (globalTurnCount === null) {
    globalTurnCount = loadTurnCount()
  }
  globalTurnCount++
  saveTurnCount(globalTurnCount)
  return globalTurnCount
}

function getNotificationTitle(config: NotifierConfig, context: MessageContext): string {
  if (config.notificationTitle !== null) {
    const title = interpolateMessage(config.notificationTitle, context)
    if (title) return title
  }
  if (config.showProjectName && context.projectName) {
    return `OpenCode (${context.projectName})`
  }
  return "OpenCode"
}

function formatTimestamp(): string {
  const now = new Date()
  const h = String(now.getHours()).padStart(2, "0")
  const m = String(now.getMinutes()).padStart(2, "0")
  const s = String(now.getSeconds()).padStart(2, "0")
  return `${h}:${m}:${s}`
}

export function extractAgentNameFromSessionTitle(sessionTitle: unknown): string {
  if (typeof sessionTitle !== "string" || sessionTitle.length === 0) {
    return ""
  }

  const match = sessionTitle.match(/\s*\(@([^\s)]+)\s+subagent\)\s*$/)
  return match ? match[1] : ""
}

export function shouldResolveSessionContextForEvent(config: NotifierConfig, eventType: EventType): boolean {
  if (getMessage(config, eventType).includes("{agentName}")) return true

  if (isEventNotificationEnabled(config, eventType) && config.notificationTitle &&
      (config.notificationTitle.includes("{agentName}") || config.notificationTitle.includes("{sessionTitle}"))) return true

  const commands = []
  if (isEventNotificationEnabled(config, eventType)) commands.push(config.onClickCommand)
  if (isEventCommandEnabled(config, eventType)) commands.push(config.command)
  return commands.some(command => command.enabled && [command.path, ...(command.args ?? [])]
    .some(value => value.includes("{agentName}") || value.includes("{sessionTitle}")))
}

// Whether the config enables any channel for this event. V2 terminals check this before claiming
// an event, so a terminal with every channel off never blocks one that would alert. Focus is checked
// after the claim: probes can block for seconds, so only the winning terminal runs them.
export function deliversAny(config: NotifierConfig, eventType: EventType): boolean {
  return CHANNELS.some(channel => config.events[eventType][channel] && (channel !== "command" || config.command.enabled))
}

export function queuedLinuxPrompt(config: NotifierConfig, event: EventType): "popup-only" | "mixed" | undefined {
  if ((event !== "permission" && event !== "question") || !usesLinuxDesktopNotifications(config.notificationSystem) ||
      !isEventNotificationEnabled(config, event)) return
  return isEventSoundEnabled(config, event) || isEventBellEnabled(config, event) || (config.command.enabled && isEventCommandEnabled(config, event))
    ? "mixed" : "popup-only"
}

export type PopupCheck = { permit?: (current: () => boolean) => Promise<boolean>; deferFocus?: boolean }

let channelFocusProbe: Promise<boolean> | undefined

function channelIsFocused(): Promise<boolean> {
  // Concurrent alerts can share the first focus snapshot. Each queued popup
  // still takes a fresh snapshot just before sending.
  return channelFocusProbe ??= Promise.resolve(isTerminalFocused()).finally(() => { channelFocusProbe = undefined })
}

export async function handleEvent(
  config: NotifierConfig,
  eventType: EventType,
  projectName: string | null,
  elapsedSeconds?: number | null,
  sessionTitle?: string | null,
  sessionID?: string | null,
  agentName?: string | null,
  focusSession?: (sessionID: string) => void,
  eligible?: () => boolean,
  beforePopup?: PopupCheck,
  identity?: { key?: string; order: number; commit?: () => void },
): Promise<void> {
  const order = identity?.order ?? nextLinuxNotificationOrder()
  // Focus is only queried when some channel cares about it.
  const focusChannels = config.suppressWhenFocused === true ? CHANNELS : config.suppressWhenFocused || []
  const muted = new Set<Channel>(!beforePopup?.deferFocus && focusChannels.length > 0 && await channelIsFocused() ? focusChannels : [])
  if (eligible && !eligible()) return
  identity?.commit?.()
  if (muted.size === CHANNELS.length) return

  if (
    (eventType === "complete" || eventType === "subagent_complete") &&
    typeof elapsedSeconds === "number" &&
    Number.isFinite(elapsedSeconds) &&
    elapsedSeconds < config.minDuration
  ) {
    return
  }

  const promises: Promise<void>[] = []

  const timestamp = formatTimestamp()
  const rawMessage = getMessage(config, eventType)
  let contents: { context: MessageContext; message: string } | undefined
  function describe() {
    if (contents) return contents
    const context: MessageContext = {
      sessionTitle: config.showSessionTitle ? sessionTitle : null,
      agentName,
      projectName,
      timestamp,
      turn: incrementTurnCount(),
    }
    return contents = { context, message: interpolateMessage(rawMessage, context) }
  }
  const delayed = beforePopup?.deferFocus === true
  if (!delayed) describe()

  const notificationEnabled = isEventNotificationEnabled(config, eventType) && !muted.has("notification")
  if (notificationEnabled) {
    const render = () => {
      const { context, message } = describe()
      return { title: getNotificationTitle(config, context), message }
    }
    const presentation = delayed ? { title: "", message: rawMessage } : render()
    const iconPath = getIconPath(config)
    const focusOnClick = config.focusOnClick && isTerminalJumpBackSupported()
    const clickCommand = config.onClickCommand
    let clicked = false
    const onNotificationClick = focusOnClick || (clickCommand.enabled && clickCommand.path) ? () => {
      if (clicked) return
      clicked = true
      debugFocusState(`notification activated (event=${eventType}), focusOnClick=${focusOnClick}`)
      if (clickCommand.enabled) {
        const { context, message } = describe()
        runCommand({ ...config, command: clickCommand }, eventType, message, sessionTitle, agentName, projectName, timestamp, context.turn, sessionID)
      }
      if (focusOnClick) {
        try { if (sessionID) focusSession?.(sessionID) }
        catch (error) { debugFocusState(`session selection failed: ${String(error)}`) }
        void focusTerminal().then(
          () => debugFocusState("focusTerminal() finished"),
          (error) => debugFocusState(`focusTerminal() threw: ${String(error)}`)
        )
      }
    } : undefined
    const checkPopupFocus = usesLinuxDesktopNotifications(config.notificationSystem) && focusChannels.includes("notification")
    const beforeSend = (beforePopup || checkPopupFocus) ? async (current: () => boolean) => (await beforePopup?.permit?.(current) ?? true) && current() &&
      (!checkPopupFocus || !(await isTerminalFocused())) : undefined
    const delivery = { key: identity?.key, order, prompt: eventType === "permission" || eventType === "question", render: delayed ? render : undefined }
    promises.push(sendNotification(presentation.title, presentation.message, config.timeout, iconPath, config.notificationSystem, config.linux.grouping,
      onNotificationClick, config.windows.appID, focusOnClick ? "Jump to terminal" : "Run command", eligible, beforeSend, delivery))
  }

  if (isEventSoundEnabled(config, eventType) && !muted.has("sound")) {
    const customSoundPath = getSoundPath(config, eventType)
    const ghosttyOnMac = process.platform === "darwin" && config.notificationSystem === "ghostty" && notificationEnabled && config.suppressGhosttySound
    if (!ghosttyOnMac) {
      const soundVolume = getSoundVolume(config, eventType)
      promises.push(playSound(eventType, customSoundPath, soundVolume))
    }
  }

  if (isEventBellEnabled(config, eventType) && !muted.has("bell")) {
    promises.push(ringBell())
  }

  const minDuration = config.command?.minDuration
  const shouldSkipCommand =
    !config.command.enabled || !isEventCommandEnabled(config, eventType) ||
    muted.has("command") ||
    (typeof minDuration === "number" &&
      Number.isFinite(minDuration) &&
      minDuration > 0 &&
      typeof elapsedSeconds === "number" &&
      Number.isFinite(elapsedSeconds) &&
      elapsedSeconds < minDuration)

  if (!shouldSkipCommand) {
    const { context, message } = describe()
    runCommand(config, eventType, message, sessionTitle, agentName, projectName, timestamp, context.turn, sessionID)
  }

  await Promise.allSettled(promises)
}
