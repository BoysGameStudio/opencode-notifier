// @ts-check

/** @typedef {0 | 1 | 2} InboxReport */
/** @typedef {{ sessionID: string, id: string, report: InboxReport }} InboxMetadata */
/** @typedef {{ id: string, idle: number | null }} InboxSession */
/** @typedef {{ sessions: InboxSession[], items: InboxMetadata[], externalActivity?: string[], externalWork?: boolean }} InboxFamily */
/** @typedef {Omit<InboxFamily, "sessions"> & { sessions: (InboxSession & { directory: string, workspaceID: string | null })[] }} StoredInboxFamily */

// OpenCode 2.3 stores attachment data in the user payload. Project its inbox
// metadata in SQLite without retrieving that payload into JavaScript.
const family = `
  WITH RECURSIVE input(sessionID) AS (SELECT ?), family(id, idle, directory, workspaceID) AS (
    SELECT id, time_idle, directory, workspace_id FROM session_v2 WHERE id = (SELECT sessionID FROM input)
    UNION
    SELECT child.id, child.time_idle, child.directory, child.workspace_id
    FROM session_v2 AS child JOIN family ON child.parent_id = family.id
  )
`

const query = family + `
  SELECT family.id AS sessionID, NULL AS id, NULL AS report, family.idle, family.directory, family.workspaceID FROM family
  UNION ALL
  SELECT inbox.session_id AS sessionID, inbox.id,
    CASE
      WHEN inbox.type != 'synthetic' THEN 0
      WHEN json_extract(inbox.payload, '$.metadata.source') = 'subagent' THEN 1
      WHEN json_extract(inbox.payload, '$.metadata.source') IN ('shell', 'codemode', 'experimental_image_generate') THEN 2
      ELSE 0
    END AS report,
    NULL AS idle, NULL AS directory, NULL AS workspaceID
  FROM family
  JOIN session_inbox AS inbox ON inbox.session_id = family.id
  LIMIT 8193
`

const outstanding = family + `
  SELECT EXISTS (
    SELECT 1 FROM family JOIN session_inbox AS inbox ON inbox.session_id = family.id
    WHERE inbox.type = 'synthetic' AND (
      json_extract(inbox.payload, '$.metadata.source') = 'subagent' OR (
        family.id != (SELECT sessionID FROM input) AND json_extract(inbox.payload, '$.metadata.source') IN ('shell', 'codemode', 'experimental_image_generate')
      )
    )
  ) AS pending
`

/** @typedef {InboxMetadata | { sessionID: string, id: null, report: null, idle: number | null, directory: string, workspaceID: string | null }} InboxRow */
/** @typedef {{ all(id: string): unknown[] }} Statement */
/** @typedef {{ close(): void }} DatabaseHandle */

/** @param {string} file @param {string} root */
async function read(file, root) {
  /** @type {DatabaseHandle | undefined} */
  let database
  try {
    /** @type {(sql: string) => unknown[]} */
    let readRows
    if (process.versions.bun) {
      const module = "bun:sqlite"
      const { Database } = /** @type {{ Database: new (file: string, options: { readonly: true }) => DatabaseHandle & { exec(sql: string): unknown; query(sql: string): Statement } }} */ (await import(module))
      const connection = new Database(file, { readonly: true })
      database = connection
      connection.exec("BEGIN")
      readRows = sql => connection.query(sql).all(root)
    } else {
      const { DatabaseSync } = await import("node:sqlite")
      const connection = new DatabaseSync(file, { readOnly: true })
      database = connection
      connection.exec("BEGIN")
      readRows = sql => connection.prepare(sql).all(root)
    }
    const rows = /** @type {InboxRow[]} */ (readRows(query))
    if (!rows.length) return null
    // Keep both reads in the same SQLite snapshot. The native lifecycle
    // fallback can inspect activity once queued child reports are ruled out.
    if (rows.length > 8192) {
      const [state] = /** @type {{ pending: number }[]} */ (readRows(outstanding))
      return state?.pending === 0 ? null : { overflow: true }
    }
    /** @type {StoredInboxFamily} */
    const result = { sessions: [], items: [] }
    for (const row of rows) {
      if (row.id === null) result.sessions.push({
        id: row.sessionID, idle: row.idle, directory: row.directory, workspaceID: row.workspaceID,
      })
      else result.items.push({ sessionID: row.sessionID, id: row.id, report: row.report })
    }
    return result
  } catch (error) {
    const sqlite = /** @type {{ code?: string, errcode?: number }} */ (error)
    if (sqlite.code === "SQLITE_CANTOPEN" || sqlite.errcode === 14) return null
    throw error
  } finally {
    database?.close()
  }
}

const [file, root] = process.argv.slice(2)
if (!file || !root) throw new Error("Missing inbox database or session")
process.stdout.write(JSON.stringify(await read(file, root)))
export {}
