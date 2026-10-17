import { git } from "./git.ts"

type Hunk = { at: string; removed: string[]; added: string[]; view?: { removed: string[]; added: string[] } }
export type FileChange = {
  path: string
  from?: string
  status: "added" | "deleted" | "modified" | "renamed"
  binary: boolean
  hunks: Hunk[]
  moved?: number
}

const COMMENT = /^(\/\/|\/\*|\*)/
const IMPORT = /^(import\b|export\s+(\*|\{[^}]*\}|type\s+\{[^}]*\})\s+from\b|\}\s+from\s+["'])/
const trivial = (line: string) => {
  const t = line.trim()
  return t.length < 4 || COMMENT.test(t)
}
const light = (line: string) => trivial(line) || IMPORT.test(line.trim())

export function size(f: FileChange): number {
  if (f.status === "deleted") return 1
  const n = f.hunks.reduce((n, h) => n + [...(h.view ?? h).removed, ...(h.view ?? h).added].filter((l) => !light(l)).length, 0)
  return f.status === "renamed" ? Math.max(n, 1) : n
}

export function readDiff(repo: string, base: string, head: string): FileChange[] {
  const out = git(repo, ["diff", "-M", "-w", "--no-color", "--no-ext-diff", "--unified=0", base, head])
  const files: FileChange[] = []
  let file: FileChange | undefined
  let hunk: Hunk | undefined
  for (const line of out.split("\n")) {
    if (line.startsWith("diff --git ")) {
      const m = line.match(/^diff --git a\/(.*) b\/(.*)$/)
      file = { path: m?.[2] ?? line, status: "modified", binary: false, hunks: [] }
      hunk = undefined
      files.push(file)
    } else if (!file) continue
    else if (!hunk) {
      if (line.startsWith("new file mode")) file.status = "added"
      else if (line.startsWith("deleted file mode")) file.status = "deleted"
      else if (line.startsWith("rename from ")) {
        file.status = "renamed"
        file.from = line.slice(12)
      } else if (line.startsWith("Binary files")) file.binary = true
      else if (line.startsWith("@@")) file.hunks.push((hunk = { at: line.replace(/^@@[^@]*@@\s?/, ""), removed: [], added: [] }))
    } else if (line.startsWith("@@")) file.hunks.push((hunk = { at: line.replace(/^@@[^@]*@@\s?/, ""), removed: [], added: [] }))
    else if (line.startsWith("+")) hunk.added.push(line.slice(1))
    else if (line.startsWith("-")) hunk.removed.push(line.slice(1))
  }
  return files
}

const key = (line: string) =>
  line
    .trim()
    .replace(/^export\s+(default\s+)?/, "")
    .replace(/[;,]$/, "")
    .replace(/\s+/g, " ")

type Side = "removed" | "added"

export function markMoves(files: FileChange[]): number {
  const where: Record<Side, Map<string, string[]>> = { removed: new Map(), added: new Map() }
  for (const f of files)
    for (const h of f.hunks)
      for (const side of ["removed", "added"] as const)
        for (const line of h[side]) {
          if (trivial(line)) continue
          const k = key(line)
          const list = where[side].get(k) ?? []
          list.push(f.path)
          where[side].set(k, list)
        }
  const left: Record<Side, Map<string, number>> = { removed: new Map(), added: new Map() }
  for (const [k, list] of where.removed) {
    const n = Math.min(list.length, where.added.get(k)?.length ?? 0)
    if (n) {
      left.removed.set(k, n)
      left.added.set(k, n)
    }
  }
  if (!left.removed.size) return 0
  let total = 0
  for (const f of files) {
    let moved = 0
    for (const h of f.hunks) {
      const view = { removed: [] as string[], added: [] as string[] }
      for (const side of ["removed", "added"] as const) {
        const other: Side = side === "added" ? "removed" : "added"
        let run: { lines: number; moved: number; from: Map<string, number> } | undefined
        const flush = () => {
          if (!run) return
          const src = [...run.from].sort((a, b) => b[1] - a[1])[0]?.[0] ?? "elsewhere"
          const where = src === f.path ? "within this file" : `${side === "added" ? "from" : "to"} ${src}`
          view[side].push(`⟨${run.lines} line${run.lines === 1 ? "" : "s"} moved ${where}, unchanged⟩`)
          run = undefined
        }
        let pending: string[] = []
        for (const line of h[side]) {
          const k = key(line)
          const budget = trivial(line) ? 0 : (left[side].get(k) ?? 0)
          if (budget > 0) {
            left[side].set(k, budget - 1)
            run ??= { lines: 0, moved: 0, from: new Map() }
            run.lines += pending.length + 1
            run.moved++
            pending = []
            const src = where[other].get(k)?.find((p) => p !== f.path) ?? f.path
            run.from.set(src, (run.from.get(src) ?? 0) + 1)
            moved++
          } else if (trivial(line) && run) pending.push(line)
          else {
            flush()
            view[side].push(...pending, line)
            pending = []
          }
        }
        flush()
        view[side].push(...pending)
      }
      h.view = view
    }
    if (moved) f.moved = moved
    else for (const h of f.hunks) delete h.view
    total += moved
  }
  return total
}
