import { decodeJoinCode } from "@mc/crypto.ts"
import type { PublicAccess } from "../../../../desktop/src/bridge.ts"
import { ops } from "../channel-ops"
import { forgetHistory } from "../history"
import { migrateLegacy } from "../legacy-seats"
import { migrateChannels, saveChannels, secrets, type ChannelView } from "../secrets"

export const CHANGED = "mc:channels"
export function changed(): void {
  window.dispatchEvent(new Event(CHANGED))
}

export function isJoinCode(code: string): boolean {
  try {
    decodeJoinCode(code)
    return true
  } catch {
    return false
  }
}

export interface StoredMember {
  code: string
  identity: { name: string; pk: string }
  access: PublicAccess
  at: number
  name?: string
}

export interface PendingJoin {
  code: string
  identity: { name: string; pk: string }
  requestId: string
}

const MEMBER_KEY = (code: string) => `mc.member.${code}`
const PENDING_KEY = (code: string) => `mc.pending.${code}`
export const RECENT_KEY = (code: string) => `mc.recent.${code}`

export function read<T>(key: string): T | null {
  try {
    return JSON.parse(localStorage.getItem(key) ?? "null") as T | null
  } catch {
    return null
  }
}

export const asMember = (e: ChannelView): StoredMember | null =>
  e.code && e.access ? { code: e.code, identity: { name: e.idName ?? e.label, pk: e.pk }, access: e.access, at: e.at ?? 0, ...(e.name ? { name: e.name } : {}) } : null
export const asPending = (e: ChannelView): PendingJoin | null =>
  e.code && !e.access && e.requestId ? { code: e.code, identity: { name: e.idName ?? e.label, pk: e.pk }, requestId: e.requestId } : null
const entries = (): ChannelView[] => secrets().channels

export async function migrateChannelsToStore(): Promise<number> {
  const n = await migrateLegacy(localStorage, migrateChannels)
  if (n) changed()
  return n
}

export function loadMember(code: string): StoredMember | null {
  const e = entries().find((x) => x.code === code && x.access)
  return e ? asMember(e) : null
}

export function renameChannel(code: string, name: string): Promise<void> {
  const done = saveChannels(entries().map((e) => (e.code === code && e.access ? { ...e, label: name, name } : e)))
  changed()
  return done
}

export function loadPending(code: string): PendingJoin | null {
  const e = entries().find((x) => x.code === code && !x.access)
  return e ? asPending(e) : null
}

export async function forgetPending(code: string): Promise<void> {
  if (loadMember(code)) return
  await ops().forget(code)
  changed()
}

export function pendingChannels(): PendingJoin[] {
  return entries().flatMap((e) => asPending(e) ?? [])
}

export function allMembers(): StoredMember[] {
  return entries().flatMap((e) => asMember(e) ?? [])
}

export async function forgetChannel(code: string): Promise<void> {
  const member = loadMember(code)
  if (member) void forgetHistory(member.access.roomId, member.identity.pk).catch(() => {})
  localStorage.removeItem(MEMBER_KEY(code))
  localStorage.removeItem(PENDING_KEY(code))
  localStorage.removeItem(RECENT_KEY(code))
  changed()
  await ops().forget(code)
  changed()
}
