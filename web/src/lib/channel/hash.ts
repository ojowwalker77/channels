export function parseHash(hash: string): { code: string } {
  const raw = hash.replace(/^#/, "")
  try {
    return { code: decodeURIComponent(raw).trim() }
  } catch {
    // Not valid percent-encoding (#%): keep it as typed, so ChannelGate says it isn’t a join code instead of the app crashing.
    return { code: raw.trim() }
  }
}
