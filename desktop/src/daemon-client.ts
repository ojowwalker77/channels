// Talking to "kiwi daemon" over its local socket, in the daemon's own protocol (src/daemon/protocol.ts is
// the contract; this follows it, so the CLI and the app reach the same daemon the same way):
//   path: $KIWI_HOME/daemon.sock (a named pipe on Windows: \\.\pipe\kiwi-daemon-<home's tag>)
//   framing: newline-delimited JSON, one request per line, one response per line
//   request:  { "id": number, "verb": string, ...params }        (params flat on the request)
//   response: { "id": number, "ok": true, ...fields } | { "id": number, "ok": false, "error": string }
//   verbs this shell needs:
//     hello -> { version, machine: pk | null, mxpk, label, person: { pk, name, … } | null, certified }
//     adopt { cert, person: { pk, xpk, name } } -> { ok: true }
//       (cert is the MachineCert the app made for this machine's key)
// It used to send { method, params } and read { result }, which the daemon ignores: every hello
// timed out, so the app never saw its own daemon and never listed this computer.

import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import type { MachineCert } from "../../src/person.ts";

export interface HelloResult {
  version: string;
  machine: { pk: string; xpk: string };
  paired: { person: string; name: string; label: string } | null;
}

export interface AdoptParams {
  cert: MachineCert;
  person: { pk: string; xpk: string; name: string };
}

export function kiwiHome(): string {
  return process.env["KIWI_HOME"] ?? path.join(os.homedir(), ".kiwi");
}

export function daemonSocketPath(home: string = kiwiHome()): string {
  // The same name the daemon picks (src/daemon/protocol.ts daemonSock): its home, as a tag.
  if (process.platform === "win32") return `\\\\.\\pipe\\kiwi-daemon-${home.replace(/[^A-Za-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(-64) || "kiwi"}`;
  return path.join(home, "daemon.sock");
}

type Reply = { id: number; ok: boolean; error?: string } & Record<string, unknown>;

function call(sock: string, verb: string, params: object = {}, timeoutMs = 5_000): Promise<Reply> {
  return new Promise((resolve, reject) => {
    const conn = net.createConnection(sock);
    let settled = false;
    const settle = (finish: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      conn.destroy();
      finish();
    };
    const timer = setTimeout(() => settle(() => reject(new Error("the daemon didn't answer"))), timeoutMs);
    let buf = "";
    conn.on("connect", () => conn.write(`${JSON.stringify({ ...params, id: 1, verb })}\n`));
    conn.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      const nl = buf.indexOf("\n");
      if (nl < 0) return;
      settle(() => {
        try {
          const res = JSON.parse(buf.slice(0, nl)) as Reply;
          if (res.ok) resolve(res);
          else reject(new Error(typeof res.error === "string" ? res.error : "the daemon refused"));
        } catch (err) {
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      });
    });
    conn.on("error", (err) => settle(() => reject(err)));
    conn.on("close", () => settle(() => reject(new Error("the daemon closed the connection"))));
  });
}

/** The daemon's hello, or null when none is listening (stale socket included) or it has no key yet. */
export async function daemonHello(home: string = kiwiHome(), timeoutMs = 5_000): Promise<HelloResult | null> {
  try {
    const r = await call(daemonSocketPath(home), "hello", {}, timeoutMs);
    if (typeof r["version"] !== "string" || typeof r["machine"] !== "string" || typeof r["mxpk"] !== "string") return null;
    const person = r["person"] as { pk?: unknown; name?: unknown } | null;
    const label = typeof r["label"] === "string" ? r["label"] : "";
    return {
      version: r["version"],
      machine: { pk: r["machine"], xpk: r["mxpk"] },
      paired: r["certified"] && person && typeof person.pk === "string" ? { person: person.pk, name: typeof person.name === "string" ? person.name : "", label } : null,
    };
  } catch {
    return null;
  }
}

/** Ask the daemon to stop (an older one, before a newer starts). True once it said it would. */
export async function daemonStop(home: string = kiwiHome(), timeoutMs = 5_000): Promise<boolean> {
  try {
    await call(daemonSocketPath(home), "stop", {}, timeoutMs);
    return true;
  } catch {
    return false;
  }
}

/** Hand the daemon its cert from the person. Throws when it refuses. */
export async function daemonAdopt(home: string, params: AdoptParams, timeoutMs = 10_000): Promise<void> {
  await call(daemonSocketPath(home), "adopt", params, timeoutMs);
}

/**
 * Start `kiwi daemon` for `home`, apart from the app (it outlives a quit). It gets the app's relay:
 * a daemon not yet adopted follows the relay it starts on (src/daemon/pair.ts ensureMachine), and the
 * app's own environment rarely says one (KIWI_RELAY), so without it a self-hosted relay's app
 * would adopt a daemon on the public one.
 */
export function spawnDaemon(bin: string, home: string, relay: string): void {
  const child = spawn(bin, ["daemon"], { env: { ...process.env, KIWI_HOME: home, KIWI_RELAY: relay }, detached: true, stdio: "ignore", windowsHide: true });
  // A binary that won't start shows as no hello (the caller waits for one), not an uncaught error.
  child.on("error", () => {});
  child.unref();
}
