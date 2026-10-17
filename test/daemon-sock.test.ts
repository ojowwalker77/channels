// The daemon's socket verbs (src/daemon/server.ts): hello names the version, the computer key and its
// pairing; adopt stores the cert; listening names the subscribers; stop quiets the socket.

import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { chmodSync, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { adoptDaemon, daemonHello, daemonListening, daemonSock, ensureDaemon, loadPairedMachine, startDaemon, stopDaemon, type Daemon } from "../src/daemon.ts";
import { generateIdentity } from "../src/identity.ts";
import { saveMachine } from "../src/machine.ts";
import { certifyMachine } from "../src/person.ts";
import { VERSION } from "../src/version.ts";

setDefaultTimeout(30_000);

const home = mkdtempSync(join(tmpdir(), "kiwi-daemon-sock-"));
const prevHome = process.env.KIWI_HOME;
process.env.KIWI_HOME = home;

let daemon: Daemon | undefined;
beforeAll(async () => {
  daemon = await startDaemon();
});
afterAll(async () => {
  await daemon?.stop().catch(() => {});
  if (prevHome === undefined) delete process.env.KIWI_HOME;
  else process.env.KIWI_HOME = prevHome;
  rmSync(home, { recursive: true, force: true });
});

async function untilDown(sock: string): Promise<void> {
  const end = Date.now() + 5_000;
  while (await daemonHello(sock).catch(() => null)) {
    if (Date.now() > end) throw new Error("timed out waiting for the daemon to stop");
    await Bun.sleep(50);
  }
}

describe("the daemon socket", () => {
  test("hello names the version, the computer key and its pairing", async () => {
    const h = await daemonHello();
    expect(h.version).toBe(VERSION);
    expect(typeof h.machine).toBe("string");
    expect(typeof h.mxpk).toBe("string");
    expect(h.certified).toBe(false);
    expect(h.person).toBeNull();
  });

  test("adopt stores the cert, and refuses another computer's", async () => {
    const h0 = await daemonHello();
    const person = await generateIdentity("Ada Lovelace");
    const cert = await certifyMachine(person, { pk: h0.machine!, xpk: h0.mxpk!, label: "sock-test" });
    await adoptDaemon(cert, { pk: person.pk, xpk: person.xpk!, name: "Ada Lovelace" });
    const h1 = await daemonHello();
    expect(h1.certified).toBe(true);
    expect(h1.person).toEqual({ pk: person.pk, xpk: person.xpk!, name: "Ada Lovelace" });
    expect(loadPairedMachine()?.cert).toEqual(cert);

    const mallory = await generateIdentity("Mallory");
    const other = await generateIdentity("someone-else");
    const foreign = await certifyMachine(mallory, { pk: other.pk, xpk: other.xpk!, label: "x" });
    await expect(adoptDaemon(foreign, { pk: mallory.pk, xpk: mallory.xpk!, name: "M" })).rejects.toThrow(/another computer/);
  });

  test("hello re-reads machine.json: no cached pairing", async () => {
    const paired = loadPairedMachine()!;
    // `kiwi pair` (or a forget) changed the file out from under the daemon: unlink it.
    saveMachine({ identity: paired.identity, label: paired.label, relay: paired.relay });
    const h = await daemonHello();
    expect(h.machine).toBe(paired.identity.pk);
    expect(h.certified).toBe(false);
    expect(h.person).toBeNull();
  });

  test("listening is empty, and stop quiets the socket", async () => {
    expect(await daemonListening()).toEqual([]);
    await stopDaemon();
    await untilDown(daemonSock());
    daemon = await startDaemon();
    expect((await daemonHello()).version).toBe(VERSION);
  });

  test("kiwi daemon runs in the foreground: one line, then SIGTERM stops it", async () => {
    const fg = mkdtempSync(join(tmpdir(), "kiwi-daemon-fg-"));
    const sock = join(fg, "daemon.sock");
    let proc: { kill(signal?: string): void; exited: Promise<number>; stdout: ReadableStream<Uint8Array> } | undefined;
    try {
      proc = Bun.spawn([process.execPath, join(import.meta.dir, "../src/cli/main.ts"), "daemon"], {
        env: { ...process.env, KIWI_HOME: fg },
        stdout: "pipe",
        stderr: "pipe",
      }) as unknown as NonNullable<typeof proc>;
      const reader = proc.stdout.getReader();
      let out = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (value) out += new TextDecoder().decode(value);
        if (out.includes("\n") || done) break;
      }
      reader.releaseLock();
      expect(out.split("\n")[0]).toMatch(new RegExp(`^kiwi daemon ${VERSION.replace(/\./g, "\\.")} on .*daemon\\.sock \\(pid \\d+\\)$`));
      // Ready, not just loud: killing during startup races the runtime's signal handling.
      const end = Date.now() + 5_000;
      while (!(await daemonHello(sock).catch(() => null))) {
        if (Date.now() > end) throw new Error("the foreground daemon never answered");
        await Bun.sleep(25);
      }
      proc.kill("SIGTERM");
      expect(await proc.exited).toBe(0);
      expect(await daemonHello(sock).catch(() => null)).toBeNull();
    } finally {
      // Never leave the foreground child behind: a failure before the SIGTERM above (or a
      // daemon that ignored it) would otherwise keep running with its KIWI_HOME unlinked.
      try {
        proc?.kill("SIGTERM");
      } catch {
        // Already gone; the socket wait below is what matters.
      }
      if (proc) await Promise.race([proc.exited, Bun.sleep(5_000)]).catch(() => {});
      await stopDaemon(sock).catch(() => {});
      await untilDown(sock).catch(() => {});
      try {
        proc?.kill("SIGKILL");
      } catch {
        // Gone.
      }
      rmSync(fg, { recursive: true, force: true });
    }
  });

  test("startDaemon locks an existing 0755 kiwi home and the socket 0600", async () => {
    if (process.platform === "win32") return;
    const dir = mkdtempSync(join(tmpdir(), "kiwi-daemon-perms-"));
    writeFileSync(join(dir, "config.json"), "{}\n", { mode: 0o644 });
    chmodSync(dir, 0o755);
    let d: Daemon | undefined;
    try {
      d = await startDaemon(dir);
      expect(statSync(dir).mode & 0o777).toBe(0o700);
      expect(statSync(daemonSock(dir)).mode & 0o777).toBe(0o600);
    } finally {
      await d?.stop().catch(() => {});
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("startDaemon does not chmod a user-supplied dir that is not a kiwi home", async () => {
    if (process.platform === "win32") return;
    const dir = mkdtempSync(join(tmpdir(), "kiwi-daemon-foreign-"));
    chmodSync(dir, 0o755);
    writeFileSync(join(dir, "notes.txt"), "hi", { mode: 0o644 });
    let d: Daemon | undefined;
    try {
      d = await startDaemon(dir);
      expect(statSync(dir).mode & 0o777).toBe(0o755);
      expect(statSync(daemonSock(dir)).mode & 0o777).toBe(0o600);
    } finally {
      await d?.stop().catch(() => {});
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("ensureDaemon starts a daemon in the background when none answers", async () => {
    const bg = mkdtempSync(join(tmpdir(), "kiwi-daemon-bg-"));
    const sock = daemonSock(bg);
    try {
      const hello = await ensureDaemon(bg, [process.execPath, join(import.meta.dir, "../src/cli/main.ts"), "daemon"]);
      expect(hello.version).toBe(VERSION);
      expect(typeof hello.machine).toBe("string");
      await stopDaemon(sock);
      await untilDown(sock);
    } finally {
      // The background daemon is detached (no handle): stopping it is the only cleanup.
      await stopDaemon(sock).catch(() => {});
      await untilDown(sock).catch(() => {});
      rmSync(bg, { recursive: true, force: true });
    }
  });
});
