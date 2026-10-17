// The daemon multiplexes one relay stream per agent seat (src/daemon/server.ts): every subscriber of
// an agent shares its seat's stream, each from its own cursor; a restart only needs the cursors.
// Against a real relay (startRelay), with the seat's home written the way a join would.

import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Channel } from "../src/client.ts";
import { loadConfig, saveConfig, saveIdentity } from "../src/config.ts";
import { daemonListening, listenDaemon, startDaemon, type Daemon, type DaemonEvent, type DaemonSubscription } from "../src/daemon.ts";
import { generateIdentity } from "../src/identity.ts";
import type { Message, Presence } from "../src/protocol.ts";
import { checked } from "./check.ts";
import { startRelay } from "../src/relay/bun.ts";

setDefaultTimeout(30_000);

const home = mkdtempSync(join(tmpdir(), "kiwi-daemon-listen-"));
const dataDir = mkdtempSync(join(tmpdir(), "kiwi-daemon-listen-relay-"));
const prevHome = process.env.KIWI_HOME;
process.env.KIWI_HOME = home;

let server: ReturnType<typeof startRelay> | undefined;
let relay = "";
let ownerCh: Channel;
let daemon: Daemon | undefined;
let code = "";
let roomId = "";
let agentPk = "";

beforeAll(async () => {
  server = startRelay({ port: 0, hostname: "127.0.0.1", dataDir });
  relay = server.url.origin;
  const owner = await generateIdentity("human");
  const agent = await generateIdentity("claude");
  const made = await Channel.create(relay, owner, { name: "human" }, [{ ...agent, info: { name: "claude" } }]);
  ownerCh = new Channel(made.access, relay, owner);
  code = made.code;
  roomId = made.access.roomId;
  agentPk = agent.pk;
  saveConfig({ channels: { test: { ...(await Channel.resume(relay, made.code, agent)), relay, code: made.code, as: "claude" } } });
  saveIdentity(agent, made.access.roomId);
  daemon = await startDaemon();
});
afterAll(async () => {
  await daemon?.stop().catch(() => {});
  server?.stop(true);
  if (prevHome === undefined) delete process.env.KIWI_HOME;
  else process.env.KIWI_HOME = prevHome;
  rmSync(home, { recursive: true, force: true });
  rmSync(dataDir, { recursive: true, force: true });
});

async function nextMsg(it: AsyncIterator<Message>, label: string, ms = 10_000): Promise<Message> {
  const got = await Promise.race([it.next(), Bun.sleep(ms).then(() => null)]);
  if (!got || got.done) throw new Error(`timed out waiting for ${label}`);
  return got.value;
}

async function waitBody(sub: DaemonSubscription, body: string): Promise<Message> {
  const it = sub[Symbol.asyncIterator]();
  const end = Date.now() + 10_000;
  for (;;) {
    const m = await nextMsg(it, `"${body}"`, Math.max(500, end - Date.now()));
    if (m.body === body) return m;
  }
}

async function until(what: string, ok: () => boolean | Promise<boolean>, ms = 10_000): Promise<void> {
  const end = Date.now() + ms;
  while (!(await ok())) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await Bun.sleep(50);
  }
}

async function subscribers(): Promise<number | null> {
  return (await daemonListening()).find((s) => s.channel === "test" && s.agent === "claude")?.subscribers ?? null;
}

async function settled(n: number): Promise<void> {
  await until(`${n} subscribers`, async () => (await subscribers()) === n);
}

let cursor = 0;

describe("the daemon's shared listening", () => {
  test("two subscribers share one agent's stream, each from its cursor", async () => {
    const [a, b] = await Promise.all([listenDaemon("test", "claude", cursor), listenDaemon("test", "claude", cursor)]);
    try {
      expect(await daemonListening()).toMatchObject([{ channel: "test", agent: "claude", subscribers: 2, read: null }]);
      const seq = await ownerCh.send("hello agents");
      expect((await waitBody(a, "hello agents")).seq).toBe(seq);
      expect((await waitBody(b, "hello agents")).seq).toBe(seq);
      cursor = seq;
    } finally {
      b.close();
    }
    await settled(1);
    a.close();
    await settled(0);
  });

  test("a resubscriber gets only what it missed", async () => {
    const a = await listenDaemon("test", "claude", cursor);
    try {
      const c = await listenDaemon("test", "claude", cursor);
      try {
        await ownerCh.send("second");
        await ownerCh.send("third");
        expect((await waitBody(a, "second")).body).toBe("second");
        const it = c[Symbol.asyncIterator]();
        expect((await nextMsg(it, "second")).body).toBe("second");
        cursor = (await nextMsg(it, "third")).seq;
        expect(cursor).toBeGreaterThan(0);
      } finally {
        c.close();
      }
    } finally {
      a.close();
    }
  });

  test("a fresh daemon reopens the seat and serves a reconnecting subscriber from its cursor", async () => {
    await ownerCh.send("before the restart");
    await daemon!.stop();
    await ownerCh.send("while down");
    daemon = await startDaemon();
    await settled(0);
    const d = await listenDaemon("test", "claude", cursor);
    try {
      const it = d[Symbol.asyncIterator]();
      expect((await nextMsg(it, "before the restart")).body).toBe("before the restart");
      expect((await nextMsg(it, "while down")).body).toBe("while down");
      expect(await subscribers()).toBe(1);
    } finally {
      d.close();
    }
  });

  test("the iterator's return() unsubscribes and closes the unix socket, and the seat stays", async () => {
    const sub = await listenDaemon("test", "claude", cursor);
    expect(await subscribers()).toBe(1);
    await sub[Symbol.asyncIterator]().return?.();
    await settled(0);
  });
});

describe("the daemon's seat is always there", () => {
  beforeAll(async () => {
    await settled(0);
    await daemon?.stop();
    daemon = await startDaemon(undefined, { beaconMs: 300 });
    await settled(0);
  });

  test("with no subscriber it announces itself as the daemon, keeps a beacon, and answers a probe", async () => {
    const heard: (Presence & { sigOk: boolean })[] = [];
    let probe: ((p: { client: string; query: boolean }) => Promise<void>) | null = null;
    const ctl = new AbortController();
    void ownerCh.stream(await ownerCh.head(), () => {}, { signal: ctl.signal, onPresence: (p) => heard.push(p), onOpen: ({ presence }) => (probe = presence) });
    try {
      const fromAgent = () => heard.filter((p) => p.pk === agentPk && !p.query);
      await until("an announce", () => fromAgent().some((p) => p.client === "daemon"));
      const before = fromAgent().length;
      await until("a beacon", () => fromAgent().length >= before + 2);
      await until("the owner's socket", () => probe !== null);
      const n = fromAgent().length;
      await probe!({ client: "probe", query: true });
      await until("an answer to the probe", () => fromAgent().length > n, 3_000);
      expect(fromAgent().every((p) => p.client === "daemon")).toBe(true);
    } finally {
      ctl.abort();
    }
  });

  test("roster events reach subscribers, and a messages-only subscriber still gets messages", async () => {
    const events: DaemonEvent[] = [];
    const sub = await listenDaemon("test", "claude", cursor, { onEvent: (e) => events.push(e) });
    const plain = await listenDaemon("test", "claude", cursor);
    try {
      const joiner = await generateIdentity("gemini");
      const ask = await Channel.requestJoin(relay, code, joiner, { name: "gemini" });
      await ownerCh.approve((await checked(ownerCh, relay, code, [{ id: joiner, requestId: ask.requestId }]))[0]!);
      await until("the roster event", () => events.includes("roster"));
      const seq = await ownerCh.send("after the roster");
      expect((await waitBody(plain, "after the roster")).seq).toBe(seq);
      cursor = seq;
    } finally {
      sub.close();
      plain.close();
    }
    await settled(0);
  });

  test("read marks go out only when a subscriber asks", async () => {
    const markOf = (): number | null => {
      const db = new Database(join(dataDir, `${roomId}.sqlite`), { readonly: true });
      try {
        return (db.query("SELECT seq FROM reads WHERE pk = ?").get(agentPk) as { seq: number } | null)?.seq ?? null;
      } finally {
        db.close();
      }
    };
    const before = markOf();
    const sub = await listenDaemon("test", "claude", cursor);
    try {
      const seq = await ownerCh.send("read me");
      await waitBody(sub, "read me");
      await Bun.sleep(1_500);
      expect(markOf()).toBe(before);
      await sub.mark(seq);
      await until("the read mark", () => markOf() === seq);
      cursor = seq;
    } finally {
      sub.close();
    }
    await settled(0);
  });

  test("a seat that opens while the relay is down tells a late subscriber so, and says when it got there", async () => {
    await daemon!.stop();
    const port = Number(server!.url.port);
    server!.stop(true);
    daemon = await startDaemon(undefined, { beaconMs: 300 });
    const statuses: string[] = [];
    const pending = listenDaemon("test", "claude", cursor, { onStatus: (s) => statuses.push(s) });
    await until("the subscriber to hear the relay is out of reach", () => statuses.some((s) => s.startsWith("can't reach the relay")));
    server = startRelay({ port, hostname: "127.0.0.1", dataDir });
    const sub = await pending;
    try {
      await until("the subscriber to hear the relay is back", () => statuses.some((s) => s.startsWith("reached the relay")));
      const seq = await ownerCh.send("back again");
      expect((await waitBody(sub, "back again")).seq).toBe(seq);
      cursor = seq;
    } finally {
      sub.close();
    }
    await settled(0);
  });

  test("a waiter leaving during the retry doesn't fail another waiter", async () => {
    await daemon!.stop();
    const port = Number(server!.url.port);
    server!.stop(true);
    daemon = await startDaemon(undefined, { beaconMs: 300 });
    const first = new AbortController();
    const a = listenDaemon("test", "claude", cursor, { signal: first.signal }).catch((err: unknown) => err);
    await Bun.sleep(200);
    const b = listenDaemon("test", "claude", cursor);
    await Bun.sleep(600);
    first.abort();
    await a;
    await Bun.sleep(300);
    server = startRelay({ port, hostname: "127.0.0.1", dataDir });
    const sub = await b;
    try {
      const seq = await ownerCh.send("for the one still waiting");
      expect((await waitBody(sub, "for the one still waiting")).seq).toBe(seq);
      await settled(1);
      cursor = seq;
    } finally {
      sub.close();
    }
    await settled(0);
  });

  test("with nobody waiting, the daemon keeps retrying and opens the seat once the relay is back", async () => {
    await daemon!.stop();
    const port = Number(server!.url.port);
    server!.stop(true);
    daemon = await startDaemon(undefined, { beaconMs: 300 });
    await Bun.sleep(700);
    expect(await subscribers()).toBeNull();
    server = startRelay({ port, hostname: "127.0.0.1", dataDir });
    await settled(0);
    await until("one beacon", () => daemon!.beacons() === 1);
  });

  test("subscribers that come and go at once leave the seat and its one beacon", async () => {
    for (let i = 0; i < 15; i++) (await listenDaemon("test", "claude", cursor)).close();
    await settled(0);
    await Bun.sleep(1_000);
    expect(daemon!.beacons()).toBe(1);
  });

  test("a key rotation is saved for the agent", async () => {
    const sub = await listenDaemon("test", "claude", cursor);
    try {
      const epoch = await ownerCh.rotate();
      const seq = await ownerCh.send("under the new key");
      expect((await waitBody(sub, "under the new key")).seq).toBe(seq);
      await until("the saved key", () => !!loadConfig().channels.test?.keys[String(epoch)]);
      cursor = seq;
    } finally {
      sub.close();
    }
    await settled(0);
  });
});
