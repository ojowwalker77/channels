// Self-hosted relay on Bun: same protocol as the Cloudflare relay, one SQLite
// file per room, created only when the room is and deleted when it closes.
// It can also serve the static site (site/), as the Worker does.
// Usage: bun src/relay/bun.ts --help

import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readdirSync, rmSync, statSync } from "node:fs";
import { basename, join, resolve, sep } from "node:path";
import type { ServerWebSocket } from "bun";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import { CLOSE_CLOSED, CLOSE_REMOVED, GONE_WORDS, MAX_BLOB_BYTES, PING, PONG } from "@kiwi/core/protocol.ts";
import { ChannelGone, NotFound, UpdateRequired } from "./failures.ts";
import { clockLayer, MAX_REQUEST_BYTES } from "./http.ts";
import { authkitDomains, devHostOk, devHumanAuth, devRequestOk, Human, HUMAN_HEADER, workosFromSettings, type HumanAuth } from "./human.ts";
import { MAILBOX_TTL_MS } from "@kiwi/core/mailbox.ts";
import { corsPreflight, onMailboxHttp, withCors, type MailboxStore } from "./mailbox.ts";
import {
  MACHINE_ASKS_PER_MINUTE,
  MACHINE_BOX_TTL_MS,
  MACHINE_UNSEEN_MS,
  MACHINE_WRITES_PER_ADDRESS,
  onMachineHttp,
  onPersonHttp,
  personMachine,
  type MachineBinding,
  type MachineBox,
  type MachineStore,
  type PersonMachinesStore,
} from "./machine.ts";
import { allow as allowPerMinute } from "./room/limits.ts";
import { clientAddress, parseTrustProxy, trustProxyFromArgs, trustProxyHint, trustsAProxy, untrustedProxy, writeAllowed, type TrustProxy } from "./address.ts";
import { Policy, policyFrom, type RelayPolicy, type RoomPolicy } from "./policy.ts";
import {
  Directory,
  forgetRoom,
  holdEntry,
  releasable,
  reserveSlot,
  RoomDb,
  relayConfig,
  authenticateSocket,
  errorResponse,
  expire,
  idleExpired,
  missingFingerprint,
  onClientFrame,
  wantsEcho,
  parseRoomPath,
  takesPresence,
  touch,
  wantsReads,
  wantsWatch,
  welcomeFrames,
  wsHeaders,
  type DirectoryEntry,
  type Effects,
  type Sql,
  type SqlParam,
} from "./room.ts";
import { makeRelay, onHttp, runRoom, runRoomSync } from "./route.ts";
import { GITHUB_QUEUE_MAX, GITHUB_QUEUE_TTL_MS, onGithubHttp, type GithubRoutes } from "./github.ts";
import { GithubApp, githubAppFrom, GithubPush, githubPushFrom, type GithubAppApi, type PushCheck } from "./github-app.ts";
import { edgePage, PAGE_CACHE_SECONDS, PAGE_PATH, type PageCache } from "./pages.ts";

interface SocketData {
  machine?: string;
  roomId: string;
  pk: string;
  since: number;
  presence: boolean;
  reads: boolean;
  echo: boolean;
  watch: boolean;
  devLimits?: boolean;
}

interface MachineRow {
  person: string;
  mxpk: string;
  label: string;
  cert_at: number;
  seen_at: number;
}

const bindingOf = (r: MachineRow): MachineBinding => ({ person: r.person, mxpk: r.mxpk, label: r.label, certAt: r.cert_at, seenAt: r.seen_at });

/** The static site's files, and the sign-in origins its page may call besides this relay. */
export interface WebApp {
  dir: string;
  connect?: string[];
}

export function startRelay(
  opts: {
    port?: number;
    hostname?: string;
    dataDir?: string;
    human?: HumanAuth | null;
    policy?: RelayPolicy;
    web?: WebApp | null;
    now?: () => number;
    trustProxy?: TrustProxy;
    githubSecret?: string | null;
    githubApp?: GithubAppApi | null;
    githubPush?: PushCheck | null;
    devNoRateLimits?: boolean;
  } = {},
) {
  if (opts.devNoRateLimits && !devHostOk(opts.hostname)) throw new Error("devNoRateLimits needs hostname 127.0.0.1 (or localhost)");
  const human = opts.human ?? null;
  // Settings come from the caller, or else from the environment, the same keys the Worker reads.
  const policy: RoomPolicy = { ...(opts.policy ?? policyFrom(process.env)), noRateLimits: false };
  const trustProxy: TrustProxy = Array.isArray(opts.trustProxy) ? parseTrustProxy(opts.trustProxy) : opts.trustProxy === true;
  if ((human?.dev || opts.devNoRateLimits) && trustsAProxy(trustProxy)) throw new Error("dev sign-in and devNoRateLimits can't go with trustProxy: a proxy in front passes other people's requests on");
  const web = opts.web ? webApp(opts.web) : null;
  const dataDir = opts.dataDir ?? ".relay-data";
  // Rooms hold ciphertext, member keys and join codes: only the relay's own user reads them.
  mkdirSync(dataDir, { recursive: true, mode: 0o700 });
  const dbs = new Map<string, Database>();
  const sqls = new Map<string, Sql>();
  const versions = new Map<string, number>();
  const sockets = new Map<string, Set<ServerWebSocket<SocketData>>>();
  const file = (roomId: string) => join(dataDir, `${roomId}.sqlite`);
  // Signed-in people's owned channels (room ids and join codes only): the quota on POST /create.
  const people = new Database(join(dataDir, "people.sqlite"), { create: true });
  people.run("CREATE TABLE IF NOT EXISTS owned (user TEXT NOT NULL, room TEXT NOT NULL, entry TEXT NOT NULL, PRIMARY KEY (user, room))");
  // One-time pairing mailboxes (src/relay/mailbox.ts): id-hashed, sealed
  // boxes the relay can't read. Expired rows are deleted on read and below.
  people.run("CREATE TABLE IF NOT EXISTS mailboxes (id TEXT PRIMARY KEY, rec TEXT NOT NULL)");
  people.run("CREATE TABLE IF NOT EXISTS github_routes (repo TEXT NOT NULL, room TEXT NOT NULL, PRIMARY KEY (repo, room))");
  people.run("CREATE TABLE IF NOT EXISTS machines (mpk TEXT PRIMARY KEY, person TEXT NOT NULL, mxpk TEXT NOT NULL, cert_at INTEGER NOT NULL, seen_at INTEGER NOT NULL)");
  if (!(people.query("PRAGMA table_info(machines)").all() as { name: string }[]).some((c) => c.name === "label")) people.run("ALTER TABLE machines ADD COLUMN label TEXT NOT NULL DEFAULT ''");
  people.run("CREATE INDEX IF NOT EXISTS machines_person ON machines (person)");
  people.run("CREATE TABLE IF NOT EXISTS machine_floors (mpk TEXT PRIMARY KEY, cert_at INTEGER NOT NULL)");
  people.run("CREATE TABLE IF NOT EXISTS machine_boxes (mpk TEXT NOT NULL, id TEXT NOT NULL, box TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (mpk, id))");
  const machineSockets = new Map<string, { ws: ServerWebSocket<SocketData>; since: number }>();
  const instance = crypto.randomUUID();
  const machines: MachineStore = {
    binding: async (mpk) => {
      const row = people.query("SELECT person, mxpk, label, cert_at, seen_at FROM machines WHERE mpk = ?").get(mpk) as MachineRow | null;
      return row ? bindingOf(row) : null;
    },
    floor: async (mpk) => (people.query("SELECT cert_at FROM machine_floors WHERE mpk = ?").get(mpk) as { cert_at: number } | null)?.cert_at ?? 0,
    bind: async (mpk, b) => {
      people.query("INSERT OR REPLACE INTO machines (mpk, person, mxpk, label, cert_at, seen_at) VALUES (?, ?, ?, ?, ?, ?)").run(mpk, b.person, b.mxpk, b.label, b.certAt, b.seenAt);
      people.query("INSERT INTO machine_floors (mpk, cert_at) VALUES (?, ?) ON CONFLICT (mpk) DO UPDATE SET cert_at = MAX(cert_at, excluded.cert_at)").run(mpk, b.certAt);
    },
    seen: async (mpk, at) => void people.query("UPDATE machines SET seen_at = ? WHERE mpk = ?").run(at, mpk),
    unbind: async (mpk) => {
      people.query("DELETE FROM machines WHERE mpk = ?").run(mpk);
      people.query("DELETE FROM machine_boxes WHERE mpk = ?").run(mpk);
    },
    pending: async (mpk, since) => (people.query("SELECT COUNT(*) AS n FROM machine_boxes WHERE mpk = ? AND at >= ?").get(mpk, since) as { n: number }).n,
    boxes: async (mpk, since) => people.query("SELECT id, box, at FROM machine_boxes WHERE mpk = ? AND at >= ? ORDER BY at").all(mpk, since) as MachineBox[],
    addBox: async (mpk, b) => void people.query("INSERT INTO machine_boxes (mpk, id, box, at) VALUES (?, ?, ?, ?)").run(mpk, b.id, b.box, b.at),
    dropBox: async (mpk, id) => void people.query("DELETE FROM machine_boxes WHERE mpk = ? AND id = ?").run(mpk, id),
    online: (mpk) => machineSockets.get(mpk)?.since ?? null,
    push: (mpk, b) => void machineSockets.get(mpk)?.ws.send(JSON.stringify({ box: b })),
    close: (mpk) => {
      machineSockets.get(mpk)?.ws.close(4000, "unpaired");
      machineSockets.delete(mpk);
    },
  };
  const personMachines: PersonMachinesStore = {
    machines: async (person) =>
      (people.query("SELECT mpk, person, mxpk, label, cert_at, seen_at FROM machines WHERE person = ?").all(person) as (MachineRow & { mpk: string })[]).map((r) =>
        personMachine(r.mpk, bindingOf(r), machineSockets.get(r.mpk)?.since ?? null),
      ),
  };
  const githubSecret = opts.githubSecret === undefined ? (process.env.GITHUB_WEBHOOK_SECRET ?? null) : opts.githubSecret;
  const mailboxes: MailboxStore = {
    load: async (id) => {
      const row = people.query("SELECT rec FROM mailboxes WHERE id = ?").get(id) as { rec: string } | null;
      return row ? (JSON.parse(row.rec) as { a: string | null; b: string | null; firstAt: number }) : null;
    },
    save: async (id, rec) => void people.query("INSERT OR REPLACE INTO mailboxes (id, rec) VALUES (?, ?)").run(id, JSON.stringify(rec)),
    drop: async (id) => void people.query("DELETE FROM mailboxes WHERE id = ?").run(id),
  };
  // Expired pairing mailboxes go on startup and hourly (reads expire them lazily too).
  const sweepMailboxes = () => {
    const now = Date.now();
    people.query("DELETE FROM machine_boxes WHERE at < ?").run(now - MACHINE_BOX_TTL_MS);
    for (const r of people.query("SELECT mpk FROM machines WHERE seen_at < ?").all(now - MACHINE_UNSEEN_MS) as { mpk: string }[]) {
      if (machineSockets.has(r.mpk)) continue;
      people.query("DELETE FROM machines WHERE mpk = ?").run(r.mpk);
      people.query("DELETE FROM machine_boxes WHERE mpk = ?").run(r.mpk);
    }
    for (const r of people.query("SELECT id, rec FROM mailboxes").all() as { id: string; rec: string }[]) {
      try {
        if (now - (JSON.parse(r.rec) as { firstAt: number }).firstAt > MAILBOX_TTL_MS) people.query("DELETE FROM mailboxes WHERE id = ?").run(r.id);
      } catch {
        people.query("DELETE FROM mailboxes WHERE id = ?").run(r.id);
      }
    }
  };
  sweepMailboxes();
  const mailboxSweep = setInterval(sweepMailboxes, 3_600_000);
  mailboxSweep.unref?.();

  /** The throwaway in-memory DBs standing in for rooms that don't exist, by the store using one: closed by doneWith. */
  const stubs = new WeakMap<RoomDb, Database>();
  /** A store for the room; rooms that don't exist get a throwaway in-memory DB (so no file appears). */
  const storeFor = (roomId: string, creating: boolean): RoomDb => {
    let db = dbs.get(roomId);
    let stub = false;
    if (!db) {
      if (!creating && !existsSync(file(roomId))) {
        db = new Database(":memory:");
        stub = true;
      } else {
        db = new Database(file(roomId), { create: true });
        db.run("PRAGMA journal_mode = WAL");
        dbs.set(roomId, db);
      }
    }
    const d = db;
    const made: Sql = {
      run: (q, ...p) => void d.query(q).run(...p),
      all: <T>(q: string, ...p: SqlParam[]) => d.query(q).all(...p) as T[],
    };
    const sql = stub ? made : (sqls.get(roomId) ?? made);
    if (!stub) {
      sqls.set(roomId, sql);
      const version = (d.query("PRAGMA data_version").get() as { data_version: number }).data_version;
      if (versions.get(roomId) !== version) forgetRoom(sql);
      versions.set(roomId, version);
    }
    const store = new RoomDb(sql, roomId);
    if (stub) stubs.set(store, d);
    return store;
  };
  const closeDb = (roomId: string) => {
    const sql = sqls.get(roomId);
    if (sql) forgetRoom(sql);
    sqls.delete(roomId);
    versions.delete(roomId);
    dbs.get(roomId)?.close();
    dbs.delete(roomId);
  };
  /** The caller is done with this store: a stand-in for a missing room is closed, a real room stays open. */
  const doneWith = (store: RoomDb) => {
    stubs.get(store)?.close();
    stubs.delete(store);
  };
  /** Creates in flight, by room: a refused one removes the file only when no other create of that id is still running. */
  const creating = new Map<string, number>();
  const listOf = (user: string) => (people.query("SELECT entry FROM owned WHERE user = ?").all(user) as { entry: string }[]).map((r) => JSON.parse(r.entry) as DirectoryEntry);

  // What every room runs with. `now` (tests) is the clock the room reads.
  const services = Layer.mergeAll(
    Layer.succeed(Human, human),
    Layer.succeed(Policy, policy),
    Layer.succeed(GithubApp, opts.githubApp === undefined ? githubAppFrom({ appId: process.env.GITHUB_APP_ID, privateKey: process.env.GITHUB_APP_PRIVATE_KEY }) : opts.githubApp),
    Layer.succeed(GithubPush, opts.githubPush === undefined ? (githubSecret ? githubPushFrom() : null) : opts.githubPush),
    Layer.succeed(Directory, {
      list: async (user: string) => listOf(user),
      // Synchronous SQLite: the count and the hold happen with nothing in between.
      reserve: async (user: string, room: string, most: number) => {
        const { ok, stale } = reserveSlot(listOf(user), room, most, Date.now());
        for (const r of stale) people.query("DELETE FROM owned WHERE user = ? AND room = ?").run(user, r);
        if (ok && !listOf(user).some((e) => e.room === room)) people.query("INSERT INTO owned (user, room, entry) VALUES (?, ?, ?)").run(user, room, JSON.stringify(holdEntry(room, Date.now())));
        return ok;
      },
      release: async (user: string, room: string) => {
        const row = people.query("SELECT entry FROM owned WHERE user = ? AND room = ?").get(user, room) as { entry: string } | null;
        if (row && releasable(JSON.parse(row.entry) as DirectoryEntry)) people.query("DELETE FROM owned WHERE user = ? AND room = ?").run(user, room);
      },
    }),
    clockLayer(opts.now),
  );
  const relay = makeRelay(services);
  const devRelay = Context.add(relay, Policy, { ...policy, noRateLimits: true });
  const devLimitsFor = (req: Request, peer: string | null) => !!opts.devNoRateLimits && devRequestOk(req, peer ?? undefined);
  const clock = opts.now ?? Date.now;
  const githubRoutes: GithubRoutes = {
    rooms: async (repo) => (people.query("SELECT room FROM github_routes WHERE repo = ?").all(repo) as { room: string }[]).map((r) => r.room),
    enqueue: async (room, item) => {
      if (!existsSync(file(room))) return false;
      const db = storeFor(room, false);
      if (!db.exists() || db.isLegacy()) return false;
      db.enqueueGithub(JSON.stringify(item), clock(), GITHUB_QUEUE_TTL_MS, GITHUB_QUEUE_MAX);
      return true;
    },
    drop: async (repo, room) => void people.query("DELETE FROM github_routes WHERE repo = ? AND room = ?").run(repo, room),
  };
  const pageCache = policy.pageKey ? memoryPages(64 * 1024 ** 2, clock) : null;
  const pageFromRoom = async (req: Request, roomId: string, rest: string): Promise<Response> => {
    const db = storeFor(roomId, false);
    try {
      return (await onHttp(relay, db, req, rest)).res;
    } finally {
      doneWith(db);
    }
  };

  const apply = (roomId: string, fx: Effects, sender?: ServerWebSocket<SocketData>) => {
    const room = sockets.get(roomId) ?? new Set();
    for (const ws of room) for (const f of (ws.data.watch ? fx.watch : undefined) ?? fx.broadcast ?? []) ws.send(f);
    if (fx.others) for (const ws of room) if (ws !== sender && !ws.data.watch) ws.send(fx.others);
    if (fx.ephemeral) for (const ws of room) if (ws !== sender && ws.data.presence) ws.send(fx.ephemeral);
    for (const f of fx.readers ?? []) for (const ws of room) if (ws.data.reads && !ws.data.watch) ws.send(f);
    if (fx.disconnect) for (const ws of room) if (ws.data.pk === fx.disconnect.pk) ws.close(CLOSE_REMOVED, GONE_WORDS[fx.disconnect.how]);
    for (const l of fx.github ?? []) {
      if (l.on) people.query("INSERT OR IGNORE INTO github_routes (repo, room) VALUES (?, ?)").run(l.repo, l.room);
      else people.query("DELETE FROM github_routes WHERE repo = ? AND room = ?").run(l.repo, l.room);
    }
    for (const u of fx.directory ?? []) {
      if (u.entry) people.query("INSERT OR REPLACE INTO owned (user, room, entry) VALUES (?, ?, ?)").run(u.user, u.room, JSON.stringify(u.entry));
      else people.query("DELETE FROM owned WHERE user = ? AND room = ?").run(u.user, u.room);
    }
    if (fx.wipe) {
      pageCache?.forget(roomId);
      closeDb(roomId);
      for (const suffix of ["", "-wal", "-shm"]) rmSync(file(roomId) + suffix, { force: true });
      for (const ws of room) ws.close(CLOSE_CLOSED, "channel closed");
      sockets.delete(roomId);
    }
  };

  /**
   * Expire idle rooms: the Bun relay's version of the Worker's alarm. Runs hourly when the relay
   * expires idle rooms at all; returns how many it expired (tests call it directly).
   */
  const sweep = (): number => {
    if (!policy.expireAfterDays) return 0;
    let expired = 0;
    for (const f of readdirSync(dataDir)) {
      const m = /^([0-9a-f]{32})\.sqlite$/.exec(f);
      if (!m || sockets.get(m[1]!)?.size) continue;
      const open = dbs.has(m[1]!);
      const db = storeFor(m[1]!, false);
      if (!runRoomSync(relay, db, idleExpired)) {
        if (!open) closeDb(m[1]!);
        continue;
      }
      apply(m[1]!, runRoomSync(relay, db, expire));
      expired++;
    }
    return expired;
  };
  const sweeper = policy.expireAfterDays ? setInterval(sweep, 3_600_000) : undefined;
  sweeper?.unref?.();

  let proxyHinted = false;
  const server = Bun.serve<SocketData>({
    port: opts.port ?? 8787,
    hostname: opts.hostname ?? "0.0.0.0",
    idleTimeout: 0,
    maxRequestBodySize: Math.max(MAX_REQUEST_BYTES, MAX_BLOB_BYTES) + 64 * 1024,
    async fetch(req, server) {
      try {
        const url = new URL(req.url);
        // Dev sign-in answers only this machine, directly: never a request a proxy passed on.
        if (human?.dev && req.headers.has(HUMAN_HEADER) && !devRequestOk(req, server.requestIP(req)?.address)) {
          return Response.json({ error: "dev sign-in only takes requests made on this machine, not through a proxy" }, { status: 403 });
        }
        if (web && !url.pathname.startsWith("/v1/") && (req.method === "GET" || req.method === "HEAD")) return web(url.pathname);
        if (url.pathname === "/") return new Response("Kiwi Channels relay (bun)\n");
        // The desktop app's renderer isn't served by the relay: /v1/* answers any origin.
        if (req.method === "OPTIONS" && url.pathname.startsWith("/v1/")) return corsPreflight();
        if (url.pathname === "/v1/config") return withCors(await relayConfig(human, process.env.GITHUB_APP_CLIENT_ID));
        const peer = server.requestIP(req)?.address ?? null;
        const forwarded = req.headers.get("x-forwarded-for");
        const ip = clientAddress(peer, forwarded, trustProxy);
        const proxy = proxyHinted ? null : untrustedProxy(peer, forwarded, trustProxy);
        if (proxy) {
          proxyHinted = true;
          process.stderr.write(`${trustProxyHint(proxy)}\n`);
        }
        const mailbox = await onMailboxHttp(req, mailboxes, ip ? { ip, allow: writeAllowed } : {});
        if (mailbox) return mailbox;
        const machine = await onMachineHttp(req, machines, {
          now: clock,
          perMachine: (key) => allowPerMinute(key, MACHINE_ASKS_PER_MINUTE, clock()),
          ...(ip ? { perAddress: () => allowPerMinute(`machine-ip:${instance}:${ip}`, MACHINE_WRITES_PER_ADDRESS, clock()) } : {}),
        });
        if (machine instanceof Response) return machine;
        const person = await onPersonHttp(req, personMachines, { now: clock });
        if (person) return person;
        if (machine) {
          const data: SocketData = { machine: machine.upgrade, roomId: "", pk: machine.upgrade, since: 0, presence: false, reads: false, echo: false, watch: false };
          if (server.upgrade(req, { data, headers: wsHeaders(req) })) return undefined;
          return withCors(errorResponse(new UpdateRequired({ message: "expected websocket" })));
        }
        const github = await onGithubHttp(req, githubSecret, githubRoutes, clock);
        if (github) return github;
        const route = parseRoomPath(url.pathname);
        if (!route) return withCors(errorResponse(new NotFound({ message: "not found" })));
        // No fingerprint: 426 before the room file is opened.
        const early = missingFingerprint(req.method, route.rest, url);
        if (early) return withCors(errorResponse(early));
        if (pageCache && PAGE_PATH.test(route.rest))
          return withCors(await edgePage(req, route.roomId, route.rest, policy.pageKey, pageCache, () => pageFromRoom(req, route.roomId, route.rest), clock()));
        const isCreate = route.rest === "/create";
        const db = storeFor(route.roomId, isCreate);
        if (isCreate) creating.set(route.roomId, (creating.get(route.roomId) ?? 0) + 1);
        const noRoom = () => {
          try {
            return !db.exists();
          } catch {
            return false;
          }
        };
        let empty: boolean | undefined;
        try {
          if (route.rest === "/ws") {
            if (db.isLegacy()) {
              apply(route.roomId, { wipe: true });
              return withCors(errorResponse(new ChannelGone({ message: "no such channel" })));
            }
            const pk = await runRoom(relay, db, authenticateSocket(req));
            const since = Number(url.searchParams.get("since") ?? 0) || 0;
            if (
              server.upgrade(req, {
                data: {
                  roomId: route.roomId,
                  pk,
                  since,
                  presence: takesPresence(url) && !wantsWatch(url),
                  reads: wantsReads(url),
                  echo: wantsEcho(url),
                  watch: wantsWatch(url),
                  devLimits: devLimitsFor(req, peer),
                },
                headers: wsHeaders(req),
              })
            )
              return undefined;
            return withCors(errorResponse(new UpdateRequired({ message: "expected websocket" })));
          }
          const result = await onHttp(devLimitsFor(req, peer) ? devRelay : relay, db, req, route.rest);
          if (isCreate) empty = noRoom();
          if (result.fx) apply(route.roomId, result.fx);
          return withCors(result.res);
        } finally {
          doneWith(db);
          if (isCreate) {
            const left = (creating.get(route.roomId) ?? 1) - 1;
            if (left) creating.set(route.roomId, left);
            else creating.delete(route.roomId);
            // A create that ended without a room (refused before auth, or failed) leaves no file behind.
            if (!left && (empty ?? noRoom())) {
              closeDb(route.roomId);
              for (const suffix of ["", "-wal", "-shm"]) rmSync(file(route.roomId) + suffix, { force: true });
            }
          }
        }
      } catch (err) {
        return withCors(errorResponse(err));
      }
    },
    websocket: {
      idleTimeout: 120,
      open(ws) {
        const mpk = ws.data.machine;
        if (mpk) {
          machineSockets.get(mpk)?.ws.close(4001, "replaced by a newer connection");
          machineSockets.set(mpk, { ws, since: clock() });
          for (const b of people.query("SELECT id, box, at FROM machine_boxes WHERE mpk = ? AND at >= ? ORDER BY at").all(mpk, clock() - MACHINE_BOX_TTL_MS) as MachineBox[]) {
            ws.send(JSON.stringify({ box: b }));
          }
          return;
        }
        let set = sockets.get(ws.data.roomId);
        if (!set) sockets.set(ws.data.roomId, (set = new Set()));
        set.add(ws);
        const db = storeFor(ws.data.roomId, false);
        try {
          for (const f of runRoomSync(relay, db, welcomeFrames(ws.data.since, ws.data.reads, ws.data.watch))) ws.send(f);
          // A connection counts as activity for idle expiry.
          runRoomSync(relay, db, touch);
        } finally {
          doneWith(db);
        }
      },
      message(ws, data) {
        const raw = typeof data === "string" ? data : new TextDecoder().decode(data);
        if (raw === PING) return void ws.send(PONG);
        if (ws.data.machine) return;
        const db = storeFor(ws.data.roomId, false);
        let fx: Effects;
        try {
          fx = runRoomSync(ws.data.devLimits ? devRelay : relay, db, onClientFrame(raw, ws.data.pk, ws.data.echo));
        } finally {
          doneWith(db);
        }
        for (const f of [fx.reply ?? []].flat()) ws.send(f);
        apply(ws.data.roomId, fx, ws);
      },
      close(ws) {
        const mpk = ws.data.machine;
        if (mpk) {
          if (machineSockets.get(mpk)?.ws !== ws) return;
          machineSockets.delete(mpk);
          people.query("UPDATE machines SET seen_at = ? WHERE mpk = ?").run(clock(), mpk);
          return;
        }
        sockets.get(ws.data.roomId)?.delete(ws);
        const db = storeFor(ws.data.roomId, false);
        try {
          runRoomSync(relay, db, touch);
        } finally {
          doneWith(db);
        }
      },
    },
  });
  return Object.assign(server, { sweep });
}

function memoryPages(maxBytes: number, now: () => number): PageCache & { forget(roomId: string): void } {
  const entries = new Map<string, { body: string; at: number }>();
  let bytes = 0;
  const drop = (key: string) => {
    const e = entries.get(key);
    if (!e) return;
    bytes -= e.body.length;
    entries.delete(key);
  };
  return {
    match: async (key) => {
      const e = entries.get(key);
      if (!e) return undefined;
      if (now() - e.at > PAGE_CACHE_SECONDS * 1000) return void drop(key);
      return new Response(e.body);
    },
    put: async (key, res) => {
      const body = await res.text();
      if (body.length > maxBytes / 8) return;
      drop(key);
      entries.set(key, { body, at: now() });
      bytes += body.length;
      for (const k of entries.keys()) {
        if (bytes <= maxBytes) break;
        drop(k);
      }
    },
    forget: (roomId) => {
      for (const k of [...entries.keys()]) if (k.includes(`/${roomId}/`)) drop(k);
    },
  };
}

/**
 * Serves the static site the way the Worker's static assets do: real files as
 * they are, any other page address gets index.html (so /join/<code> loads
 * the page), and the same security headers as site/_headers. The CSP is
 * built here because its sign-in origins depend on this relay's settings.
 */
function webApp(app: WebApp): (pathname: string) => Response {
  const root = resolve(app.dir);
  if (!existsSync(join(root, "index.html"))) throw new Error(`no index.html in ${root} (build it with: bun run web:build)`);
  const connect = ["'self'", ...(app.connect ?? [])].join(" ");
  const security = {
    "content-security-policy": `default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src ${connect}; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
  };
  const isFile = (p: string) => existsSync(p) && statSync(p).isFile();

  return (pathname) => {
    let rel: string;
    try {
      rel = decodeURIComponent(pathname);
    } catch {
      return new Response("bad path\n", { status: 400 });
    }
    const path = resolve(root, "." + rel);
    // Never outside the dashboard folder, and never Cloudflare's own config files.
    const inside = path.startsWith(root + sep) && !["_headers", "_redirects"].includes(basename(path));
    if (inside && isFile(path)) {
      const headers: Record<string, string> = { ...security };
      // Vite fingerprints everything under /assets; the rest must be rechecked each time.
      headers["cache-control"] = rel.startsWith("/assets/") ? "public, max-age=31536000, immutable" : "no-cache";
      // The installers are fetched with curl | sh and must read as text.
      if (/^\/install(\.sh|\.ps1)?$/.test(rel)) headers["content-type"] = "text/plain; charset=utf-8";
      return new Response(Bun.file(path), { headers });
    }
    // A missing file (it has an extension) is a 404, not the app: a stale script must fail loudly.
    if (/\.[A-Za-z0-9]+$/.test(basename(rel))) return new Response("not found\n", { status: 404, headers: security });
    return new Response(Bun.file(join(root, "index.html")), { headers: { ...security, "cache-control": "no-cache" } });
  };
}

const usage = (cmd: string) => `Kiwi Channels relay (Bun)

Usage: ${cmd} [options]

  --port <n>                    port to listen on (default 8787; env PORT)
  --hostname <addr>             address to bind (default 0.0.0.0; 127.0.0.1 behind a reverse proxy; env KIWI_HOSTNAME)
  --data <dir>                  where rooms are stored (default .relay-data; env KIWI_DATA)
  --web <dir>                   serve the static site from here (default site/ when present; env KIWI_WEB)
  --no-web                      don't serve the static site
  --workos-client-id <id>       turn on sign-in with your WorkOS AuthKit app (env WORKOS_CLIENT_ID)
  --workos-authkit-domain <url> your AuthKit domain, https://….authkit.app or your custom one; comma-separate
                                both while moving to a custom domain (env WORKOS_AUTHKIT_DOMAIN)
  --dev-sign-in                 testing only: the token "dev:<name>" signs in as <name>, no password.
                                Needs --hostname 127.0.0.1 (or localhost); refused on any other address.
  --dev-no-rate-limits          testing only: no per-minute cap on a member's messages or history reads,
                                for requests made on this machine directly (never through a proxy).
                                Needs --hostname 127.0.0.1 (or localhost), like --dev-sign-in.
  --trust-proxy [addr,cidr]     a reverse proxy sits in front (env KIWI_TRUST_PROXY=1, or a list).
                                Bare, or =1: trust a loopback peer. An address or CIDR (repeat
                                the flag, or comma-separate) trusts that peer too, for example
                                --trust-proxy 172.17.0.1 for Docker's bridge. A trusted peer's
                                last X-Forwarded-For address is the client. Any other peer
                                ignores the header. A prefix of /0 is refused.

  WORKOS_API_KEY (env only, it's a secret) lets the beta list match people by email.
  Without a WorkOS client id the relay has no sign-in: anyone can create channels.
  Beta and quota settings are read from the environment: the same KIWI_* keys the
  Worker reads from wrangler.jsonc (see src/relay/policy.ts). Unset: no gate, no quotas.
`;

/** The relay from the command line: `bun src/relay/bun.ts …` from a checkout, or `kiwi relay …` from the binary. */
export function runRelay(argv: string[], cmd = "bun src/relay/bun.ts"): void {
  if (argv.includes("--help") || argv.includes("-h")) {
    process.stdout.write(usage(cmd));
    process.exit(0);
  }
  const joined = argv.find((a) => /^--[a-z-]+=/.test(a));
  if (joined) {
    const flag = joined.slice(0, joined.indexOf("="));
    process.stderr.write(`${cmd} takes a flag's value after a space: write ${flag} ${joined.slice(flag.length + 1)}, not ${joined} (it would be ignored).\n`);
    process.exit(2);
  }
  const arg = (name: string, env: string) => {
    const i = argv.indexOf(name);
    return (i >= 0 ? argv[i + 1] : undefined) ?? (process.env[env] || undefined);
  };
  const authkitDomain = arg("--workos-authkit-domain", "WORKOS_AUTHKIT_DOMAIN");
  const hostname = arg("--hostname", "KIWI_HOSTNAME");
  const dev = argv.includes("--dev-sign-in");
  if (dev && !devHostOk(hostname)) {
    process.stderr.write("--dev-sign-in lets anyone sign in as anyone: it needs --hostname 127.0.0.1 (or localhost).\n");
    process.exit(2);
  }
  const unlimited = argv.includes("--dev-no-rate-limits");
  if (unlimited && !devHostOk(hostname)) {
    process.stderr.write("--dev-no-rate-limits lets one member flood a channel: it needs --hostname 127.0.0.1 (or localhost).\n");
    process.exit(2);
  }
  let trustProxy: TrustProxy;
  try {
    trustProxy = trustProxyFromArgs(argv, process.env.KIWI_TRUST_PROXY);
  } catch (err) {
    process.stderr.write(`${err instanceof Error ? err.message : "bad --trust-proxy"}\n`);
    process.exit(2);
  }
  const devFlag = dev ? "--dev-sign-in" : unlimited ? "--dev-no-rate-limits" : null;
  if (devFlag && trustsAProxy(trustProxy)) {
    process.stderr.write(`${devFlag} is for requests made on this machine: it can't go with --trust-proxy (or KIWI_TRUST_PROXY), since a proxy in front passes other people's requests on.\n`);
    process.exit(2);
  }
  const human = dev ? devHumanAuth() : workosFromSettings({ clientId: arg("--workos-client-id", "WORKOS_CLIENT_ID"), authkitDomain, apiKey: process.env.WORKOS_API_KEY || undefined });
  const siteDir = resolve(import.meta.dir, "../../site");
  const builtWeb = resolve(import.meta.dir, "../../web/dist");
  const defaultSite = existsSync(join(siteDir, "index.html")) ? siteDir : existsSync(join(builtWeb, "index.html")) ? builtWeb : undefined;
  const webDir = argv.includes("--no-web") ? undefined : (arg("--web", "KIWI_WEB") ?? defaultSite);
  const connect = human ? ["https://api.workos.com", ...authkitDomains(authkitDomain)] : [];
  const server = startRelay({
    port: Number(arg("--port", "PORT") ?? 8787),
    hostname,
    dataDir: arg("--data", "KIWI_DATA"),
    human,
    web: webDir ? { dir: webDir, connect } : null,
    trustProxy,
    devNoRateLimits: unlimited,
  });
  console.log(`Kiwi Channels relay listening on ${server.url}`);
  console.log(
    `  sign-in: ${human?.dev ? 'DEV: the token "dev:<name>" is <name>, no password (this machine only)' : human ? `WorkOS ${human.clientId}${human.profile ? " (with emails, for the beta list)" : ""}` : "off"}`,
  );
  if (unlimited) console.log("  rate limits: DEV: off for requests made on this machine, not through a proxy");
  console.log(`  site: ${webDir ? resolve(webDir) : "not served"}`);
}

if (import.meta.main) runRelay(process.argv.slice(2));
