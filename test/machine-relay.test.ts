import { afterAll, beforeAll, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { signRequest } from "@kiwi/core/auth.ts";
import { generateIdentity, withExchangeKey, type Identity } from "@kiwi/core/identity.ts";
import { ASK_MAX_BYTES } from "@kiwi/core/machine-ask.ts";
import { certifyMachine } from "@kiwi/core/person.ts";
import { WS_PROTOCOL } from "@kiwi/core/protocol.ts";
import { startRelay } from "../src/relay/bun.ts";
import { MACHINE_BOX_TTL_MS, MACHINE_UNSEEN_MS, machineAlarmAt, machineScope, personScope, type PersonMachine } from "../src/relay/machine.ts";

setDefaultTimeout(30_000);

const dataDir = mkdtempSync(join(tmpdir(), "kiwi-machine-relay-"));
let t = Date.now();
let server: ReturnType<typeof startRelay>;
let relay: string;
let person: Identity;
let stranger: Identity;

beforeAll(async () => {
  server = startRelay({ port: 0, hostname: "127.0.0.1", dataDir, now: () => t });
  relay = server.url.origin;
  person = await withExchangeKey(await generateIdentity("person"));
  stranger = await withExchangeKey(await generateIdentity("stranger"));
});

beforeEach(() => {
  t = Date.now();
});

afterAll(() => {
  server.stop(true);
  rmSync(dataDir, { recursive: true, force: true });
});

const base = (m: Identity) => `/v1/machine/${m.pk}`;

async function token(id: Identity, m: Identity, method: string, path: string, text = ""): Promise<string> {
  return signRequest(id, machineScope(m.pk), method, path, text);
}

async function call(id: Identity, m: Identity, method: string, path: string, body?: unknown): Promise<Response> {
  const text = body === undefined ? "" : JSON.stringify(body);
  const auth = await token(id, m, method, path, text);
  return fetch(relay + path, { method, headers: { authorization: `Bearer ${auth}`, ...(text ? { "content-type": "application/json" } : {}) }, ...(text ? { body: text } : {}) });
}

async function registered(label: string, owner: Identity = person): Promise<Identity> {
  const m = await withExchangeKey(await generateIdentity(label));
  const cert = await certifyMachine(owner, { pk: m.pk, xpk: m.xpk!, label });
  expect((await call(m, m, "PUT", base(m), { cert })).status).toBe(204);
  return m;
}

async function openInbox(as: Identity, m: Identity): Promise<{ ws: WebSocket; got: unknown[]; closed: Promise<number> }> {
  const path = `${base(m)}/ws`;
  const ws = new WebSocket(relay.replace(/^http/, "ws") + path, [WS_PROTOCOL, await token(as, m, "GET", path)]);
  const got: unknown[] = [];
  ws.onmessage = (e) => got.push(JSON.parse(String(e.data)));
  const closed = new Promise<number>((resolve) => (ws.onclose = (e) => resolve(e.code)));
  await new Promise<void>((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = () => reject(new Error("the inbox socket didn't open"));
  });
  return { ws, got, closed };
}

async function until(what: string, ok: () => boolean | Promise<boolean>, ms = 5_000): Promise<void> {
  const end = Date.now() + ms;
  while (!(await ok())) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await Bun.sleep(20);
  }
}

describe("registering a paired computer", () => {
  test("only the machine, with its own cert; never another machine; never an older cert, even after an unpair", async () => {
    const m = await withExchangeKey(await generateIdentity("studio"));
    const other = await withExchangeKey(await generateIdentity("other"));
    const cert = await certifyMachine(person, { pk: m.pk, xpk: m.xpk!, label: "studio" }, t - 10_000);
    expect((await call(person, m, "PUT", base(m), { cert })).status).toBe(403);
    expect((await call(other, m, "PUT", base(m), { cert })).status).toBe(403);
    expect((await call(m, m, "PUT", base(m), { cert: await certifyMachine(person, { pk: other.pk, xpk: other.xpk!, label: "x" }) })).status).toBe(400);
    expect((await call(m, m, "PUT", base(m), { cert: { ...cert, label: "forged" } })).status).toBe(400);
    expect((await call(m, m, "PUT", base(m), { cert })).status).toBe(204);
    expect((await call(m, m, "PUT", base(m), { cert })).status).toBe(204);
    const older = await certifyMachine(stranger, { pk: m.pk, xpk: m.xpk!, label: "studio" }, t - 20_000);
    expect((await call(m, m, "PUT", base(m), { cert: older })).status).toBe(409);
    expect((await call(m, m, "DELETE", base(m))).status).toBe(204);
    expect((await call(m, m, "PUT", base(m), { cert })).status).toBe(409);
    expect((await call(m, m, "PUT", base(m), { cert: older })).status).toBe(409);
    expect((await call(m, m, "PUT", base(m), { cert: await certifyMachine(person, { pk: m.pk, xpk: m.xpk!, label: "studio" }, t) })).status).toBe(204);
  });
});

describe("a signature made for something else", () => {
  test("another path, another scope, another method, another body or an old one is refused", async () => {
    const m = await registered("signed");
    expect((await fetch(relay + base(m))).status).toBe(401);
    const statusToken = await token(person, m, "GET", base(m));
    expect((await fetch(relay + base(m), { headers: { authorization: `Bearer ${statusToken}` } })).status).toBe(200);
    const inboxToken = await token(person, m, "GET", `${base(m)}/inbox`);
    expect((await fetch(relay + base(m), { headers: { authorization: `Bearer ${inboxToken}` } })).status).toBe(401);
    const roomToken = await signRequest(person, "0123456789abcdef0123456789abcdef", "GET", base(m));
    expect((await fetch(relay + base(m), { headers: { authorization: `Bearer ${roomToken}` } })).status).toBe(401);
    expect((await fetch(relay + base(m), { method: "DELETE", headers: { authorization: `Bearer ${statusToken}` } })).status).toBe(401);
    const signedBody = JSON.stringify({ box: "a.b.c" });
    const postToken = await token(person, m, "POST", `${base(m)}/inbox`, signedBody);
    const swapped = await fetch(`${relay}${base(m)}/inbox`, { method: "POST", headers: { authorization: `Bearer ${postToken}` }, body: JSON.stringify({ box: "x.y.z" }) });
    expect(swapped.status).toBe(401);
    t = Date.now() + 6 * 60_000;
    expect((await fetch(relay + base(m), { headers: { authorization: `Bearer ${statusToken}` } })).status).toBe(401);
  });
});

describe("a paired computer's inbox", () => {
  test("takes sealed asks from its person only, delivers them on its socket, and shows it online meanwhile", async () => {
    const m = await registered("inbox");
    expect((await call(stranger, m, "POST", `${base(m)}/inbox`, { box: "x.y.z" })).status).toBe(403);
    expect((await call(m, m, "POST", `${base(m)}/inbox`, { box: "x.y.z" })).status).toBe(403);
    expect((await call(person, m, "POST", `${base(m)}/inbox`, { box: "x".repeat(ASK_MAX_BYTES + 1) })).status).toBe(400);
    expect((await call(person, m, "POST", `${base(m)}/inbox`, { box: "x".repeat(ASK_MAX_BYTES * 2) })).status).toBe(413);
    const first = (await (await call(person, m, "POST", `${base(m)}/inbox`, { box: "sealed.one.box" })).json()) as { id: string };
    await expect(openInbox(stranger, m)).rejects.toThrow();
    await expect(openInbox(person, m)).rejects.toThrow();
    const inbox = await openInbox(m, m);
    await until("the waiting ask on open", () => inbox.got.length === 1);
    expect(inbox.got[0]).toMatchObject({ box: { id: first.id, box: "sealed.one.box" } });
    expect(((await (await call(person, m, "GET", base(m))).json()) as { online: boolean }).online).toBe(true);
    expect((await call(stranger, m, "GET", base(m))).status).toBe(403);
    const second = (await (await call(person, m, "POST", `${base(m)}/inbox`, { box: "sealed.two.box" })).json()) as { id: string };
    await until("the live push", () => inbox.got.length === 2);
    expect(inbox.got[1]).toMatchObject({ box: { id: second.id, box: "sealed.two.box" } });
    expect((await call(person, m, "DELETE", `${base(m)}/inbox/${first.id}`)).status).toBe(403);
    expect((await call(m, m, "DELETE", `${base(m)}/inbox/${first.id}`)).status).toBe(204);
    expect((await call(person, m, "GET", `${base(m)}/inbox`)).status).toBe(403);
    const left = (await (await call(m, m, "GET", `${base(m)}/inbox`)).json()) as { boxes: { id: string }[] };
    expect(left.boxes.map((b) => b.id)).toEqual([second.id]);
    inbox.ws.close();
    await inbox.closed;
    await until("offline", async () => !((await (await call(person, m, "GET", base(m))).json()) as { online: boolean }).online);
  });

  test("an ask is gone after its few minutes, so stale ones never hold the slots", async () => {
    const m = await registered("expiry");
    await call(person, m, "POST", `${base(m)}/inbox`, { box: "old.ask.box" });
    t += MACHINE_BOX_TTL_MS - 1_000;
    expect(((await (await call(m, m, "GET", `${base(m)}/inbox`)).json()) as { boxes: unknown[] }).boxes).toHaveLength(1);
    t += 2_000;
    expect(((await (await call(m, m, "GET", `${base(m)}/inbox`)).json()) as { boxes: unknown[] }).boxes).toHaveLength(0);
  });

  test("at most 20 asks a minute, counted apart from the 20 that may wait", async () => {
    const rate = await registered("rate");
    const codes: number[] = [];
    for (let i = 0; i < 21; i++) {
      const res = await call(person, rate, "POST", `${base(rate)}/inbox`, { box: `box.${i}.x` });
      codes.push(res.status);
      if (res.status === 202) await call(rate, rate, "DELETE", `${base(rate)}/inbox/${((await res.json()) as { id: string }).id}`);
    }
    expect(codes.filter((c) => c === 202)).toHaveLength(20);
    expect(codes[20]).toBe(429);

    const full = await registered("full");
    for (let i = 0; i < 10; i++) expect((await call(person, full, "POST", `${base(full)}/inbox`, { box: `box.${i}.x` })).status).toBe(202);
    t += 61_000;
    for (let i = 10; i < 20; i++) expect((await call(person, full, "POST", `${base(full)}/inbox`, { box: `box.${i}.x` })).status).toBe(202);
    const refused = await call(person, full, "POST", `${base(full)}/inbox`, { box: "box.20.x" });
    expect(refused.status).toBe(429);
    expect(await refused.json()).toMatchObject({ error: "this machine already has 20 asks waiting" });
  });

  test("a machine nobody registered has no inbox or socket", async () => {
    const lone = await withExchangeKey(await generateIdentity("lone"));
    expect((await call(person, lone, "POST", `${base(lone)}/inbox`, { box: "a.b.c" })).status).toBe(403);
    await expect(openInbox(lone, lone)).rejects.toThrow();
  });
});

describe("unpairing", () => {
  test("by its person drops its inbox and closes its socket", async () => {
    const m = await registered("unpair-person");
    const inbox = await openInbox(m, m);
    expect((await call(stranger, m, "DELETE", base(m))).status).toBe(403);
    expect((await call(person, m, "DELETE", base(m))).status).toBe(204);
    expect(await inbox.closed).toBe(4000);
    expect((await call(person, m, "POST", `${base(m)}/inbox`, { box: "a.b.c" })).status).toBe(403);
    expect((await call(person, m, "GET", base(m))).status).toBe(403);
  });

  test("by the machine itself does the same", async () => {
    const m = await registered("unpair-machine");
    const inbox = await openInbox(m, m);
    expect((await call(m, m, "DELETE", base(m))).status).toBe(204);
    expect(await inbox.closed).toBe(4000);
    expect((await call(person, m, "POST", `${base(m)}/inbox`, { box: "a.b.c" })).status).toBe(403);
  });
});

async function computers(as: Identity, of: Identity = as, at = relay): Promise<Response> {
  const path = `/v1/person/${of.pk}/machines`;
  return fetch(at + path, { headers: { authorization: `Bearer ${await signRequest(as, personScope(of.pk), "GET", path)}` } });
}

async function listed(p: Identity, at = relay): Promise<PersonMachine[]> {
  const res = await computers(p, p, at);
  expect(res.status).toBe(200);
  return ((await res.json()) as { machines: PersonMachine[] }).machines;
}

describe("a person's paired computers", () => {
  test("lists only their own, by label, with each one's keys, label, and whether it's online", async () => {
    const me = await withExchangeKey(await generateIdentity("me"));
    const studio = await registered("studio", me);
    const attic = await registered("attic", me);
    await registered("theirs", stranger);
    expect(await listed(me)).toEqual([
      { key: attic.pk, xpk: attic.xpk!, label: "attic", online: false, seenAt: t },
      { key: studio.pk, xpk: studio.xpk!, label: "studio", online: false, seenAt: t },
    ]);
    const inbox = await openInbox(studio, studio);
    expect((await listed(me))[1]).toEqual({ key: studio.pk, xpk: studio.xpk!, label: "studio", online: true, since: t, seenAt: t });
    inbox.ws.close();
    await inbox.closed;
  });

  test("offline once its socket closes, last seen when it closed", async () => {
    const me = await withExchangeKey(await generateIdentity("me"));
    const laptop = await registered("laptop", me);
    const inbox = await openInbox(laptop, laptop);
    const opened = t;
    t += 60_000;
    inbox.ws.close();
    await inbox.closed;
    await until("offline", async () => !(await listed(me))[0]!.online);
    expect((await listed(me))[0]).toEqual({ key: laptop.pk, xpk: laptop.xpk!, label: "laptop", online: false, seenAt: opened + 60_000 });
  });

  test("only the person may list them, signed for this path", async () => {
    const me = await withExchangeKey(await generateIdentity("me"));
    await registered("private", me);
    const path = `/v1/person/${me.pk}/machines`;
    expect((await fetch(relay + path)).status).toBe(401);
    expect((await computers(stranger, me)).status).toBe(403);
    const machineToken = await signRequest(me, machineScope(me.pk), "GET", path);
    expect((await fetch(relay + path, { headers: { authorization: `Bearer ${machineToken}` } })).status).toBe(401);
    const otherPath = await signRequest(me, personScope(me.pk), "GET", `/v1/machine/${me.pk}`);
    expect((await fetch(relay + path, { headers: { authorization: `Bearer ${otherPath}` } })).status).toBe(401);
    const good = await signRequest(me, personScope(me.pk), "GET", path);
    expect((await fetch(relay + path, { method: "POST", headers: { authorization: `Bearer ${good}` } })).status).toBe(404);
  });

  test("an unpaired computer drops off, and one paired to someone else moves to their list", async () => {
    const me = await withExchangeKey(await generateIdentity("me"));
    const them = await withExchangeKey(await generateIdentity("them"));
    const kept = await registered("kept", me);
    const moving = await registered("moving", me);
    expect((await listed(me)).map((m) => m.label)).toEqual(["kept", "moving"]);
    expect((await call(me, moving, "DELETE", base(moving))).status).toBe(204);
    expect((await listed(me)).map((m) => m.key)).toEqual([kept.pk]);
    const cert = await certifyMachine(them, { pk: moving.pk, xpk: moving.xpk!, label: "theirs now" }, Date.now() + 1_000);
    expect((await call(moving, moving, "PUT", base(moving), { cert })).status).toBe(204);
    expect((await listed(them)).map((m) => [m.key, m.label])).toEqual([[moving.pk, "theirs now"]]);
    expect((await listed(me)).map((m) => m.key)).toEqual([kept.pk]);
  });

  test("a relay from before labels keeps its paired computers, unlabelled until each registers again", async () => {
    const dir = mkdtempSync(join(tmpdir(), "kiwi-machine-relay-old-"));
    const me = await withExchangeKey(await generateIdentity("me"));
    const old = await withExchangeKey(await generateIdentity("old"));
    const db = new Database(join(dir, "people.sqlite"), { create: true });
    db.run("CREATE TABLE machines (mpk TEXT PRIMARY KEY, person TEXT NOT NULL, mxpk TEXT NOT NULL, cert_at INTEGER NOT NULL, seen_at INTEGER NOT NULL)");
    db.query("INSERT INTO machines (mpk, person, mxpk, cert_at, seen_at) VALUES (?, ?, ?, ?, ?)").run(old.pk, me.pk, old.xpk!, t - 5_000, t - 5_000);
    db.close();
    const upgraded = startRelay({ port: 0, hostname: "127.0.0.1", dataDir: dir, now: () => t });
    try {
      const at = upgraded.url.origin;
      expect(await listed(me, at)).toEqual([{ key: old.pk, xpk: old.xpk!, label: "", online: false, seenAt: t - 5_000 }]);
      const cert = await certifyMachine(me, { pk: old.pk, xpk: old.xpk!, label: "old mac" }, t);
      const path = base(old);
      const text = JSON.stringify({ cert });
      const put = await fetch(at + path, { method: "PUT", headers: { authorization: `Bearer ${await token(old, old, "PUT", path, text)}`, "content-type": "application/json" }, body: text });
      expect(put.status).toBe(204);
      expect((await listed(me, at)).map((m) => m.label)).toEqual(["old mac"]);
    } finally {
      upgraded.stop(true);
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("the Worker object's alarm", () => {
  test("wakes for the oldest ask's expiry or a binding unseen for a month, whichever is first, and not at all with neither", () => {
    expect(machineAlarmAt(null, null)).toBeNull();
    expect(machineAlarmAt(1_000, null)).toBe(1_000 + MACHINE_BOX_TTL_MS);
    expect(machineAlarmAt(null, 5_000)).toBe(5_000 + MACHINE_UNSEEN_MS);
    expect(machineAlarmAt(1_000, 5_000)).toBe(1_000 + MACHINE_BOX_TTL_MS);
  });
});
