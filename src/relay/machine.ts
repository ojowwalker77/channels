import { verifyRequest } from "@kiwi/core/auth.ts";
import { b64url } from "@kiwi/core/crypto.ts";
import { ASK_MAX_BYTES } from "@kiwi/core/machine-ask.ts";
import { readMachineCert } from "@kiwi/core/person.ts";
import { BadRequest, Conflict, Forbidden, NotFound, TooLarge, TooMany, Unauthorized, UpdateRequired } from "./failures.ts";
import { readCapped } from "./http.ts";
import { corsPreflight, withCors } from "./mailbox.ts";
import { errorResponse } from "./room/http.ts";
import { requestToken } from "./room/auth.ts";

export const MACHINE_BOX_TTL_MS = 3 * 60_000;
export const MACHINE_UNSEEN_MS = 30 * 24 * 3_600_000;
export const MACHINE_ASKS_PER_MINUTE = 20;
export const MACHINE_WRITES_PER_ADDRESS = 60;
const MACHINE_PENDING_MAX = 20;
const REGISTER_MAX_BYTES = 8_192;
const INBOX_MAX_BYTES = ASK_MAX_BYTES + 256;

export interface MachineBinding {
  person: string;
  mxpk: string;
  label: string;
  certAt: number;
  seenAt: number;
}

export interface MachineBox {
  id: string;
  box: string;
  at: number;
}

export interface MachineStore {
  binding(mpk: string): Promise<MachineBinding | null>;
  floor(mpk: string): Promise<number>;
  bind(mpk: string, b: MachineBinding): Promise<void>;
  seen(mpk: string, at: number): Promise<void>;
  unbind(mpk: string): Promise<void>;
  pending(mpk: string, since: number): Promise<number>;
  boxes(mpk: string, since: number): Promise<MachineBox[]>;
  addBox(mpk: string, b: MachineBox): Promise<void>;
  dropBox(mpk: string, id: string): Promise<void>;
  online(mpk: string): number | null;
  push(mpk: string, b: MachineBox): void;
  close(mpk: string): void;
}

export interface PersonMachine {
  key: string;
  xpk: string;
  label: string;
  online: boolean;
  since?: number;
  seenAt: number;
}

export interface PersonMachinesStore {
  machines(person: string): Promise<PersonMachine[]>;
}

export type MachineOutcome = Response | { upgrade: string } | null;

const PATH = /^\/v1\/machine\/([A-Za-z0-9_-]{43})(?:\/(inbox|ws)(?:\/([A-Za-z0-9_-]{22}))?)?$/;

const PERSON_PATH = /^\/v1\/person\/([A-Za-z0-9_-]{43})\/machines$/;

export const machineScope = (mpk: string): string => `machine:${mpk}`;

export const personScope = (pk: string): string => `person:${pk}`;

export function personMachine(mpk: string, b: MachineBinding, online: number | null): PersonMachine {
  return { key: mpk, xpk: b.mxpk, label: b.label, online: online !== null, ...(online === null ? {} : { since: online }), seenAt: b.seenAt };
}

const refuse = (err: unknown): Response => withCors(errorResponse(err));
const answer = (data: unknown, status = 200): Response => withCors(Response.json(data, { status }));
const done = (): Response => withCors(new Response(null, { status: 204 }));

export function machineAlarmAt(oldestBoxAt: number | null, seenAt: number | null): number | null {
  const times = [...(oldestBoxAt === null ? [] : [oldestBoxAt + MACHINE_BOX_TTL_MS]), ...(seenAt === null ? [] : [seenAt + MACHINE_UNSEEN_MS])];
  return times.length ? Math.min(...times) : null;
}

function parse(text: string): Record<string, unknown> | null {
  try {
    const v = JSON.parse(text) as unknown;
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
  } catch {
    // Not JSON: refused as a bad request by the caller.
    return null;
  }
}

export interface MachineRequest {
  mpk: string;
  sub: string | undefined;
  boxId: string | undefined;
  signer: string;
  body: string;
}

export async function signedMachineRequest(req: Request, now: number): Promise<MachineRequest | Response | null> {
  const url = new URL(req.url);
  const m = PATH.exec(url.pathname);
  if (!m) return null;
  const mpk = m[1]!;
  const sub = m[2];
  const max = sub === "inbox" ? INBOX_MAX_BYTES : REGISTER_MAX_BYTES;
  const bytes = req.method === "PUT" || req.method === "POST" ? await readCapped(req, max) : new Uint8Array(0);
  if (bytes === null) return refuse(new TooLarge({ message: "that request is too large for a machine route" }));
  const body = new TextDecoder().decode(bytes);
  const token = requestToken(req);
  const signer = token ? await verifyRequest(token, machineScope(mpk), req.method, url.pathname, body, now) : null;
  if (!signer) return refuse(new Unauthorized({ message: "sign this request for this machine and path" }));
  return { mpk, sub, boxId: m[3], signer, body };
}

export async function onMachineHttp(
  req: Request,
  store: MachineStore,
  opts: { now?: () => number; perMachine?: (key: string) => boolean | Promise<boolean>; perAddress?: () => boolean | Promise<boolean> } = {},
): Promise<MachineOutcome> {
  if (!PATH.test(new URL(req.url).pathname)) return null;
  if (req.method === "OPTIONS") return corsPreflight();
  const now = (opts.now ?? Date.now)();
  const signed = await signedMachineRequest(req, now);
  if (!signed || signed instanceof Response) return signed;
  const { mpk, sub, boxId, signer, body } = signed;
  const bound = await store.binding(mpk);
  const isMachine = signer === mpk;
  const isPerson = !!bound && signer === bound.person;
  const writes = (req.method === "PUT" || req.method === "POST") && opts.perAddress && !(await opts.perAddress());
  if (writes) return refuse(new TooMany({ message: "too many machine requests from this address; try again in a few minutes" }));

  if (!sub && req.method === "PUT") {
    if (!isMachine) return refuse(new Forbidden({ message: "only the machine registers itself" }));
    const cert = await readMachineCert(parse(body)?.cert);
    if (!cert || cert.machine !== mpk) return refuse(new BadRequest({ message: "a machine registers with its own cert" }));
    const floor = await store.floor(mpk);
    if (bound ? cert.at < bound.certAt : floor > 0 && cert.at <= floor) return refuse(new Conflict({ message: "a newer cert was registered for this machine" }));
    await store.bind(mpk, { person: cert.pk, mxpk: cert.mxpk, label: cert.label, certAt: cert.at, seenAt: now });
    return done();
  }
  if (!sub && req.method === "DELETE") {
    if (!isMachine && !isPerson) return refuse(new Forbidden({ message: "only the machine or its person unpairs it" }));
    await store.unbind(mpk);
    store.close(mpk);
    return done();
  }
  if (!sub && req.method === "GET") {
    if (!isPerson) return refuse(new Forbidden({ message: "only the machine's person sees whether it's online" }));
    const since = store.online(mpk);
    return answer(since === null ? { online: false } : { online: true, since });
  }
  if (sub === "inbox" && !boxId && req.method === "POST") {
    if (!isPerson) return refuse(new Forbidden({ message: "only the machine's person writes to its inbox" }));
    const box = parse(body)?.box;
    if (typeof box !== "string" || !box || box.length > ASK_MAX_BYTES) return refuse(new BadRequest({ message: "an inbox takes {box}, at most 4096 bytes" }));
    if (opts.perMachine && !(await opts.perMachine(machineScope(mpk)))) return refuse(new TooMany({ message: "too many asks for this machine; try again in a minute" }));
    if ((await store.pending(mpk, now - MACHINE_BOX_TTL_MS)) >= MACHINE_PENDING_MAX) return refuse(new TooMany({ message: "this machine already has 20 asks waiting" }));
    const kept: MachineBox = { id: b64url(crypto.getRandomValues(new Uint8Array(16))), box, at: now };
    await store.addBox(mpk, kept);
    store.push(mpk, kept);
    return answer({ id: kept.id }, 202);
  }
  if (sub === "inbox" && !boxId && req.method === "GET") {
    if (!isMachine || !bound) return refuse(new Forbidden({ message: "only a registered machine reads its inbox" }));
    return answer({ boxes: await store.boxes(mpk, now - MACHINE_BOX_TTL_MS) });
  }
  if (sub === "inbox" && boxId && req.method === "DELETE") {
    if (!isMachine) return refuse(new Forbidden({ message: "only the machine takes asks out of its inbox" }));
    await store.dropBox(mpk, boxId);
    return done();
  }
  if (sub === "ws" && req.method === "GET") {
    if (!isMachine || !bound) return refuse(new Forbidden({ message: "only a registered machine opens its inbox" }));
    if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") return refuse(new UpdateRequired({ message: "expected websocket" }));
    await store.seen(mpk, now);
    return { upgrade: mpk };
  }
  return refuse(new NotFound({ message: "not found" }));
}

export async function signedPersonRequest(req: Request, now: number): Promise<string | Response | null> {
  const url = new URL(req.url);
  const m = PERSON_PATH.exec(url.pathname);
  if (!m) return null;
  const pk = m[1]!;
  if (req.method !== "GET") return refuse(new NotFound({ message: "not found" }));
  const token = requestToken(req);
  const signer = token ? await verifyRequest(token, personScope(pk), "GET", url.pathname, "", now) : null;
  if (!signer) return refuse(new Unauthorized({ message: "sign this request for this person and path" }));
  if (signer !== pk) return refuse(new Forbidden({ message: "only the person lists their own computers" }));
  return pk;
}

export async function onPersonHttp(req: Request, store: PersonMachinesStore, opts: { now?: () => number } = {}): Promise<Response | null> {
  if (!PERSON_PATH.test(new URL(req.url).pathname)) return null;
  if (req.method === "OPTIONS") return corsPreflight();
  const pk = await signedPersonRequest(req, (opts.now ?? Date.now)());
  if (pk === null || pk instanceof Response) return pk;
  const machines = await store.machines(pk);
  return answer({ machines: machines.sort((a, b) => a.label.localeCompare(b.label) || a.key.localeCompare(b.key)) });
}
