// kiwi pair (src/daemon/pair.ts): the computer writes its signed ask to slot a of the relay's
// pairing mailbox, waits on slot b, checks the cert is for this computer, keeps it in
// machine.json and deletes the mailbox. Against a real relay (startRelay).

import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPairedMachine, pairComputer, PairError } from "../src/daemon.ts";
import { generateIdentity } from "../src/identity.ts";
import { loadMachine } from "../src/machine.ts";
import { answerPair, newSecret, openMailbox, pairCode, readPairAsk } from "../src/mailbox.ts";
import { startRelay } from "../src/relay/bun.ts";

setDefaultTimeout(30_000);

const home = mkdtempSync(join(tmpdir(), "kiwi-daemon-pair-"));
const dataDir = mkdtempSync(join(tmpdir(), "kiwi-daemon-pair-relay-"));
const prevHome = process.env.KIWI_HOME;
process.env.KIWI_HOME = home;

let server: ReturnType<typeof startRelay> | undefined;
let relay = "";
beforeAll(() => {
  server = startRelay({ port: 0, hostname: "127.0.0.1", dataDir });
  relay = server.url.origin;
});
afterAll(() => {
  server?.stop(true);
  if (prevHome === undefined) delete process.env.KIWI_HOME;
  else process.env.KIWI_HOME = prevHome;
  rmSync(home, { recursive: true, force: true });
  rmSync(dataDir, { recursive: true, force: true });
});

const silent = { onStatus: () => {} };

describe("kiwi pair", () => {
  test("a computer pairs through a real relay and stores its cert", async () => {
    const person = await generateIdentity("Ada Lovelace");
    const secret = newSecret();
    const pairing = pairComputer(pairCode(secret), { relay, timeoutMs: 15_000, ...silent });
    // The app's side, through the same mailbox: read the ask, confirm, answer.
    const app = await openMailbox(relay, secret);
    const ask = await readPairAsk(await app.wait("a", { timeoutMs: 10_000 }), app);
    expect(ask?.pk).toBe(loadMachine()?.identity.pk);
    expect(typeof ask?.label).toBe("string");
    const cert = await answerPair(person, app, ask!);
    const { cert: stored } = await pairing;
    expect(stored).toEqual(cert);
    // machine.json holds the cert and the person; the mailbox is deleted.
    expect(loadPairedMachine()?.cert).toEqual(cert);
    expect(loadPairedMachine()?.person).toEqual({ pk: person.pk, xpk: person.xpk!, name: "Ada Lovelace" });
    expect(await app.get("a")).toBeNull();
    expect(await app.get("b")).toBeNull();
  });

  test("a bad code is a human line, not a stack", async () => {
    await expect(pairComputer("kp1-short", { relay, ...silent })).rejects.toThrow(/isn't a pairing code/);
    await expect(pairComputer("mc2-not-a-pair-code", { relay, ...silent })).rejects.toThrow(/isn't a pairing code/);
    await expect(pairComputer("", { relay, ...silent })).rejects.toThrow(/isn't a pairing code/);
  });

  test("the slot is written once: a re-run with the same code resumes the same ask", async () => {
    const person = await generateIdentity("Ada Lovelace");
    const secret = newSecret();
    // The first run asks, then goes away (aborted) before the app answers.
    const ac = new AbortController();
    const first = pairComputer(pairCode(secret), { relay, timeoutMs: 15_000, signal: ac.signal, ...silent }).then(
      () => "paired",
      () => "aborted",
    );
    const app = await openMailbox(relay, secret);
    const ask = await readPairAsk(await app.wait("a", { timeoutMs: 10_000 }), app);
    ac.abort();
    expect(await first).toBe("aborted");
    // The second run finds its own ask already in slot a and just waits on it.
    const second = pairComputer(pairCode(secret), { relay, timeoutMs: 15_000, ...silent });
    await Bun.sleep(100);
    await answerPair(person, app, ask!);
    const { cert } = await second;
    expect(cert.machine).toBe(loadMachine()!.identity.pk);
    expect(loadPairedMachine()?.person?.pk).toBe(person.pk);
  });

  test("nothing approving in time is a timeout with exit code 3", async () => {
    const err = await pairComputer(pairCode(newSecret()), { relay, timeoutMs: 400, ...silent }).then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(PairError);
    expect((err as PairError).code).toBe(3);
    expect((err as PairError).message).toMatch(/in time/);
  });
});
