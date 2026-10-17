// This computer's key (machine.json). `kiwi pair` adds its person's cert (src/daemon/pair.ts), and
// agents started here then carry a computer claim in their join requests (computerClaimFor).

import { existsSync, readFileSync } from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";
import { home, writePrivate } from "./config.ts";
import { generateIdentity, type Identity } from "./identity.ts";
import { claimComputer, type ComputerClaim, type MachineCert } from "./person.ts";

export interface MachineFile {
  identity: Identity;
  label: string;
  relay: string;
}

const path = () => join(home(), "machine.json");

export function loadMachine(): MachineFile | null {
  try {
    return existsSync(path()) ? (JSON.parse(readFileSync(path(), "utf8")) as MachineFile) : null;
  } catch {
    return null;
  }
}

export function saveMachine(m: MachineFile): void {
  writePrivate(path(), JSON.stringify(m, null, 2) + "\n");
}

/** A fresh machine key, labelled after this computer. */
export async function newMachine(relay: string): Promise<MachineFile> {
  const label = (hostname().replace(/\.local$/, "") || "This computer").slice(0, 60);
  return { identity: await generateIdentity("machine"), label, relay };
}

/**
 * The claim an agent's join request carries when this computer is paired with its person's app:
 * the cert, and this computer's signature over this agent key in this channel. Null when unpaired.
 */
export async function computerClaimFor(roomId: string, agentPk: string): Promise<{ computer: ComputerClaim } | undefined> {
  const m = loadMachine() as (MachineFile & { cert?: MachineCert }) | null;
  if (!m?.cert || m.cert.machine !== m.identity.pk) return undefined;
  return { computer: await claimComputer(m.identity, m.cert, roomId, agentPk) };
}
