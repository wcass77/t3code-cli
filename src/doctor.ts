import { access } from "node:fs/promises";

import { assertSupportedProtocol, withT3Api } from "./api.js";
import { CliError } from "./errors.js";
import { commandExists, resolveT3Invocation } from "./process.js";
import { hasProtocolHandler } from "./platformOpen.js";
import { discoverRuntime, resolveT3Home } from "./runtime.js";
import type { CliConfig } from "./types.js";

export async function doctor(config: CliConfig, configPath: string, configExists: boolean) {
  const [git, desktopProtocol, exactThreadProtocol, t3HomeExists, runtime] =
    await Promise.all([
      commandExists("git"),
      hasProtocolHandler("t3code"),
      hasProtocolHandler("t3"),
      access(resolveT3Home(config))
        .then(() => true)
        .catch(() => false),
      discoverRuntime(config, { startDesktopIfNeeded: false }).catch(() => null),
    ]);
  const invocation = await resolveT3Invocation(config.t3Command,
    runtime ? { runtime, t3Home: resolveT3Home(config) } : undefined).catch((error: unknown) => ({ error }));
  let protocolSupported = false;
  if (runtime) {
    try { assertSupportedProtocol(runtime); protocolSupported = true; } catch { /* Report below. */ }
  }
  const auth = runtime && protocolSupported && !("error" in invocation)
    ? await withT3Api(runtime, config, async (api) => {
        const snapshot = runtime.orchestrationProtocolVersion === 2
          ? await api.request("GET", "/api/projects") as { projects?: unknown }
          : await api.shellSnapshot();
        if (!Array.isArray(snapshot?.projects)) throw new CliError("T3_INVALID_SNAPSHOT", "T3 returned no project array.");
        return { ok: true, errorCode: null };
      }).catch((error: unknown) => ({ ok: false, errorCode: error instanceof CliError ? error.code : "T3_AUTH_FAILED" }))
    : { ok: false, errorCode: "T3_CONNECTION_UNAVAILABLE" };

  const invocationResult = "error" in invocation
    ? { available: false, source: null, version: null }
    : { available: true, source: invocation.source, version: invocation.version };
  const checks = {
    node: { ok: true, version: process.version },
    git: { ok: git },
    t3Cli: { ok: invocationResult.available, ...invocationResult },
    t3Home: { ok: t3HomeExists, path: resolveT3Home(config) },
    t3Server: {
      ok: runtime !== null,
      origin: runtime?.origin ?? null,
      environmentId: runtime?.environmentId ?? null,
      version: runtime?.serverVersion ?? null,
    },
    orchestrationProtocol: { ok: protocolSupported, version: runtime?.orchestrationProtocolVersion ?? (runtime ? 1 : null) },
    authentication: auth,
    desktopProtocol: { ok: desktopProtocol, scheme: "t3code" },
    exactThreadProtocol: { ok: exactThreadProtocol, scheme: "t3" },
    config: { ok: true, path: configPath, exists: configExists },
  };

  return {
    ok: checks.git.ok && checks.t3Cli.ok && checks.t3Server.ok && protocolSupported && auth.ok,
    checks,
  };
}
