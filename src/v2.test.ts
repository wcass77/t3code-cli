import { once } from "node:events";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { WebSocketServer } from "ws";
import { afterEach, expect, test } from "vitest";
import { DEFAULT_CONFIG } from "./config.js";
import { doctor } from "./doctor.js";
import { createHandoverThread, listProjects } from "./service.js";
import { resolveT3Invocation, runProcess } from "./process.js";
import type { CliConfig, T3Project } from "./types.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const clean of cleanups.splice(0).reverse()) await clean(); });
const version = "0.0.46-nightly.20261003.2623";

async function harness(options: { protocol?: number; rejectAuth?: boolean; failLaunch?: boolean; missingProject?: boolean; htmlProjects?: boolean } = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), "t3code-v2-"));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  await runProcess("git", ["init", "-b", "main"], { cwd: root });
  const home = path.join(root, ".t3");
  const state = path.join(home, "userdata");
  await mkdir(state, { recursive: true });
  const actionsPath = path.join(root, "auth-actions.jsonl");
  const mockT3 = path.join(root, "auth.mjs");
  await writeFile(mockT3, `import { appendFileSync } from 'node:fs';
const action=process.argv.includes('issue')?'issue':'revoke';
appendFileSync(${JSON.stringify(actionsPath)},JSON.stringify(action)+'\\n');
if(action==='issue')process.stdout.write(JSON.stringify({sessionId:'fixture-session',token:'fixture-token'}));\n`);
  const liveProject: T3Project = { id: "live-project", title: "Live project", workspaceRoot: root, defaultModelSelection: null };
  const projects = options.missingProject ? [] : [liveProject];
  // A valid preserved V1 projection must never override current V2 projects.
  const db = new DatabaseSync(path.join(state, "state.sqlite"));
  db.exec(`CREATE TABLE projection_projects (project_id TEXT, title TEXT, workspace_root TEXT,
    default_model_selection_json TEXT, default_thread_env_mode TEXT, scripts_json TEXT,
    created_at TEXT, updated_at TEXT, deleted_at TEXT)`);
  db.prepare("INSERT INTO projection_projects VALUES (?, ?, ?, NULL, NULL, '[]', '', '', NULL)")
    .run("stale-project", "Stale", root);
  db.close();
  const httpPaths: string[] = [];
  const rpcCalls: Array<{ tag: string; payload: Record<string, any> }> = [];
  const json = (res: ServerResponse, status: number, data: unknown) => {
    res.writeHead(status, { "content-type": "application/json" }); res.end(JSON.stringify(data));
  };
  const body = async (req: IncomingMessage) => { let s = ""; for await (const c of req) s += c; return JSON.parse(s); };
  const server = createServer(async (req, res) => {
    httpPaths.push(req.url!);
    if (req.url === "/.well-known/t3/environment") {
      json(res, 200, { environmentId: "env", serverVersion: version, orchestrationProtocolVersion: options.protocol ?? 2 }); return;
    }
    if (req.headers["x-t3-orchestration-protocol"] !== "2") {
      json(res, 426, { error: "protocol_header_required" }); return;
    }
    if (options.rejectAuth || req.headers.authorization !== "Bearer fixture-token") {
      json(res, 401, { error: "invalid_credential" }); return;
    }
    if (req.url === "/api/projects") {
      if (options.htmlProjects) { res.writeHead(200, { "content-type": "text/html" }); res.end("<html>T3 app</html>"); }
      else json(res, 200, { projects });
      return;
    }
    if (req.url === "/api/projects/mutate") {
      const command = await body(req);
      const project = { id: command.projectId, title: command.title, workspaceRoot: command.workspaceRoot,
        defaultModelSelection: command.defaultModelSelection };
      projects.push(project); json(res, 200, project); return;
    }
    if (req.url === "/api/auth/websocket-ticket") { json(res, 200, { ticket: "fixture-ticket" }); return; }
    json(res, 404, { error: "legacy_endpoint_removed" });
  });
  const ws = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url!, "http://localhost");
    if (url.searchParams.get("orchestrationProtocol") !== "2" || url.searchParams.get("wsTicket") !== "fixture-ticket") {
      socket.end("HTTP/1.1 426 Upgrade Required\r\n\r\n"); return;
    }
    ws.handleUpgrade(req, socket, head, (client) => {
      client.on("message", (raw) => {
        const request = JSON.parse(raw.toString());
        rpcCalls.push(request);
        const exit = (value: unknown) => client.send(JSON.stringify({ _tag: "Exit", requestId: request.id,
          exit: { _tag: "Success", value } }));
        if (request.tag === "server.getConfig") {
          exit({ providers: [{ instanceId: "codex", driver: "codex", enabled: true, models: [{ slug: "live-model",
            isDefault: true, capabilities: { optionDescriptors: [
              { id: "reasoningEffort", type: "select", options: [{ id: "medium" }, { id: "high" }] },
              { id: "serviceTier", type: "select", options: [{ id: "default", isDefault: true }, { id: "priority" }] },
            ] } }] }] }); return;
        }
        if (request.tag === "orchestration.launchThread") {
          if (options.failLaunch) {
            client.send(JSON.stringify({ _tag: "Exit", requestId: request.id, exit: { _tag: "Failure",
              cause: [{ _tag: "Fail", error: { _tag: "OrchestrationV2ThreadLaunchError", message: "launch rejected" } }] } }));
          } else {
            exit({ threadId: request.payload.threadId, projection: { runs: [{ id: "run-1", status: "preparing" }] } });
          }
          return;
        }
        throw new Error("Unexpected RPC tag");
      });
    });
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  const address = server.address(); if (!address || typeof address === "string") throw new Error("Missing address");
  const origin = `http://127.0.0.1:${address.port}`;
  await writeFile(path.join(state, "server-runtime.json"), JSON.stringify({ version: 1, pid: process.pid, port: address.port,
    origin, startedAt: new Date().toISOString() }));
  cleanups.push(async () => { for (const client of ws.clients) client.terminate(); ws.close(); server.closeAllConnections(); server.close(); await once(server, "close"); });
  const config: CliConfig = { ...DEFAULT_CONFIG, t3Home: home, origin, t3Command: [process.execPath, mockT3] };
  const actions = async () => (await readFile(actionsPath, "utf8").catch(() => "")).trim().split("\n").filter(Boolean).map(value => JSON.parse(value));
  return { root, home, config, rpcCalls, httpPaths, projects, actions };
}

test("V2 handover uses live projects/catalog and one protocol-2 launch; auth is revoked", async () => {
  const h = await harness();
  const result = await createHandoverThread(h.config, { cwd: h.root, prompt: "Prompt with $(literal) and `literal`",
    thinkingEffort: "medium", speedMode: "fast", openMode: "none" });
  expect(result.project.id).toBe("live-project");
  expect(result.thread).toMatchObject({ rpc: "orchestration.launchThread", runId: "run-1", status: "preparing" });
  const launch = h.rpcCalls.find(x => x.tag === "orchestration.launchThread")!;
  expect(launch.payload).toMatchObject({ threadId: result.thread.id, projectId: "live-project", runtimeMode: "full-access",
    interactionMode: "default", workspaceStrategy: { type: "root", branch: "main" },
    modelSelection: { instanceId: "codex", model: "live-model", options: [
      { id: "reasoningEffort", value: "medium" }, { id: "serviceTier", value: "priority" }] },
    initialMessage: { text: "Prompt with $(literal) and `literal`", attachments: [] } });
  expect(h.httpPaths).not.toContain("/api/orchestration/dispatch");
  expect(h.rpcCalls.filter(x => x.tag === "orchestration.launchThread")).toHaveLength(1);
  expect(await h.actions()).toEqual(["issue", "revoke"]);
});

test("V2 dry-run validates catalog but writes no project, thread, or worktree", async () => {
  const h = await harness({ missingProject: true });
  const result = await createHandoverThread(h.config, { cwd: h.root, prompt: "Plan", threadEnvMode: "worktree", dryRun: true });
  expect(result.thread.command).toMatchObject({ workspaceStrategy: { type: "worktree", baseRef: "main", startFromOrigin: true } });
  expect(h.rpcCalls.map(x => x.tag)).toEqual(["server.getConfig"]);
  expect(h.projects).toHaveLength(0);
  expect(h.httpPaths).not.toContain("/api/projects/mutate");
  expect(await h.actions()).toEqual(["issue", "revoke"]);
});

test("V2 missing project is created through the project domain before launch", async () => {
  const h = await harness({ missingProject: true });
  const result = await createHandoverThread(h.config, { cwd: h.root, prompt: "Start", openMode: "none" });
  expect(result.projectCreated).toBe(true);
  expect(h.projects[0]?.id).toBe(result.project.id);
  expect(h.httpPaths).toContain("/api/projects/mutate");
});

test("V2 linked worktree launch binds the existing checkout", async () => {
  const h = await harness();
  await runProcess("git", ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "Initial"], { cwd: h.root });
  const worktree = path.join(h.root, "linked");
  await runProcess("git", ["worktree", "add", "-b", "existing", worktree], { cwd: h.root });
  const result = await createHandoverThread(h.config, { cwd: worktree, prompt: "Keep checkout", openMode: "none" });
  expect(result.thread.command).toMatchObject({ workspaceStrategy: { type: "existing_worktree", worktreePath: worktree, branch: "existing" } });
});

test("V2 failed launch reports identifiers for reconciliation and revokes auth without deletion or retry", async () => {
  const h = await harness({ failLaunch: true });
  await expect(createHandoverThread(h.config, { cwd: h.root, prompt: "Start", openMode: "none" }))
    .rejects.toMatchObject({ code: "THREAD_START_FAILED", details: { threadId: expect.any(String), commandId: expect.any(String) } });
  expect(h.rpcCalls.map(x => x.tag)).toEqual(["server.getConfig", "orchestration.launchThread"]);
  expect(await h.actions()).toEqual(["issue", "revoke"]);
});

test("V2 doctor reports rejected authentication and revokes the credential", async () => {
  const h = await harness({ rejectAuth: true });
  const result = await doctor(h.config, "fixture-config", false);
  expect(result.ok).toBe(false);
  expect(result.checks.authentication).toMatchObject({ ok: false, errorCode: "T3_API_ERROR" });
  expect(await h.actions()).toEqual(["issue", "revoke"]);
});

test("V2 discovery rejects an app HTML fallback and still revokes authentication", async () => {
  const h = await harness({ htmlProjects: true });
  await expect(listProjects(h.config)).rejects.toMatchObject({ code: "T3_INVALID_RESPONSE" });
  expect(await h.actions()).toEqual(["issue", "revoke"]);
});

test("unknown orchestration protocols fail before any credential is issued", async () => {
  const h = await harness({ protocol: 3 });
  await expect(listProjects(h.config)).rejects.toMatchObject({ code: "T3_PROTOCOL_UNSUPPORTED" });
  expect(await h.actions()).toEqual([]);
});

test("V2 auth discovery prefers its installed executable over the V1 bundled CLI", async () => {
  const h = await harness();
  const executable = path.join(h.home, "runtime", "versions", version, process.platform === "win32" ? "t3.exe" : "t3");
  await mkdir(path.dirname(executable), { recursive: true }); await writeFile(executable, "fixture"); await chmod(executable, 0o755);
  const invocation = await resolveT3Invocation(undefined, { t3Home: h.home, runtime: {
    origin: h.config.origin!, environmentId: "env", serverVersion: version, orchestrationProtocolVersion: 2,
    stateDir: path.join(h.home, "userdata"), runtimeStatePath: null, settingsPath: null, capabilities: {} } });
  expect(invocation).toMatchObject({ command: executable, source: "runtime", version });
});
