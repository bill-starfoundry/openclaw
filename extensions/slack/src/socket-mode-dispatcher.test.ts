import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import { resolveSlackProxyDispatcher, resolveSlackSocketModeDispatcher } from "./client-options.js";

const PROXY_KEYS = [
  "ALL_PROXY",
  "HTTPS_PROXY",
  "HTTP_PROXY",
  "all_proxy",
  "https_proxy",
  "http_proxy",
  "NO_PROXY",
  "no_proxy",
  "OPENCLAW_PROXY_ACTIVE",
  "OPENCLAW_PROXY_CA_FILE",
] as const;
const originalEnv = { ...process.env };
const tempDirs: string[] = [];
const closers: Array<() => Promise<void> | void> = [];

// The undici copy @slack/socket-mode itself loads (bare import, as the SDK does).
type SocketModeUndici = typeof import("undici");
function loadSocketModeUndici(): SocketModeUndici {
  const requireFromTest = createRequire(import.meta.url);
  const requireFromBolt = createRequire(requireFromTest.resolve("@slack/bolt/package.json"));
  const requireFromSocketMode = createRequire(
    requireFromBolt.resolve("@slack/socket-mode/package.json"),
  );
  return requireFromSocketMode("undici") as SocketModeUndici;
}

function clearProxyEnv() {
  for (const key of PROXY_KEYS) {
    delete process.env[key];
  }
}

function restoreProxyEnv() {
  for (const key of PROXY_KEYS) {
    if (originalEnv[key] !== undefined) {
      process.env[key] = originalEnv[key];
    } else {
      delete process.env[key];
    }
  }
}

async function startConnectProxy(): Promise<{ url: string; targets: string[] }> {
  const targets: string[] = [];
  const server = http.createServer((_req, res) => {
    res.writeHead(405);
    res.end();
  });
  server.on("connect", (req, client, head) => {
    targets.push(String(req.url));
    const [host, port] = String(req.url).split(":");
    const upstream = net.connect(Number(port), host, () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      upstream.write(head);
      upstream.pipe(client);
      client.pipe(upstream);
    });
    upstream.on("error", () => client.destroy());
    client.on("error", () => upstream.destroy());
  });
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  closers.push(
    () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  );
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, targets };
}

async function startEchoWebSocketServer(): Promise<string> {
  const wss = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  wss.on("connection", (socket) =>
    socket.on("message", (data, isBinary) => socket.send(data, { binary: isBinary })),
  );
  await new Promise<void>((resolve) => {
    wss.once("listening", () => resolve());
  });
  closers.push(
    () =>
      new Promise<void>((resolve) => {
        for (const client of wss.clients) {
          client.terminate();
        }
        wss.close(() => resolve());
      }),
  );
  return `ws://127.0.0.1:${(wss.address() as AddressInfo).port}`;
}

// Open a WebSocket the way @slack/socket-mode 3 does and report the outcome.
function echoThroughSocketModeWebSocket(
  url: string,
  dispatcher: unknown,
): Promise<{ ok: true; echoed: string } | { ok: false; code?: number }> {
  const { WebSocket } = loadSocketModeUndici();
  return new Promise((resolve) => {
    const ws = new WebSocket(url, { dispatcher } as ConstructorParameters<typeof WebSocket>[1]);
    const timer = setTimeout(() => {
      ws.close();
      resolve({ ok: false });
    }, 5_000);
    ws.addEventListener("open", () => ws.send("hello"));
    ws.addEventListener("message", (event) => {
      clearTimeout(timer);
      ws.close();
      resolve({ ok: true, echoed: String(event.data) });
    });
    ws.addEventListener("close", (event) => {
      clearTimeout(timer);
      resolve({ ok: false, code: event.code });
    });
  });
}

describe("slack socket mode dispatcher", () => {
  beforeEach(() => {
    clearProxyEnv();
  });

  afterEach(async () => {
    for (const close of closers.splice(0).toReversed()) {
      await close();
    }
    for (const dir of tempDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
    restoreProxyEnv();
  });

  it("keeps Socket Mode's default connection when no proxy env is set", () => {
    expect(resolveSlackSocketModeDispatcher()).toBeUndefined();
  });

  it("builds the dispatcher from the undici copy Socket Mode uses", async () => {
    process.env.HTTPS_PROXY = "http://proxy.example.com:3128";
    const dispatcher = resolveSlackSocketModeDispatcher();
    const webApiDispatcher = resolveSlackProxyDispatcher();
    try {
      expect(dispatcher).toBeInstanceOf(loadSocketModeUndici().EnvHttpProxyAgent);
      // The Web API dispatcher comes from the runtime's undici and must not be
      // handed to Socket Mode.
      expect(webApiDispatcher).not.toBeInstanceOf(loadSocketModeUndici().EnvHttpProxyAgent);
    } finally {
      await dispatcher?.close();
      await webApiDispatcher?.close();
    }
  });

  it("creates the dispatcher while managed proxy CA trust is active", async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "openclaw-slack-socket-ca-"));
    tempDirs.push(dir);
    const caFile = path.join(dir, "ca.pem");
    writeFileSync(caFile, "slack-socket-mode-managed-proxy-ca");
    process.env.HTTPS_PROXY = "https://proxy.example.com:8443";
    process.env.OPENCLAW_PROXY_ACTIVE = "1";
    process.env.OPENCLAW_PROXY_CA_FILE = caFile;

    const dispatcher = resolveSlackSocketModeDispatcher();
    expect(dispatcher).toBeInstanceOf(loadSocketModeUndici().EnvHttpProxyAgent);
    await dispatcher?.close();
  });

  it("opens Socket Mode's WebSocket through an HTTP CONNECT proxy", async () => {
    const proxy = await startConnectProxy();
    const target = await startEchoWebSocketServer();
    process.env.HTTPS_PROXY = proxy.url;
    process.env.HTTP_PROXY = proxy.url;

    const dispatcher = resolveSlackSocketModeDispatcher();
    try {
      await expect(echoThroughSocketModeWebSocket(target, dispatcher)).resolves.toEqual({
        ok: true,
        echoed: "hello",
      });
      expect(proxy.targets).toContain(target.replace("ws://", ""));
    } finally {
      await dispatcher?.close();
    }
  });
});
