import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "127.0.0.1";
const PROXY_API_KEY = process.env.PROXY_API_KEY || "";
const UPSTREAM_BASE = (process.env.UPSTREAM_BASE || "https://api.xiaomimimo.com").replace(/\/+$/, "");
const MAX_429_WAIT_MS = Number(process.env.MAX_429_WAIT_MS || 180_000);
const DEFAULT_MODEL = process.env.DEFAULT_MODEL || "mimo-auto";
const BOOTSTRAP_USER_AGENT = "mimocode/0.1.0";
const CHAT_USER_AGENT = "mimocode/0.1.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14";
const REQUIRED_SYSTEM_MESSAGES = [
  {
    role: "system",
    content: "You are MiMoCode, an interactive CLI tool that helps users with software engineering tasks.",
  },
  {
    role: "system",
    content: "# Memory system",
  },
];

if (!PROXY_API_KEY) {
  console.error("PROXY_API_KEY is required. Refusing to start an open proxy.");
  process.exit(1);
}

const dataDir = path.join(os.homedir(), ".local", "share", "mimo-free-proxy");
const preferredClientFile = path.join(os.homedir(), ".local", "share", "mimocode", "mimo-free-client");
const fallbackClientFile = path.join(dataDir, "client");
const jwtFile = path.join(dataDir, "jwt");

let cachedJwt = "";
let cachedJwtExp = 0;
let bootstrapping = null;

function json(status, body, extraHeaders = {}) {
  return {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  };
}

function readBody(req, limitBytes = 20 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let total = 0;
    const chunks = [];
    req.on("data", (chunk) => {
      total += chunk.length;
      if (total > limitBytes) {
        reject(Object.assign(new Error("request body too large"), { status: 413 }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
}

function stableClient() {
  for (const file of [preferredClientFile, fallbackClientFile]) {
    try {
      const value = fs.readFileSync(file, "utf8").trim();
      if (value) return value;
    } catch {}
  }

  ensureDir(dataDir);
  const seed = [
    os.hostname(),
    process.platform,
    process.arch,
    os.cpus()[0]?.model || "unknown-cpu",
    os.userInfo().username || "unknown-user",
  ].join("|");
  const value = crypto.createHash("sha256").update(seed).digest("hex");
  fs.writeFileSync(fallbackClientFile, value, { mode: 0o600 });
  return value;
}

function jwtExp(jwt) {
  const [, payload] = String(jwt).split(".");
  if (!payload) return Date.now() + 30 * 60_000;
  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (typeof parsed.exp === "number") return parsed.exp * 1000;
  } catch {}
  return Date.now() + 30 * 60_000;
}

function readCachedJwt() {
  if (cachedJwt && cachedJwtExp - Date.now() > 5 * 60_000) return cachedJwt;
  try {
    const value = fs.readFileSync(jwtFile, "utf8").trim();
    const exp = jwtExp(value);
    if (value && exp - Date.now() > 5 * 60_000) {
      cachedJwt = value;
      cachedJwtExp = exp;
      return cachedJwt;
    }
  } catch {}
  return "";
}

function writeCachedJwt(jwt) {
  try {
    ensureDir(dataDir);
    fs.writeFileSync(jwtFile, jwt, { mode: 0o600 });
  } catch (error) {
    console.error(`could not persist jwt cache: ${error?.message || error}`);
  }
}

async function bootstrapJwt(force = false) {
  if (!force) {
    const cached = readCachedJwt();
    if (cached) return cached;
  }
  if (!force && bootstrapping) return bootstrapping;

  bootstrapping = (async () => {
    const started = Date.now();
    for (let attempt = 0; ; attempt++) {
      const resp = await fetch(`${UPSTREAM_BASE}/api/free-ai/bootstrap`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "*/*",
          "user-agent": BOOTSTRAP_USER_AGENT,
        },
        body: JSON.stringify({ client: stableClient() }),
      });
      const text = await resp.text();
      if (resp.ok) {
        const data = JSON.parse(text);
        if (!data.jwt) throw new Error("bootstrap response missing jwt");
        cachedJwt = data.jwt;
        cachedJwtExp = jwtExp(cachedJwt);
        writeCachedJwt(cachedJwt);
        return cachedJwt;
      }

      if (resp.status !== 429) throw new Error(`bootstrap failed ${resp.status}: ${text.slice(0, 500)}`);

      const delay = parseRetryAfter(resp.headers.get("retry-after")) ?? backoffMs(attempt);
      if (Date.now() - started + delay > MAX_429_WAIT_MS) {
        throw new Error(`bootstrap failed ${resp.status}: ${text.slice(0, 500)}`);
      }
      console.error(`bootstrap got 429, waiting ${Math.round(delay / 1000)}s before retry`);
      await sleep(delay);
    }
  })();

  try {
    return await bootstrapping;
  } finally {
    bootstrapping = null;
  }
}

function authorized(req) {
  const value = req.headers.authorization || "";
  const apiKey = req.headers["x-api-key"] || "";
  return value === `Bearer ${PROXY_API_KEY}` || apiKey === PROXY_API_KEY;
}

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000;
  const dateMs = Date.parse(value);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return null;
}

function backoffMs(attempt) {
  const steps = [3_000, 8_000, 15_000, 30_000, 45_000, 60_000];
  const base = steps[Math.min(attempt, steps.length - 1)];
  return base + Math.floor(base * Math.random() * 0.2);
}

function proxyHeaders(jwt) {
  return {
    "content-type": "application/json",
    accept: "*/*",
    authorization: `Bearer ${jwt}`,
    "x-mimo-source": "mimocode-cli-free",
    "x-session-affinity": `ses_${crypto.randomBytes(18).toString("base64url")}`,
    "user-agent": CHAT_USER_AGENT,
  };
}

async function upstreamChat(bodyText) {
  let jwt = await bootstrapJwt();
  const started = Date.now();

  for (let attempt = 0; ; attempt++) {
    const resp = await fetch(`${UPSTREAM_BASE}/api/free-ai/openai/chat`, {
      method: "POST",
      headers: proxyHeaders(jwt),
      body: bodyText,
    });

    if (resp.status === 401 || resp.status === 403) {
      jwt = await bootstrapJwt(true);
      continue;
    }

    if (resp.status !== 429) return resp;

    const delay = parseRetryAfter(resp.headers.get("retry-after")) ?? backoffMs(attempt);
    if (Date.now() - started + delay > MAX_429_WAIT_MS) return resp;
    await resp.arrayBuffer().catch(() => {});
    await sleep(delay);
  }
}

function normalizeChatBody(rawText) {
  const body = rawText ? JSON.parse(rawText) : {};
  body.model = body.model || DEFAULT_MODEL;
  if (body.model !== DEFAULT_MODEL && process.env.ALLOW_CUSTOM_MODEL !== "1") {
    body.model = DEFAULT_MODEL;
  }
  body.max_tokens = body.max_tokens || 128000;
  body.temperature = body.temperature ?? 1;
  body.messages = Array.isArray(body.messages) ? body.messages : [];
  const hasMimoSystem = body.messages.some(
    (message) =>
      message?.role === "system" &&
      typeof message.content === "string" &&
      message.content.includes("You are MiMoCode"),
  );
  const hasMemorySystem = body.messages.some(
    (message) =>
      message?.role === "system" &&
      typeof message.content === "string" &&
      message.content.includes("# Memory system"),
  );
  body.messages = [
    ...(!hasMimoSystem ? [REQUIRED_SYSTEM_MESSAGES[0]] : []),
    ...(!hasMemorySystem ? [REQUIRED_SYSTEM_MESSAGES[1]] : []),
    ...body.messages,
  ];
  if (body.stream && !body.stream_options) {
    body.stream_options = { include_usage: true };
  }
  return JSON.stringify(body);
}

function copyResponseHeaders(upstreamHeaders) {
  const headers = {};
  for (const [key, value] of upstreamHeaders.entries()) {
    const lower = key.toLowerCase();
    if (["connection", "content-encoding", "content-length", "transfer-encoding"].includes(lower)) continue;
    headers[key] = value;
  }
  headers["access-control-allow-origin"] = "*";
  return headers;
}

async function handle(req) {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

  if (req.method === "OPTIONS") {
    return {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-headers": "authorization, content-type",
        "access-control-allow-methods": "GET, POST, OPTIONS",
      },
      body: "",
    };
  }

  if (req.method === "GET" && url.pathname === "/health") {
    return json(200, {
      ok: true,
      upstream: UPSTREAM_BASE,
    });
  }

  if (!authorized(req)) {
    return json(401, { error: { message: "Unauthorized" } });
  }

  if (req.method === "GET" && (url.pathname === "/v1/models" || url.pathname === "/models")) {
    return json(200, {
      object: "list",
      data: [{ id: DEFAULT_MODEL, object: "model", owned_by: "mimo-free-proxy" }],
    });
  }

  const isChat =
    req.method === "POST" &&
    ["/v1/chat/completions", "/chat/completions", "/api/free-ai/openai/chat"].includes(url.pathname);

  if (!isChat) return json(404, { error: { message: "Not found" } });

  const rawText = await readBody(req);
  const upstreamBody = normalizeChatBody(rawText);
  const upstreamResp = await upstreamChat(upstreamBody);

  return {
    status: upstreamResp.status,
    headers: copyResponseHeaders(upstreamResp.headers),
    body: upstreamResp.body,
    stream: true,
  };
}

const server = http.createServer(async (req, res) => {
  try {
    const result = await handle(req);
    res.writeHead(result.status, result.headers);

    if (result.stream && result.body) {
      for await (const chunk of result.body) res.write(chunk);
      res.end();
      return;
    }

    res.end(result.body || "");
  } catch (error) {
    const status = error?.status || 500;
    const result = json(status, { error: { message: error?.message || String(error) } });
    res.writeHead(result.status, result.headers);
    res.end(result.body);
  }
});

server.listen(PORT, HOST, () => {
  console.log(`mimo-free-proxy listening on http://${HOST}:${PORT}`);
  console.log(`client prefix: ${stableClient().slice(0, 12)}`);
});
