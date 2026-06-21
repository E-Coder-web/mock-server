/**
 * Cloud backend for Aviator phone APK (HTTPS on Render).
 * Phone connects over mobile data — no local Wi-Fi needed.
 *
 * AVIATOR BRIDGE (PC) POSTs live odds to /api/ingest
 * Phone polls Socket.IO for status/signal/odds
 */
const http = require("http");
const crypto = require("crypto");

// --- Socket.IO / Engine.IO v3 helpers (phone app requires EIO=3) ---
function isSocketPath(path) {
  return path.includes("/socket.io") || path.endsWith("socket.io");
}

function getSocketTransportOpts(url) {
  const eio = url.searchParams.get("EIO") === "4" ? 4 : 3;
  const b64 = url.searchParams.get("b64") === "1";
  return { eio, b64 };
}

function decodePollingBody(body, opts) {
  let raw = body || "";
  if (!raw || opts.eio !== 3) return raw;
  let out = "";
  let idx = 0;
  while (idx < raw.length) {
    const colon = raw.indexOf(":", idx);
    if (colon < 0) break;
    const len = Number.parseInt(raw.slice(idx, colon), 10);
    if (!Number.isFinite(len)) break;
    const msg = raw.slice(colon + 1, colon + 1 + len);
    if (msg.length !== len) break;
    out += msg;
    idx = colon + 1 + len;
  }
  return out || raw;
}

function encodePollingBody(packets, opts) {
  if (!packets.length) return opts.eio === 3 ? "0:" : "6";
  if (opts.eio === 3) {
    return packets.map((packet) => `${packet.length}:${packet}`).join("");
  }
  return packets.join("");
}

function buildOpenPacket(sessionId, upgrades = []) {
  const upgradeList = upgrades.length ? `"${upgrades.join('","')}"` : "";
  const json = `{"sid":"${sessionId}","upgrades":[${upgradeList}],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`;
  return `0${json}`;
}

function buildDefaultPayload(snapshot, source = "777aviator") {
  if (snapshot) {
    const mult =
      typeof snapshot.multiplier === "number" && snapshot.multiplier > 0
        ? snapshot.multiplier
        : 1.0;
    const multStr = mult.toFixed(2);
    const display = snapshot.display || snapshot.value || `${multStr}x`;
    const roundId = snapshot.roundId ?? snapshot.liveRoundId ?? 0;
    return {
      multiplier: mult,
      coef: mult,
      coefficient: mult,
      crashPoint: mult,
      value: display,
      display,
      history: snapshot.history || [],
      historyNew: snapshot.historyNew || snapshot.value || "",
      roundId,
      liveRoundId: snapshot.liveRoundId ?? roundId,
      timeLeftMs: snapshot.timeLeftMs ?? -1,
      hunting: Boolean(snapshot.hunting),
      connected: true,
      source,
      version: snapshot.version ?? 0,
    };
  }
  return {
    multiplier: 1.0,
    coef: 1.0,
    coefficient: 1.0,
    crashPoint: 1.0,
    value: "1.00x",
    display: "1.00x",
    history: ["2.00x", "1.50x", "1.20x"],
    historyNew: "1.00x",
    roundId: 1,
    liveRoundId: 1,
    timeLeftMs: -1,
    hunting: false,
    connected: true,
    source,
    version: 1,
  };
}

function buildRelayNotification(payload, row = "header_row_2") {
  const b64 = Buffer.from(JSON.stringify(payload)).toString("base64");
  return `NOTIFICATION:${row};${b64}`;
}

function enqueueSessionPackets(session, snapshot, playerId, source) {
  const payload = buildDefaultPayload(snapshot, source);
  const multStr = Number(payload.multiplier).toFixed(2);
  const notification = buildRelayNotification(payload);
  const noteJson = JSON.stringify(notification);
  session.packets.push(
    `42["status",{"connected":true,"player_id":"${playerId}","source":"${source}","roundId":${payload.roundId}}]`
  );
  session.packets.push(
    `42["signal",${JSON.stringify({
      multiplier: payload.multiplier,
      mode: "2x",
      value: payload.display,
      roundId: payload.roundId,
    })}]`
  );
  session.packets.push(
    `42["odds",${JSON.stringify({
      multiplier: multStr,
      x: multStr,
      coef: multStr,
      value: payload.display,
      roundId: payload.roundId,
    })}]`
  );
  session.packets.push(`42["message",${noteJson}]`);
  session.packets.push(`42["notification",${noteJson}]`);
}

function handleSocketPolling(req, res, url, sessions, hooks) {
  const transport = url.searchParams.get("transport");
  const sidParam = url.searchParams.get("sid");
  const opts = getSocketTransportOpts(url);
  const socketHeaders = { "Content-Type": "text/plain; charset=UTF-8" };

  if (transport !== "polling") {
    res.writeHead(404, { "Content-Type": "text/plain" });
    return res.end("not found");
  }

  if (req.method === "GET" && !sidParam) {
    const id = hooks.newSessionId();
    sessions.set(id, { packets: [] });
    const open = buildOpenPacket(id, hooks.upgrades || []);
    if (hooks.onOpen) hooks.onOpen(id, req);
    res.writeHead(200, socketHeaders);
    return res.end(encodePollingBody([open], opts));
  }

  if (req.method === "GET" && sidParam) {
    const session = sessions.get(sidParam) || { packets: [] };
    if (!session.activated) {
      session.activated = true;
      session.packets.push(`40{"sid":"${sidParam}"}`);
      hooks.onNamespaceConnect(session, sidParam);
      sessions.set(sidParam, session);
    }
    const out = encodePollingBody(session.packets.splice(0), opts);
    res.writeHead(200, socketHeaders);
    return res.end(out);
  }

  if (req.method === "POST" && sidParam) {
    return hooks.readBody(req).then((body) => {
      const session = sessions.get(sidParam) || { packets: [] };
      const decoded = decodePollingBody(body, opts);
      if (decoded === "2" || decoded.startsWith("2probe")) {
        res.writeHead(200, socketHeaders);
        return res.end(encodePollingBody(["3"], opts));
      }
      if (decoded.includes("40") && !session.activated) {
        session.activated = true;
        session.packets.push(`40{"sid":"${sidParam}"}`);
        hooks.onNamespaceConnect(session, sidParam);
      }
      sessions.set(sidParam, session);
      res.writeHead(200, socketHeaders);
      return res.end(encodePollingBody([], opts));
    });
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  return res.end("not found");
}

const PORT = Number(process.env.PORT || 3000);
const ID_LIST = process.env.ID_LIST || "759304290";
const PLAYER_ID = process.env.PLAYER_ID || "759304290";
const INGEST_KEY = process.env.INGEST_KEY || "engineer5252";
const SERVER_STARTED_AT = new Date().toISOString();

const sessions = new Map();
let lastVersion = -1;
let lastSnapshot = null;

function nowIso() {
  return new Date().toISOString();
}

function sid() {
  return crypto.randomBytes(8).toString("hex");
}

function parseUrl(req) {
  const raw = req.url || "/";
  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    return new URL(raw);
  }
  const pathOnly = raw.startsWith("/") ? raw : "/" + raw;
  return new URL("http://internal" + pathOnly);
}

function parseMultiplier(raw) {
  if (!raw) return { display: "1.00x", value: 1.0 };
  const s = String(raw).trim().toLowerCase().replace(/\s+/g, "");
  const m = s.match(/^(\d+(?:\.\d+)?)(x)?$/);
  if (!m) return { display: "1.00x", value: 1.0 };
  const num = Number(m[1]);
  const display = s.endsWith("x") ? s : `${m[1]}x`;
  return { display, value: Number.isFinite(num) ? num : 1.0 };
}

function buildPayload(snapshot) {
  const source =
    snapshot?.value ||
    snapshot?.historyNew ||
    snapshot?.latestHistory ||
    (Array.isArray(snapshot?.history) ? snapshot.history[0] : "") ||
    "";
  const parsed = parseMultiplier(source);
  const mult =
    typeof snapshot?.multiplier === "number" && snapshot.multiplier > 0
      ? snapshot.multiplier
      : parsed.value;
  const multStr = mult.toFixed(2);
  const roundId = snapshot?.roundId ?? snapshot?.liveRoundId ?? 0;
  return {
    multiplier: mult,
    coef: mult,
    coefficient: mult,
    crashPoint: mult,
    value: parsed.display || `${multStr}x`,
    display: parsed.display || `${multStr}x`,
    history: snapshot?.history || [],
    historyNew: snapshot?.value || snapshot?.historyNew || "",
    roundId,
    liveRoundId: snapshot?.liveRoundId ?? roundId,
    timeLeftMs: snapshot?.timeLeftMs ?? -1,
    hunting: Boolean(snapshot?.hunting),
    connected: true,
    source: "777aviator",
    version: snapshot?.version ?? 0,
  };
}

const TINY_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64"
);
const REGISTER_URL =
  process.env.REGISTER_URL || "https://wasafibet.co.tz/casino/list?register";

function versionInfoBody() {
  return JSON.stringify({
    status: "ok",
    success: true,
    key: "engineer5252",
    active: true,
    player_id: PLAYER_ID,
    register_url: REGISTER_URL,
    connected: true,
    source: "777aviator",
  });
}

function buildNotification(payload) {
  const b64 = Buffer.from(JSON.stringify(payload)).toString("base64");
  return `NOTIFICATION:header_row_2;${b64}`;
}

function pushSessionPackets(session, snapshot) {
  const payload = snapshot
    ? buildPayload(snapshot)
    : {
        multiplier: 1.0,
        coef: 1.0,
        coefficient: 1.0,
        crashPoint: 1.0,
        value: "1.00x",
        display: "1.00x",
        history: ["2.00x", "1.50x", "1.20x"],
        historyNew: "1.00x",
        roundId: 1,
        liveRoundId: 1,
        timeLeftMs: -1,
        hunting: false,
        connected: true,
        source: "777aviator",
        version: 1,
      };
  const multStr = Number(payload.multiplier).toFixed(2);
  const notification = buildNotification(payload);
  const noteJson = JSON.stringify(notification);
  session.packets.push(
    `42["status",{"connected":true,"player_id":"${PLAYER_ID}","source":"777aviator","roundId":${payload.roundId}}]`
  );
  session.packets.push(
    `42["signal",${JSON.stringify({
      multiplier: payload.multiplier,
      mode: "2x",
      value: payload.display,
      roundId: payload.roundId,
    })}]`
  );
  session.packets.push(
    `42["odds",${JSON.stringify({
      multiplier: multStr,
      x: multStr,
      coef: multStr,
      value: payload.display,
      roundId: payload.roundId,
    })}]`
  );
  session.packets.push(`42["message",${noteJson}]`);
  session.packets.push(`42["notification",${noteJson}]`);
}

function pushToAllSessions(snapshot) {
  for (const session of sessions.values()) {
    pushSessionPackets(session, snapshot);
  }
}

function pushActiveSessionPackets(session, snapshot) {
  if (snapshot) {
    pushSessionPackets(session, snapshot);
    return;
  }
  enqueueSessionPackets(session, null, PLAYER_ID, "777aviator");
}

function readBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => resolve(body));
  });
}

const server = http.createServer(async (req, res) => {
  const url = parseUrl(req);
  const path = url.pathname.replace(/\/+/g, "/").replace(/\/+$/, "") || "/";

  if (!path.includes("socket.io")) {
    console.log(nowIso(), req.method, path);
  }

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "*");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  if (path === "/api/id_list" || path.startsWith("/api/id_list")) {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end(ID_LIST);
  }

  if (path === "/api/start" || path.startsWith("/api/start")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(
      JSON.stringify({
        status: "ok",
        success: true,
        token: PLAYER_ID,
        player_id: PLAYER_ID,
        register_url: REGISTER_URL,
        active: true,
      })
    );
  }

  if (
    path.includes("/api/version_info/global/additional_row_for_first_line") ||
    path.includes("/api/version_info/global/engineer5252")
  ) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(versionInfoBody());
  }

  if (path.includes("generate_204")) {
    res.writeHead(204);
    return res.end();
  }

  if (path === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(
      JSON.stringify({
        ok: true,
        cloud: true,
        server_time_utc: nowIso(),
        server_started_at_utc: SERVER_STARTED_AT,
        connected: Boolean(lastSnapshot),
        version: lastVersion,
        sessions: sessions.size,
      })
    );
  }

  if (path === "/api/ingest" && req.method === "POST") {
    const body = await readBody(req);
    let data = null;
    try {
      data = JSON.parse(body || "{}");
    } catch {
      res.writeHead(400, { "Content-Type": "application/json" });
      return res.end('{"ok":false,"error":"invalid json"}');
    }
    if (data.key && data.key !== INGEST_KEY) {
      res.writeHead(403, { "Content-Type": "application/json" });
      return res.end('{"ok":false,"error":"bad key"}');
    }
    const version = data.version ?? lastVersion + 1;
    if (version !== lastVersion) {
      lastVersion = version;
      lastSnapshot = { ...data, ok: true, version };
      pushToAllSessions(lastSnapshot);
      console.log("[ingest] v" + version, data.value || data.multiplier);
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true, version: lastVersion }));
  }

  if (path === "/latest") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(
      JSON.stringify(
        lastSnapshot || { ok: true, connected: false, version: 0, value: "1.00x" }
      )
    );
  }

  if (path.includes("/media/img/") && path.endsWith(".jpg")) {
    res.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "public, max-age=3600" });
    return res.end(TINY_PNG);
  }

  if (isSocketPath(path)) {
    return handleSocketPolling(req, res, url, sessions, {
      newSessionId: sid,
      upgrades: [],
      readBody,
      onOpen(id, req) {
        console.log("[socket] open", id, req.headers["user-agent"] || "");
      },
      onNamespaceConnect(session) {
        pushActiveSessionPackets(session, lastSnapshot);
        console.log("[socket] connect packets queued");
      },
    });
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("not found");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Aviator cloud server on port", PORT);
  console.log("ID:", PLAYER_ID, "| ingest key:", INGEST_KEY);
});
