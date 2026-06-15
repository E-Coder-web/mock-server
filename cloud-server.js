/**
 * Cloud backend for Aviator phone APK (HTTPS on Render).
 * Phone connects over mobile data — no local Wi-Fi needed.
 *
 * AVIATOR BRIDGE (PC) POSTs live odds to /api/ingest
 * Phone polls Socket.IO for status/signal/odds
 */
const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 3000);
const ID_LIST = process.env.ID_LIST || "759304290";
const PLAYER_ID = process.env.PLAYER_ID || "759304290";
const INGEST_KEY = process.env.INGEST_KEY || "engineer5252";

const sessions = new Map();
let lastVersion = -1;
let lastSnapshot = null;

function sid() {
  return crypto.randomBytes(8).toString("hex");
}

function parseUrl(req) {
  return new URL(req.url, "http://localhost");
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
        history: [],
        historyNew: "",
        roundId: 0,
        liveRoundId: 0,
        timeLeftMs: -1,
        hunting: false,
        connected: true,
        source: "777aviator",
        version: 0,
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
  pushSessionPackets(session, snapshot);
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
  const path = url.pathname.replace(/\/+$/, "") || "/";

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
      })
    );
  }

  if (path.includes("generate_204")) {
    res.writeHead(204);
    return res.end();
  }

  if (path.includes("/api/version_info/global/engineer5252")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(
      JSON.stringify({
        status: "ok",
        key: "engineer5252",
        active: true,
        player_id: PLAYER_ID,
      })
    );
  }

  if (path === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(
      JSON.stringify({
        ok: true,
        cloud: true,
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

  if (path === "/socket.io" || path.startsWith("/socket.io")) {
    const transport = url.searchParams.get("transport");
    const sidParam = url.searchParams.get("sid");

    if (transport === "polling" && req.method === "GET" && !sidParam) {
      const id = sid();
      sessions.set(id, { packets: [] });
      const open = `0{"sid":"${id}","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`;
      res.writeHead(200, { "Content-Type": "text/plain; charset=UTF-8" });
      return res.end(open);
    }

    if (transport === "polling" && req.method === "GET" && sidParam) {
      const session = sessions.get(sidParam) || { packets: [] };
      const out = session.packets.splice(0).join("") || "2:40";
      res.writeHead(200, { "Content-Type": "text/plain; charset=UTF-8" });
      return res.end(out);
    }

    if (transport === "polling" && req.method === "POST" && sidParam) {
      const session = sessions.get(sidParam) || { packets: [] };
      const body = await readBody(req);
      if (body.includes("40")) {
        session.packets.push(`40{"sid":"${sidParam}"}`);
        pushActiveSessionPackets(session, lastSnapshot);
      }
      sessions.set(sidParam, session);
      res.writeHead(200, { "Content-Type": "text/plain; charset=UTF-8" });
      return res.end("ok");
    }
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("not found");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Aviator cloud server on port", PORT);
  console.log("ID:", PLAYER_ID, "| ingest key:", INGEST_KEY);
});
