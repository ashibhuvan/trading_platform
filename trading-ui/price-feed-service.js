/**
 * Price Feed Service
 *
 * Redis subscriber → WebSocket bridge.
 * Subscribes to Redis Pub/Sub channels (ticks:*, bars:*, status:feeds)
 * and fans out to connected WebSocket clients filtered by their symbol
 * subscriptions.
 *
 * Client protocol:
 *   → { "action": "subscribe",   "symbols": ["ES", "NQ"] }
 *   → { "action": "unsubscribe", "symbols": ["ES"] }
 *   ← { "type": "tick", "symbol": "ES", ... }
 *   ← { "type": "bar",  "symbol": "ES", "timeframe": "1m", ... }
 *   ← { "type": "status", "connected": true, "feeds": [...] }
 *
 * Env vars:
 *   REDIS_HOST  (default localhost)
 *   REDIS_PORT  (default 6379)
 *   WS_PORT     (default 8080)
 *   REDIS_CHANNEL_PREFIX (default "trading")
 */

const { createClient } = require("redis");
const { WebSocketServer } = require("ws");

// ── Config ────────────────────────────────────────────────────────────
const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);
const WS_PORT = parseInt(process.env.WS_PORT || "8080", 10);
const CHANNEL_PREFIX = process.env.REDIS_CHANNEL_PREFIX || "trading";
const HEARTBEAT_INTERVAL_MS = 5000;

// ── Redis subscriber ──────────────────────────────────────────────────
let redisSub = null;

async function connectRedis() {
  redisSub = createClient({
    socket: { host: REDIS_HOST, port: REDIS_PORT },
  });

  redisSub.on("error", (err) => {
    console.error("[redis] connection error:", err.message);
  });

  await redisSub.connect();
  console.log(`[redis] connected to ${REDIS_HOST}:${REDIS_PORT}`);

  // Subscribe to pattern channels for ticks and bars
  await redisSub.pSubscribe(
    `${CHANNEL_PREFIX}:ticks:*`,
    (message, channel) => {
      broadcast(message, "tick", channel);
    }
  );

  await redisSub.pSubscribe(
    `${CHANNEL_PREFIX}:bars:*`,
    (message, channel) => {
      broadcast(message, "bar", channel);
    }
  );

  // Subscribe to status channel (non-pattern)
  await redisSub.subscribe(
    `${CHANNEL_PREFIX}:status:feeds`,
    (message) => {
      broadcastAll(message);
    }
  );

  console.log("[redis] subscribed to tick/bar/status channels");
}

// ── WebSocket server ──────────────────────────────────────────────────

/** @type {Set<{ws: import('ws').WebSocket, symbols: Set<string>, alive: boolean}>} */
const clients = new Set();

function startWebSocket() {
  const wss = new WebSocketServer({ port: WS_PORT });

  wss.on("listening", () => {
    console.log(`[ws] server listening on port ${WS_PORT}`);
  });

  wss.on("connection", (ws) => {
    const client = { ws, symbols: new Set(), alive: true };
    clients.add(client);
    console.log(`[ws] client connected (${clients.size} total)`);

    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        handleClientMessage(client, msg);
      } catch {
        ws.send(JSON.stringify({ type: "error", message: "invalid JSON" }));
      }
    });

    ws.on("pong", () => {
      client.alive = true;
    });

    ws.on("close", () => {
      clients.delete(client);
      console.log(`[ws] client disconnected (${clients.size} total)`);
    });

    ws.on("error", (err) => {
      console.error("[ws] client error:", err.message);
      clients.delete(client);
    });
  });

  // Heartbeat: ping every client, drop unresponsive ones
  setInterval(() => {
    for (const client of clients) {
      if (!client.alive) {
        console.log("[ws] terminating unresponsive client");
        client.ws.terminate();
        clients.delete(client);
        continue;
      }
      client.alive = false;
      client.ws.ping();
    }
  }, HEARTBEAT_INTERVAL_MS);
}

// ── Message handling ──────────────────────────────────────────────────

function handleClientMessage(client, msg) {
  const { action, symbols } = msg;

  if (!Array.isArray(symbols)) return;

  if (action === "subscribe") {
    for (const s of symbols) client.symbols.add(s);
    client.ws.send(
      JSON.stringify({
        type: "subscribed",
        symbols: Array.from(client.symbols),
      })
    );
  } else if (action === "unsubscribe") {
    for (const s of symbols) client.symbols.delete(s);
    client.ws.send(
      JSON.stringify({
        type: "unsubscribed",
        symbols: Array.from(client.symbols),
      })
    );
  }
}

/**
 * Broadcast a tick or bar message to clients subscribed to its symbol.
 * The Redis channel encodes the symbol: e.g. "trading:ticks:ES"
 */
function broadcast(rawMessage, msgType, channel) {
  // Extract symbol from channel: prefix:ticks:SYMBOL or prefix:bars:SYMBOL:tf
  const parts = channel.split(":");
  const symbol = parts[2]; // "ES", "NQ", etc.

  if (!symbol) return;

  for (const client of clients) {
    if (client.symbols.has(symbol) && client.ws.readyState === 1) {
      client.ws.send(rawMessage);
    }
  }
}

/** Broadcast a message to all connected clients (e.g., status). */
function broadcastAll(rawMessage) {
  for (const client of clients) {
    if (client.ws.readyState === 1) {
      client.ws.send(rawMessage);
    }
  }
}

// ── Startup ───────────────────────────────────────────────────────────

async function main() {
  startWebSocket();
  await connectRedis();
}

main().catch((err) => {
  console.error("[fatal]", err);
  process.exit(1);
});
