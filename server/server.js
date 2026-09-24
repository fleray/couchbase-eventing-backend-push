// Relais Couchbase Eventing -> navigateurs, en Server-Sent Events.
//
//   POST /hook    appelé par la fonction Eventing (curl), protégé par Bearer token
//   GET  /events  flux SSE auquel s'abonnent les navigateurs
//   GET  /health  sonde de vie
//   GET  /        sert le front-end (../client)

import express from "express";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PORT ?? 3000);
const HOOK_TOKEN = process.env.HOOK_TOKEN ?? "change-me";
const BUFFER_SIZE = Number(process.env.BUFFER_SIZE ?? 500); // événements gardés pour rejeu
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS ?? 15000);
const CORS_ORIGIN = process.env.CORS_ORIGIN ?? ""; // ex. https://app.monsite.com si front sur un autre domaine

if (HOOK_TOKEN === "change-me") {
  console.warn("⚠️  HOOK_TOKEN non défini : utilisation de la valeur par défaut 'change-me'.");
}

const app = express();
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- État en mémoire -------------------------------------------------------

/** @type {Map<import('express').Response, {prefix: string}>} */
const clients = new Map();
/** Tampon circulaire des derniers événements, pour le rejeu via Last-Event-ID. */
const history = [];
let lastEventId = 0;
// Les ids SSE sont "<boot>-<n>" : après un redémarrage du backend, le compteur repart à 1
// et un navigateur qui revient avec un id d'une instance précédente reçoit tout le tampon.
const BOOT_ID = Date.now().toString(36);

// --- CORS (seulement si le front est servi depuis une autre origine) -------

if (CORS_ORIGIN) {
  app.use("/events", (req, res, next) => {
    res.set("Access-Control-Allow-Origin", CORS_ORIGIN);
    res.set("Access-Control-Allow-Credentials", "true");
    next();
  });
}

// --- SSE : abonnement des navigateurs ---------------------------------------

app.get("/events", (req, res) => {
  const prefix = String(req.query.prefix ?? "");

  res.set({
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no", // désactive le buffering derrière nginx
  });
  res.flushHeaders();

  // Délai de reconnexion conseillé au navigateur.
  res.write("retry: 3000\n\n");

  // Rejeu : EventSource renvoie automatiquement Last-Event-ID à la reconnexion.
  const since = parseEventId(req.get("Last-Event-ID") ?? req.query.lastEventId);
  if (since !== null) {
    for (const evt of history) {
      if (evt.id > since && matches(evt, prefix)) writeEvent(res, evt);
    }
  }

  clients.set(res, { prefix });
  console.log(`+ client SSE (${clients.size} connectés)${prefix ? ` prefix=${prefix}` : ""}`);

  req.on("close", () => {
    clients.delete(res);
    console.log(`- client SSE (${clients.size} connectés)`);
  });
});

// Commentaire SSE périodique : garde la connexion ouverte à travers proxies / load balancers.
const heartbeat = setInterval(() => {
  for (const res of clients.keys()) res.write(": ping\n\n");
}, HEARTBEAT_MS);

// --- Webhook appelé par la fonction Eventing --------------------------------

app.post("/hook", requireToken, express.json({ limit: "5mb" }), (req, res) => {
  const { type, id } = req.body ?? {};
  if (!["mutation", "deletion", "expiration"].includes(type) || typeof id !== "string") {
    return res.status(400).json({ error: "payload invalide : 'type' et 'id' requis" });
  }

  const evt = { ...req.body, id: ++lastEventId, docId: id, receivedAt: new Date().toISOString() };
  history.push(evt);
  if (history.length > BUFFER_SIZE) history.shift();

  let delivered = 0;
  for (const [client, { prefix }] of clients) {
    if (matches(evt, prefix)) {
      writeEvent(client, evt);
      delivered++;
    }
  }

  // Répondre vite : curl() est synchrone et bloque le worker Eventing.
  res.status(200).json({ ok: true, eventId: evt.id, delivered });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", clients: clients.size, lastEventId });
});

// Front-end servi en même origine : pas de CORS nécessaire.
app.use(express.static(path.join(__dirname, "..", "client")));

// --- Utilitaires -------------------------------------------------------------

function requireToken(req, res, next) {
  const header = req.get("Authorization") ?? "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : "";
  const a = Buffer.from(token);
  const b = Buffer.from(HOOK_TOKEN);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
}

/** Numéro d'événement à partir duquel rejouer, 0 si l'id vient d'une instance précédente, null si absent. */
function parseEventId(raw) {
  if (!raw) return null;
  const [boot, seq] = String(raw).split("-");
  if (boot !== BOOT_ID) return 0;
  const n = Number(seq);
  return Number.isFinite(n) ? n : 0;
}

function matches(evt, prefix) {
  return !prefix || evt.docId.startsWith(prefix);
}

/** Envoie un événement SSE nommé ("mutation", "deletion", "expiration"). */
function writeEvent(res, evt) {
  const { id, type, ...data } = evt;
  res.write(`id: ${BOOT_ID}-${id}\nevent: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
}

// --- Démarrage / arrêt propre -------------------------------------------------

const server = app.listen(PORT, () => {
  console.log(`Relais SSE à l'écoute sur http://localhost:${PORT}`);
});

function shutdown() {
  clearInterval(heartbeat);
  for (const res of clients.keys()) res.end();
  server.close(() => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
