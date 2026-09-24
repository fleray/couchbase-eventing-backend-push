# couchbase-eventing-backend-push

Real-time push of Couchbase document changes to a plain HTML/JS web page: a Couchbase Eventing function calls a Node.js backend with `curl()`, and the backend relays each change to browsers over Server-Sent Events (SSE), with durable retries when the backend is unreachable.

```
┌──────────────┐  mutation   ┌───────────────────┐  curl() POST /hook   ┌──────────────────┐  SSE /events   ┌──────────┐
│  Couchbase   │ ──────────▶ │ Eventing function │ ───────────────────▶ │ Node.js backend  │ ─────────────▶ │ Browsers │
│  (source     │   (DCP)     │ push_to_backend.js│  Bearer token        │ server.js        │  EventSource   │index.html│
│  collection) │             └───────────────────┘  retries via timers  └──────────────────┘                └──────────┘
```

| Path | Content |
|---|---|
| [docker-compose.yml](docker-compose.yml) | Full demo stack: Couchbase, init job, data generator, backend |
| [eventing/push_to_backend.js](eventing/push_to_backend.js) | Eventing function: `OnUpdate` / `OnDelete` → `curl("POST", backend, …)`, retries with Eventing timers, optional dead-letter collection |
| [init/setup.py](init/setup.py) | One-shot init job (REST API, Python stdlib only): cluster, bucket, scopes, collections, Eventing function deployment |
| [app/generator.py](app/generator.py) | Python data generator (Couchbase Python SDK) |
| [server/server.js](server/server.js) | Express relay: `POST /hook` (token-protected) → SSE broadcast on `GET /events` |
| [client/index.html](client/index.html) | Dependency-free HTML/JS front end: `EventSource`, live document table and event log |
| [server/simulate-eventing.js](server/simulate-eventing.js) | Sends fake events to the backend, to test without Couchbase |

## Prerequisites

- **Docker** with Docker Compose v2 (Docker Desktop on macOS/Windows works). Give Docker at least **4 GB of RAM**: Couchbase alone uses about 1 GB for its service quotas.
- **Free host ports**: `8091` (Couchbase console) and `3000` (front end). Both can be remapped, see [Configuration](#configuration).
- **Internet access** on first run, to pull `couchbase/server:8.0.2`, `python:3.12-slim` and `node:22-alpine`.
- A web browser.

Optional, only to run the backend outside Docker: **Node.js 18+**.

Couchbase Server **Enterprise Edition** is required, because Eventing is not available in Community Edition. The `couchbase/server` image tags without a suffix (such as `8.0.2`) are Enterprise. On Capella, Eventing (and therefore `curl()`) is not available on the free tier.

## Setup

Nothing needs to be installed on the host besides Docker: all dependencies are installed inside the images at build time.

```bash
git clone <this-repo-url>
cd couchbase-eventing-backend-push
docker compose build
```

On `docker compose up`, the one-shot `couchbase-init` container configures Couchbase automatically:

1. Initializes the cluster with the Data, Index, Query and Eventing services.
2. Creates the bucket `demo_event`, the scope `app` with the collection `agent_event`, and the scope `eventing` with the collections `metadata` (Eventing metadata and timers) and `dlq` (dead letters).
3. Creates and deploys the Eventing function `push_to_backend` from [eventing/push_to_backend.js](eventing/push_to_backend.js), with its bindings and the "From now" feed boundary.

Each step checks what already exists, so the init job is safe to re-run.

### Deploying the Eventing function on another cluster (Capella or self-managed)

To use the function outside this demo, create it in the console under **Eventing** > **Add Function**:

1. **Source keyspace**: the collection to watch.
2. **Metadata keyspace**: a dedicated collection, never the source one. Retry timers are persisted there.
3. **Bindings**:

   | Type | Alias | Value | Access |
   |---|---|---|---|
   | URL Alias | `backend` | Backend base URL, **without** `/hook` (for example `https://api.example.com`). Auth **Bearer**, token = `HOOK_TOKEN` | – |
   | Bucket Alias | `src` | The source collection | Read Only |
   | Bucket Alias | `dlq` | _(optional)_ A dedicated dead-letter collection, **never the source** | Read and Write |

4. Paste the code of `eventing/push_to_backend.js`.
5. **Deploy** with the feed boundary set to **From now**. Otherwise the whole collection history is sent to the backend.

The backend must be **reachable from the cluster**. On Capella, that means exposed over public HTTPS. For a local test, a tunnel is enough (`cloudflared tunnel --url http://localhost:3000`), then use the `https://….trycloudflare.com` URL in the binding.

## Configuration

All settings are environment variables with defaults. Set them in the shell or in a `.env` file next to `docker-compose.yml`.

### Docker Compose stack

| Variable | Default | Purpose |
|---|---|---|
| `CB_UI_PORT` | `8091` | Host port of the Couchbase web console |
| `BACKEND_PORT` | `3000` | Host port of the backend and front end |
| `CB_VERSION` | `8.0.2` | `couchbase/server` image tag (must be Enterprise) |
| `CB_USERNAME` / `CB_PASSWORD` | `Administrator` / `password` | Couchbase admin credentials |
| `CB_BUCKET` / `CB_SCOPE` / `CB_COLLECTION` | `demo_event` / `app` / `agent_event` | Watched keyspace |
| `HOOK_TOKEN` | `demo-secret` | Shared secret between the Eventing URL binding and the backend |
| `MIN_DELAY_SEC` / `MAX_DELAY_SEC` | `1` / `5` | Random interval between two document creations |
| `UPDATE_PROBABILITY` / `DELETE_PROBABILITY` | `0.3` / `0.1` | Chance of also updating or deleting a document at each step. Set both to `0` for creations only |

### Backend ([server/server.js](server/server.js))

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `HOOK_TOKEN` | `change-me` | Expected Bearer token on `POST /hook` |
| `BUFFER_SIZE` | `500` | Number of events kept in memory for replay after a browser reconnects |
| `HEARTBEAT_MS` | `15000` | Interval of the SSE keep-alive comment (keeps connections open through proxies) |
| `CORS_ORIGIN` | _(empty)_ | Set it when the front end is served from another origin |

### Eventing function ([eventing/push_to_backend.js](eventing/push_to_backend.js))

Eventing only allows functions in the global scope, so the settings live in the `settings()` function at the top of the file:

| Setting | Default | Purpose |
|---|---|---|
| `keyPrefix` | `""` | Only forward documents whose key starts with this prefix (empty = all) |
| `maxAttempts` | `8` | Give up after this many attempts (about 20 minutes with the defaults) |
| `baseDelaySec` / `maxDelaySec` | `5` / `300` | Exponential backoff: 5 s, 10 s, 20 s, 40 s… capped at 5 minutes |

## Usage

### Run the demo

```bash
docker compose up --build
```

If ports `8091` or `3000` are already taken:

```bash
CB_UI_PORT=18091 BACKEND_PORT=13000 docker compose up --build
```

First start takes about one minute. The init job prints each step and ends with:

```
✅ Couchbase prêt : demo_event.app.agent_event -> Eventing -> http://backend:3000/hook
```

The `app` container then starts and logs its writes:

```
+ agent_event::e31188332155 hugo call_started (billing)
~ agent_event::66fb29191e2c -> in_progress
- agent_event::3d8026b80e3b
```

### What to look at

- **Front end**: http://localhost:3000. The status turns to **Connected** and new `agent_event::…` documents appear every 1 to 5 seconds. Each change briefly highlights its row, deletions remove the row, and every event is listed in the log on the right. The key-prefix field filters the stream server-side (for example `agent_event::`).
- **Couchbase console**: http://localhost:8091 (`Administrator` / `password`).
  - **Documents**: browse `demo_event` > `app` > `agent_event`.
  - **Eventing** > `push_to_backend`: deployed function, statistics and **Log**.
- **Raw SSE stream**: `curl -N http://localhost:3000/events`
- **Backend health**: `curl http://localhost:3000/health` returns the number of connected browsers and the last event id.

Open several browser tabs: they all receive the same events.

### Event format

Events are named `mutation`, `deletion` or `expiration`. Their id has the form `<boot>-<n>`, where `<boot>` identifies the backend instance:

```
id: muffqnhy-42
event: mutation
data: {"docId":"agent_event::cc4fd8d4ef3a","cas":"…","expiration":0,"keyspace":{…},"doc":{…},"attempt":1,"receivedAt":"…"}
```

- `deletion` and `expiration` events have no `doc` field.
- `attempt` is the delivery attempt number: 1 for a direct delivery, 2 and more for a retry.
- After a disconnection, `EventSource` reconnects on its own and sends `Last-Event-ID`. The backend replays the missed events still in its buffer. If the id belongs to a previous backend instance, it replays its whole buffer.

### Demonstrate retries on failure

Stop the backend container alone for more than a minute, then start it again:

```bash
docker stop cb-eventing-demo-backend-1
```

```bash
docker start cb-eventing-demo-backend-1
```

- During the outage, the function log (console > **Eventing** > `push_to_backend` > **Log**) shows lines such as `Tentative 3 échouée (Error: Unable to perform the request: Could not resolve hostname), relance dans 20 s pour mutation agent_event::…`.
- Once the backend is back, the front end receives the pending events, flagged `tentative 2`, `3`, `4`… in the log.

Do not use `docker compose stop backend`: it also stops `app`, which depends on it, so no document changes and nothing to retry. A very short outage (a few seconds) may trigger no retry at all: `curl()` waits on its existing connection and succeeds when the backend comes back.

How the retries work:

- **When**: network errors (backend down, DNS, TLS, timeouts), HTTP 5xx, 408 and 429. Other 4xx errors (401 wrong token, 400…) are configuration errors: no retry, straight to the dead-letter collection.
- **Durable**: retries use Eventing timers, persisted in the metadata collection, so they survive a restart.
- **One timer per document** (reference `retry::<key>`): a newer event on the same document replaces the pending retry, and a successful delivery cancels it.
- **Current state**: for a mutation, the document is re-read through the `src` binding when the retry fires. The latest version is sent, and a document deleted in the meantime is not resent. This also keeps the timer context well under its 1 KB default limit.
- **Giving up**: a line in the Eventing log and, when the `dlq` binding exists, a `dlq::<key>::<timestamp>` document in `demo_event.eventing.dlq` with the event and the failure reason.
- **Ordering**: a retry may arrive after a newer event. The front end compares `cas` values and ignores stale versions (flagged "ignoré" in the log).

### Test the backend without Couchbase

```bash
cd server
npm install
HOOK_TOKEN=a-long-secret npm start
```

Open http://localhost:3000, then in another terminal:

```bash
cd server
HOOK_TOKEN=a-long-secret npm run simulate
```

### Change the Eventing function

`couchbase-init` only creates the function when it does not exist yet. After editing `push_to_backend.js`, either reset the demo (see below) or paste the new code in the console (**Eventing** > `push_to_backend` > **Edit JavaScript**, then redeploy).

### Reset to a clean slate

```bash
docker compose down -v
```

```bash
docker compose up --build
```

`down -v` removes the containers and the Couchbase data volume. The next `up` recreates the cluster, the collections and the Eventing function from scratch. To pause the demo and resume it later with its data, use `docker compose stop` and `docker compose start` instead.

### Limitations

- **Delivery window**: retries cover a backend outage of up to about 20 minutes. Beyond that, or for guaranteed delivery with full history replay, use an outbox collection read by the backend, or the Couchbase Kafka connector.
- **At-least-once delivery**: if the backend processed an event but its response got lost, the event is sent again. Duplicates are harmless thanks to the `cas` check in the front end.
- **`curl()` is synchronous**: it blocks the Eventing worker until the response arrives, so `/hook` answers immediately. Keep an eye on the function's execution timeout.
- **Single backend instance**: connected clients and the replay buffer live in memory. Running several instances behind a load balancer needs a shared bus between them (Redis pub/sub, NATS…) and long-lived connection support on the load balancer.
- **Browser authentication**: `/events` is public in this demo. In production, protect it with a session cookie: `EventSource` cannot send custom headers, and the client already sets `withCredentials` for cross-origin use.

## Attribution

Original work, not adapted from an existing demo or dataset. It builds on:

- [Express](https://expressjs.com/) (MIT) for the backend.
- The [Couchbase Python SDK](https://docs.couchbase.com/python-sdk/current/hello-world/overview.html) (Apache 2.0) for the data generator.
- The Couchbase Eventing documentation, in particular the [cURL](https://docs.couchbase.com/server/current/eventing/eventing-curl-spec.html) and [timers](https://docs.couchbase.com/server/current/eventing/eventing-timers.html) pages.

## License

TODO: no license has been chosen yet. Without a `LICENSE` file, all rights are reserved by default. Add one (for example Apache 2.0) before sharing the repo outside Couchbase.

## Maintainer

Fabrice Leray (GitHub: TODO)
