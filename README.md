# FlowForge — clustered Node.js workflow orchestrator

Two applications from one codebase:

| App | Command | Job |
|---|---|---|
| **master** | `node bin/master.js --config config/master-1.yaml` | REST API + Fabric.js designer, HTTP / RabbitMQ / MQTT / schedule triggers, workflow + instance store, scheduling. Run 2+ for high availability: one is elected **leader**, the rest are hot **followers**. |
| **worker** | `node bin/worker.js --config config/worker-1.yaml` | Only hosts workflow processes: one forked Node process per running workflow instance, kept alive until the workflow ends. Add workers to scale out. |

```
                          clients / webhooks / designer
                                     │
                              load balancer (any master)
                  ┌──────────────────┴──────────────────┐
          ┌───────▼────────┐   Peer.Invoke / Watch   ┌───▼────────────┐
          │ master-1 LEADER │◀───────────────────────│ master-2       │
          │ schedules work, │   (followers forward    │ follower: API, │
          │ broker triggers │    runtime calls)       │ hooks, reads   │
          └──┬───────────▲──┘                         └───────┬────────┘
             │ leader election + discovery (etcd or MongoDB)  │
             │           shared store (etcd or MongoDB)       │
   WorkerLink.Attach (bidi gRPC, Basic auth) — followers answer with Redirect
     ┌───────┴───────────┬─────────────────────┐
┌────▼─────┐        ┌────▼─────┐          ┌────▼─────┐
│ worker-1 │        │ worker-2 │   …      │ worker-N │   tags, capacity, drain
│ ├ wf A   │        │ ├ wf C   │          │          │   Orchestrator.Connect (loopback,
│ └ wf B   │        │ └ wf D   │          │          │   Basic instanceId:token)
└──────────┘        └──────────┘          └──────────┘
```

## Quick start

```bash
npm install
npm start                       # single box: master + embedded worker (config/single-node.yaml)
npm test                        # 18 e2e tests – master and worker as separate processes
ETCD_BIN=/path/to/etcd npm run test:cluster   # 14 HA tests: 2 masters + 2 workers, everything shared in etcd
```

Local HA cluster (needs etcd on 127.0.0.1:2379):

```bash
export FLOWFORGE_GRPC_PASSWORD=$(openssl rand -hex 16)               # resolved on each node, never stored
node bin/flowforge.js config push config/cluster.yaml --config config/master-1.yaml   # shared settings → etcd
node bin/master.js --config config/master-1.yaml     # :3000 / gRPC :50051
node bin/master.js --config config/master-2.yaml     # :3001 / gRPC :50052
node bin/worker.js --config config/worker-1.yaml     # finds the leader via etcd
node bin/worker.js --config config/worker-2.yaml
curl localhost:3001/api/cluster                      # works on any master
```

`docker compose up --build` runs etcd, 2 masters behind nginx (:8080), 2 workers, RabbitMQ, Mosquitto and MongoDB.

## Configuration: shared in etcd/MongoDB + a small YAML per node

In cluster mode **nothing lives in a node's local files**:

| What | Where (cluster mode) |
|---|---|
| Cluster-wide settings (gRPC credentials, store, brokers, data DB, log retention, worker limits …) | one **shared config document** in etcd or MongoDB (`config/cluster.yaml`, pushed with the CLI) |
| Workflow definitions, instances, activities, incoming request data, logs | the shared **store** (`store.type: etcd \| mongo`) |
| Rows written by the `db.write` activity | `dataDb`: MongoDB (`mongoUrl`, or the store's Mongo server) or etcd keys `<prefix>data/<collection>/<id>` – local JSON files are refused when `cluster.enabled` |
| Leader record, master + worker registry | the coordinator (`cluster.coordinator: etcd \| mongo`) |
| Node identity, ports, advertised address, TLS file paths, where the shared doc lives | the node's own small YAML (`config/master-1.yaml`, `config/worker-1.yaml` …) |

Effective config of a node = built-in defaults ← `common` ← `master` / `worker` section of the shared document ← the node's own YAML (local values win, e.g. a bigger `runtime.maxConcurrent` on a large worker). `${VAR}` / `${VAR:-default}` references are kept as text in etcd/Mongo and resolved by each node from its own environment, so passwords can stay out of the shared store.

A node YAML only needs:

```yaml
node: { id: master-1 }
http: { port: 3000, advertiseUrl: http://10.0.0.11:3000 }
grpc: { port: 50051, advertiseAddress: 10.0.0.11:50051 }
shared:
  source: etcd                       # or: mongo
  etcd: { hosts: ["10.0.0.5:2379"], prefix: /flowforge/ }
  # mongo: { url: mongodb://10.0.0.6:27017, db: flowforge }
```

**Managing the shared document**

```bash
node bin/flowforge.js config validate config/cluster.yaml
node bin/flowforge.js config push config/cluster.yaml --config config/master-1.yaml   # or --etcd host:2379 / --mongo <url>
node bin/flowforge.js config pull --config config/master-1.yaml [--reveal] [--out cluster.yaml]
node bin/flowforge.js config effective --config config/worker-1.yaml                  # merged view (secrets masked)
```

* Pushes are validated (master and worker configs are built from the document) and versioned with optimistic concurrency (`--if-version N`; `--force` overwrites). Pulled output masks secrets; pushing a masked `***` value is refused.
* Every running node **watches** the document. Live settings apply immediately — gRPC credentials, API auth, log level and retention, `workerLostGraceMs`, `onWorkerLost`, sync interval, terminate grace, worker capacity / buffer / drain timeout; everything else is logged as "restart required".
* Over HTTP: `GET /api/cluster/config` (document + this node's effective config, secrets masked) and `PUT /api/cluster/config` `{ doc, version }` (only with `security.api` Basic auth enabled). `PUT /api/settings/log-retention` writes into the shared document, so every master follows.
* A fresh cluster can start its nodes before the first push: they wait up to `shared.waitMs` (30 s) for it.

`config/single-node.yaml` shows the other mode: one self-contained YAML, file store, no shared document.

### Settings reference

**Master** (keys valid in the shared `master:`/`common:` sections or in a node YAML)

| Key | Default | Meaning |
|---|---|---|
| `node.id` | `master-<hostname>` | unique node id |
| `http.port`, `http.advertiseUrl` | 3000 | API / designer / hooks |
| `grpc.port`, `grpc.advertiseAddress` | 50051 | where workers and peers reach this master |
| `grpc.tls.{enabled,certFile,keyFile,caFile,requireClientCert}` | off | TLS for internal gRPC (recommended with Basic auth) |
| `security.grpc.{username,password}` | **set it** | Basic credentials every worker and peer master must present; `additionalUsers` for rotation |
| `security.api.{enabled,username,password,protectHooks}` | off | Basic auth for REST API + designer (and optionally `/hooks/*`) |
| `cluster.enabled`, `cluster.coordinator` | false, `none` | `etcd` or `mongo` leader election + discovery |
| `cluster.leaseTtlMs` | 10000 | leader lease; failover takes ≈ one lease |
| `cluster.workerLostGraceMs` | 15000 | how long a disconnected worker may take to re-attach before its instances are handled |
| `cluster.onWorkerLost` | `fail` | `fail` → instance FAILED (retry API available); `restart` → re-queued and re-run from the beginning on another worker |
| `cluster.syncIntervalMs` | 5000 | masters reload workflow definitions changed elsewhere |
| `cluster.etcd` / `cluster.mongo` | = store settings | coordinator connection |
| `store.type` + `store.{file,mongo,etcd}` | `file` | `memory`/`file` (single master only) or `mongo`/`etcd` (shared, required for clustering) |
| `rabbitmq.url`, `mqtt.url`, `mqtt.sharedGroup` | emulated | brokers (empty = in-process emulation for development) |
| `dataDb.type`, `dataDb.mongoUrl` | `auto` | `db.write` target: `mongo`, `etcd`, or `file` (single node only); `auto` = Mongo if `mongoUrl`, else the store's backend |
| `logs.retentionHours`, `logs.level` | 168, info | log retention (Mongo TTL index / etcd lease / periodic purge) |
| `runtime.defaultMaxOldSpaceMb` | 256 | heap per workflow process (per workflow: `settings.maxOldSpaceMb`) |
| `runtime.embeddedWorker.enabled` | false | also run a worker inside this master (single-box installs) |

**Worker** (shared `worker:`/`common:` sections or a node YAML)

| Key | Default | Meaning |
|---|---|---|
| `node.id`, `node.tags` | `worker-<hostname>`, `[]` | id; tags matched against a workflow's `settings.workerTags` |
| `grpc.host/port` | 127.0.0.1:50061 | loopback server the worker's child processes dial |
| `http.port` | 3101 | `GET /health` (503 while not attached to a leader) + status JSON |
| `security.grpc.{username,password}` | **set it** | credentials presented to the masters |
| `masters.tls` | off | TLS settings for dialling masters (`caFile`) |
| `discovery.type` | `static` | `static` (list of masters; any master redirects to the leader), `etcd` or `mongo` (registers the worker, follows the leader record) |
| `discovery.static.masters` | | `["host:50051", "host:50052"]` |
| `discovery.etcd` / `discovery.mongo` | | coordinator connection (e.g. Mongo URL) |
| `runtime.maxConcurrent` | 50 | workflow processes this worker accepts |
| `runtime.childMaxOldSpaceMb` | 256 | default heap per workflow process |
| `runtime.bufferLimit` | 50000 | messages kept while no leader is reachable |
| `runtime.drainTimeoutMs` | 60000 | on SIGTERM: stop taking work, wait for running workflows |

## How high availability works

| Event | What happens |
|---|---|
| **Leader master dies** | Its lease expires (≈ `leaseTtlMs`); a follower wins the election (new term = fencing number). Workers notice the dropped stream, **keep their workflow processes running**, buffer their messages, find the new leader (discovery or redirect) and re-register their running instances; the new leader **adopts** them, restores broker subscriptions and flushes the buffered messages. Workflows waiting on signals, timers, events or sub-workflows simply continue. In-flight publish/db requests are failed back to the node (its retry policy re-sends them). |
| **Follower master dies** | Nothing to recover; the load balancer stops sending it traffic. |
| **Master restarts** | Joins as follower; triggers and workflow definitions come from the shared store. |
| **Worker dies** | Its children die with it. After `workerLostGraceMs` the leader marks those instances FAILED (or re-runs them with `onWorkerLost: restart`). |
| **Worker network blip** | Re-attaches within the grace period → instances are adopted, nothing lost. |
| **Worker SIGTERM** | Drains: tells the leader to stop assigning work, waits for running workflows, then exits (rolling deploys). |
| **etcd / Mongo unreachable** | Leader steps down once its lease can't be renewed; no node schedules until the coordinator is back. |

Which master does what: every master serves the API, designer and `/hooks/*`; followers forward start/control/wait calls to the leader over gRPC (`Peer.Invoke`) and mirror its live events (`Peer.Watch`) for the designer. Broker consumers and schedule triggers run on the leader only, so events are not processed twice.

## Internal gRPC and authentication

`proto/orchestrator.proto` defines three services; every call carries `authorization: Basic base64(user:password)` metadata, checked in constant time:

* **WorkerLink.Attach** (worker → master, cluster credentials). Register (tags, capacity, running children) → Spawn / ToChild / Kill from the leader; Child envelopes, Spawned, Exited, heartbeats from the worker.
* **Peer.Invoke / Peer.Watch** (follower → leader, cluster credentials).
* **Orchestrator.Connect** (child → its worker on loopback, credentials `instanceId : random per-child token`; the token never leaves the worker).

Basic auth sends the password with every call, so enable `grpc.tls` (and `masters.tls.caFile` on workers) on any network you don't fully trust.

## Nodes (all parameters are configured in the designer; schema served by `GET /api/node-types`)

| Node | Ports | What it does |
|---|---|---|
| **Start** | out | Entry; `initVars` seeds `vars` |
| **Activity** | out, error (+timeout for receive) | `http` (call API) · `rabbitmq.publish` · `mqtt.publish` · `event.receive` (wait for a RabbitMQ/MQTT event, with filter expression + timeout) · `db.write` (insert/upsert/update/delete) · `transform` (JSON template). Retries, retry delay, timeout, `outputVar` |
| **Function** | out, error | Sandboxed JavaScript (`return` a value, mutate `vars`, `log()`, `await fetch`, `await sleep`) |
| **Decision** | true, false | Boolean expression |
| **Switch** | one per case + default | Routes on an expression value; cases are editable → ports update live |
| **Join** | out | Waits for all (or N) incoming branches |
| **Response** | out | Replies to the waiting synchronous HTTP caller, workflow continues |
| **Timer** | out | `at` a date/epoch expression, or `interval` × N ticks |
| **Delay** | out | `500`, `30s`, `5m`, `1h30m`, or an expression |
| **Signal** | out, timeout | `wait`: block until `POST /api/instances/:id/signal`; `listen`: armed for the whole run — each arrival **interrupts** by starting its branch (e.g. cancel → compensation → End) |
| **Sub-workflow** | out, error | Starts another workflow as a child instance (own process), optionally waits for its output; cancelled with the parent |
| **End** | – | Sets output and final status (COMPLETED / FAILED); cancels other branches |

Unconnected `error` ports mean failure fails the instance. Branches run concurrently (token-based executor); `settings.maxSteps` guards against infinite loops.

**Templates** — any string/JSON parameter may contain `{{ expression }}`. A value that is exactly one placeholder keeps its type (`"{{ input.items }}"` → array). Scope: `input`, `vars`, `nodes.<nodeId>` (outputs), `signals`, `workflow` (instanceId, workflowId…), `trigger` (headers/query/topic…), `env`, `now()`, `uuid()`.

## Triggers (per workflow, several allowed)

| type | config |
|---|---|
| `http` | `method`, `path` (`/orders/:id`), `sync` (wait for Response/End node, else 202), `responseTimeoutMs`, `instanceIdFrom` |
| `rabbitmq` | `queue` and/or `exchange` + `routingKey` pattern; messages are acked after the instance is persisted |
| `mqtt` | `topic` filter (`+`/`#`), `qos`; subscribed as `$share/<mqtt.sharedGroup>/…` when a group is configured |
| `schedule` | `intervalMs`, `input` |

**Instance id**: auto-generated UUID, or user-supplied via `instanceIdFrom` (`body:orderId`, `header:x-request-id`, `query:id`, `param:id`), `x-workflow-instance-id` header, `?instanceId=`, or `instanceId` in the start body. Ids are unique: a duplicate returns **409** (and duplicate broker events are ignored → idempotent consumers).

## REST API

| Method & path | |
|---|---|
| `GET/POST /api/workflows`, `GET/PUT/DELETE /api/workflows/:id` | CRUD (validated, versioned; triggers re-armed on save) |
| `POST /api/workflows/validate` | validate without saving |
| `POST /api/workflows/:id/start` | `{ instanceId?, input?, wait?: true\|'finish', timeoutMs? }` |
| `ALL /hooks/<path>` | HTTP triggers |
| `GET /api/instances?workflowId=&status=&parentInstanceId=&limit=&offset=` | list |
| `GET /api/instances/:id` (`?include=all`) | full instance (+ definition snapshot, activities, request) |
| `GET /api/instances/:id/status` | lightweight status, current nodes, activity counts, pid |
| `GET /api/instances/:id/activities[/:activityId]` | activity history (input, output, port, attempts, timings) |
| `GET /api/instances/:id/request` | stored incoming request/event data (auth headers redacted) |
| `GET /api/instances/:id/logs`, `GET /api/logs?level=&since=&until=` | logs |
| `GET /api/instances/:id/children` | sub-workflow instances |
| `GET /api/instances/:id/wait?timeoutMs=` | long-poll until finished |
| `POST /api/instances/:id/stop` | graceful: cancel waits, status STOPPED |
| `POST /api/instances/:id/terminate` | immediate; SIGKILL after grace period; status TERMINATED |
| `POST /api/instances/:id/pause` · `/resume` | no new node starts while paused |
| `POST /api/instances/:id/signal` | `{ name, payload }` |
| `POST /api/instances/:id/retry` | new instance, same input (`retryOf` link) |
| `DELETE /api/instances/:id` | finished instances only |
| `GET/PUT /api/settings/log-retention`, `POST /api/logs/purge` | retention |
| `POST /api/events/publish` | inject `{source:'rabbitmq'\|'mqtt', …}` (testing / bridging) |
| `GET /api/stream` | Server-Sent Events for live instance/activity updates |
| `GET /api/data`, `GET /api/data/:collection?limit=` | collections and rows written by `db.write` (shared backend) |
| `GET/PUT /api/cluster/config` | shared cluster config (see above) |
| `GET /api/cluster` | masters (leader/followers), workers (capacity, load, tags, draining), orphaned instances |
| `GET /api/health` | node id, leader flag, leader id, term (unauthenticated – for load balancers) |
| `GET /api/node-types` | node/trigger schema for the designer |

Statuses: `QUEUED → PENDING → RUNNING ⇄ WAITING ⇄ PAUSED → COMPLETED | FAILED | STOPPED | TERMINATED`. Activity statuses: `RUNNING, RETRYING, COMPLETED, FAILED, CANCELLED`.

## Designer

Fabric.js canvas on a 20 px grid (snap-to-grid), palette drag & drop, drag from an output port to another node to connect, `Del` to delete, `Ctrl+D` duplicate, `Ctrl+S` save, wheel zoom, Alt/Space-drag to pan. The property panel is generated from the node-type schema (fields appear/disappear with the chosen activity kind, switch cases add ports). The **Cluster** button shows masters and workers. The instance drawer shows live runs; selecting one colours the graph by activity status and offers signal / pause / resume / stop / terminate / retry.

## Design notes & limits

* **Workflow state lives in the workflow's process.** It survives master failover (that is the point of the worker split) but not the loss of its worker; durable resume-from-checkpoint would need event-sourcing activities and replay — the activity log already holds the data.
* **Fencing** — a deposed leader stops writing as soon as it learns it lost the lease, and workers ignore leaders with an older term; a split-brain window of up to one lease remains possible during network partitions.
* **Sandbox** — Function nodes and expressions run in `node:vm` with a CPU timeout inside the workflow's own process: isolation, not a security boundary. Workflow authors must be trusted.
* **Scale** — one process per instance ≈ 40–70 MB RSS; size `runtime.maxConcurrent` per worker to its RAM and add workers. For large volumes use the Mongo store; etcd is best kept for metadata.
* **etcd as a data store** keeps every `db.write` row as one key and scans a collection for upserts by field — fine for reference data and small tables; use MongoDB (`dataDb.mongoUrl`) for real volumes.
* **Tested here**: single node, master + separate worker, and the etcd HA cluster (shared config push/pull/watch, live changes, optimistic concurrency, shared `db.write` data across failover, election, forwarding, sync, tags, leader failover with adoption, rejoin, worker loss, drain, API auth switched on live), plus gRPC over TLS. The MongoDB variants (store, coordinator, shared config, data) and real RabbitMQ/MQTT brokers are implemented but were not available in the build environment.
# tether
