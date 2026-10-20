# Operating FlowForge

## Files
| File | Purpose |
|---|---|
| `config/cluster.yaml` | shared cluster settings (`common`, `master`, `worker`), pushed to etcd/MongoDB |
| `config/master-N.yaml` | per master: `node.id`, `http`, `grpc` (port, `advertiseAddress`, `tls`), `shared:` |
| `config/worker-N.yaml` | per worker: `node.id`, `node.tags`, `grpc` (loopback port for children), `http` (health), optional local overrides such as `runtime.maxConcurrent`, `shared:` |
| `config/single-node.yaml` | standalone: everything local, file store, embedded worker |

Minimal node YAML:
```yaml
node: { id: worker-3, tags: [default, gpu] }
grpc: { host: 127.0.0.1, port: 50063 }
http: { port: 3103 }
shared: { source: etcd, etcd: { hosts: ["10.0.0.5:2379"], prefix: /flowforge/ } }
# or: shared: { source: mongo, mongo: { url: "mongodb://10.0.0.6:27017", db: flowforge } }
```
Node ids must be unique. Masters must set `grpc.advertiseAddress` / `http.advertiseUrl` to addresses that other nodes can reach.

## Admin CLI (`bin/flowforge.js`)
```bash
node bin/flowforge.js config validate config/cluster.yaml
node bin/flowforge.js config push config/cluster.yaml --config config/master-1.yaml   # or --etcd h:2379 [--prefix] | --mongo URL [--db]
node bin/flowforge.js config push config/cluster.yaml --config config/master-1.yaml --if-version 42   # refuse if changed
node bin/flowforge.js config pull --config config/master-1.yaml [--reveal] [--out cluster.yaml]
node bin/flowforge.js config effective --config config/worker-1.yaml [--role worker] [--reveal]
```
Push validates by building a master and a worker config from the document. Each node keeps its own `node`/`shared` keys, so those are rejected in the shared document, as are masked `***` values.

## Applied live vs restart
- **Masters, live**: `logs.*`, `security.grpc.*` (including `additionalUsers`), `security.api.*`, `cluster.workerLostGraceMs`, `cluster.onWorkerLost`, `cluster.syncIntervalMs`, `runtime.terminateGraceMs`, `runtime.defaultMaxOldSpaceMb`, `runtime.instanceRetentionHours`.
- **Workers, live**: `runtime.*` (capacity is reported to the leader at once), `logs.*`, `security.grpc.*` (used on the next connect), `discovery.static`.
- **Everything else** (store, brokers, ports, coordinator, TLS): the node logs `restart required` and keeps the old value.

## Startup order (cluster)
1. etcd or MongoDB is up.
2. `config push` (nodes started earlier wait up to `shared.waitMs`, 30 s).
3. Masters start. One logs `elected LEADER (term N)`, the others log `following leader …`.
4. Workers start: `attached to leader … (term N)`.
5. Check: `GET /api/cluster` shows one leader and every worker `connected`. Each worker's `/health` returns 200.

## Failure handling (what to expect, what to do)
| Situation | System behaviour | Operator action |
|---|---|---|
| Leader master crashes | A new leader within ≈ `leaseTtlMs`. Workers re-attach, running instances are **adopted** (no restart), buffered messages are flushed, broker subscriptions restored | none; restart the dead master later (it rejoins as follower) |
| Follower crashes | LB routes around it | restart |
| Worker crashes | after `workerLostGraceMs` its instances become FAILED (`onWorkerLost: fail`) or re-queued from scratch (`restart`) | `POST /api/instances/:id/retry` for the failed ones if needed |
| Worker deploy | send **SIGTERM**: the worker drains (no new work, waits up to `runtime.drainTimeoutMs`) | wait for exit, then start the new version |
| Instances stuck `QUEUED` | no worker attached, all full, or no worker has the workflow's `settings.workerTags` | check `/api/cluster`; add a worker or capacity, or fix the tags |
| `UNAUTHENTICATED` in logs | gRPC credentials differ between nodes | align `security.grpc` (shared config) and each node's env for `${FLOWFORGE_GRPC_PASSWORD}` |
| `no leader elected yet` (503) | coordinator unreachable or election in progress | check etcd/Mongo; it heals by itself once reachable |
| `shared config was changed by someone else` (409) | concurrent edit | pull, re-apply the edit, push again |

## Security checklist
- Set `FLOWFORGE_GRPC_PASSWORD` on every node and reference it as `${FLOWFORGE_GRPC_PASSWORD}` in `cluster.yaml`. Never leave `change-me`.
- Enable `grpc.tls` on masters and `masters.tls.caFile` on workers across untrusted networks; Basic auth sends the password with every call. Use hostnames in addresses (TLS SNI).
- Enable `security.api` (Basic auth for REST/UI) and put masters behind a TLS-terminating load balancer (`deploy/nginx.conf`).
- Restrict etcd/MongoDB access: they hold workflows, payloads and the shared config.
- Function nodes run in `node:vm`, which is not a security boundary, so only trusted users should author workflows.

## Sizing
One workflow process uses about 40–70 MB RSS. Set each worker's `runtime.maxConcurrent` ≈ (RAM − 1 GB) / 80 MB. Use MongoDB for the store and `dataDb` at high volume; keep etcd for coordination, shared config and small data.
