---
name: flowforge
description: Build, run and operate FlowForge, the Node.js workflow orchestrator (master + worker apps, gRPC, etcd/Mongo cluster). Use when writing or fixing FlowForge workflow JSON, wiring triggers, configuring masters/workers or the shared cluster config, or debugging instances.
---

# FlowForge

FlowForge is a Node.js workflow orchestrator. Two apps share one codebase:

- **master** (`node bin/master.js --config <yaml>`): REST API, Fabric.js designer, triggers (HTTP / RabbitMQ / MQTT / schedule), store, scheduling. Run 2+ for HA: one is elected **leader**, the rest are **followers** that forward runtime calls to it.
- **worker** (`node bin/worker.js --config <yaml>`): only hosts workflow processes, one forked Node process per running instance, alive until the workflow ends. Add workers to scale out.

Masters ⇄ workers ⇄ child processes talk over gRPC (`proto/orchestrator.proto`) with **Basic auth** in the `authorization` metadata. In cluster mode everything shared lives in **etcd or MongoDB**: the store, the `db.write` data, leader election/discovery and one **shared config document**. A node's own YAML holds only its id, ports, advertised address and a `shared:` block.

Before acting, decide which job this is and read the matching section:

| Job | Section |
|---|---|
| Write or fix a workflow definition | [Authoring workflows](#authoring-workflows) + `references/nodes.md` |
| Start / control / inspect instances, read logs | [Running and debugging](#running-and-debugging) + `references/api.md` |
| Configure nodes, cluster, shared config, credentials | [Operating the cluster](#operating-the-cluster) + `references/operations.md` |
| Change FlowForge's own code | [Changing the codebase](#changing-the-codebase) |

## Authoring workflows

A workflow is JSON:

```json
{
  "id": "order-approval",              // letters, digits, _ . -
  "name": "Order approval",
  "enabled": true,
  "triggers": [ { "type": "http", "method": "POST", "path": "/orders", "sync": true, "instanceIdFrom": "body:orderId" } ],
  "settings": { "maxSteps": 1000, "workerTags": [], "maxOldSpaceMb": 256 },
  "nodes": [ { "id": "start", "type": "start", "name": "Start", "x": 40, "y": 140, "config": {} } ],
  "edges": [ { "from": "start", "to": "next", "port": "out" } ]
}
```

Rules the validator enforces (`POST /api/workflows/validate`, or `validateWorkflow` in `src/shared/validate.js`):

- Exactly **one `start`** node. Nothing may connect *into* `start`.
- Every edge's `from`/`to` must exist, and `port` must be a real output port of the `from` node (default `out`).
- `switch` needs ≥1 case. Its ports are the case values plus `default`. `signal` needs `signalName`. `subworkflow` needs `workflowId` and must not start itself.
- Triggers: `http` needs `path`; `mqtt` needs `topic`; `rabbitmq` needs `queue` or `exchange`; `schedule` needs `intervalMs ≥ 1000`.

Node types and their parameters are listed in `references/nodes.md`. Read it before writing nodes, and never invent parameter names. Key behaviours:

- **Ports decide flow.** A node with an unconnected `error` port *fails the instance* when it errors. Connect `error` to handle failures. `event.receive`, `signal` (wait mode) and others also have a `timeout` port.
- **Branches run concurrently.** One output port may feed several nodes. Use `join` (count 0 = all incoming edges) to merge them.
- **`end` finishes the instance** and cancels other branches. Without an `end`, the instance completes when all branches drain, and output = `vars`.
- **`response`** replies to a waiting synchronous HTTP caller and the workflow *continues*. That is how you return 202 early and keep working.
- **Interrupts**: a `signal` node with `mode: listen` and no incoming edge is armed for the whole run. Each arrival of that signal starts its branch (e.g. cancel → compensation → end FAILED).

**Templates.** Any string or JSON param may contain `{{ expression }}`. A value that is *exactly one* placeholder keeps its type (`"{{ input.items }}"` → array); otherwise the placeholders are interpolated as text. Scope:

| Name | Meaning |
|---|---|
| `input` | trigger payload (HTTP body merged with path params; broker message) |
| `vars` | mutable workflow variables (`start.initVars`, Function code, `outputVar`) |
| `nodes.<nodeId>` | output of an executed node (`{ error }` after an error-port hop) |
| `signals.<name>` | last payload of each signal received |
| `workflow` | `instanceId`, `workflowId`, `name`, `parentInstanceId`, `startedAt` |
| `trigger` | trigger metadata (`type`, `headers`, `query`, `params`, `topic`, `routingKey` …) |
| `env.WF_*` | environment variables starting with `WF_` (API keys) |
| `now()`, `uuid()` | helpers |

Function nodes run JavaScript in `node:vm` (async allowed). They can `return` a value, mutate `vars`, and call `log(...)`, `await fetch(...)` and `await sleep(ms)`. Keep code deterministic and short; heavy work belongs in an `http` activity.

Authoring checklist:
1. Sketch the happy path, then add `error` / `timeout` edges for every activity that talks to the outside world.
2. Use `outputVar` to keep results in `vars` instead of digging through `nodes.<id>`.
3. For HTTP-triggered flows, decide sync vs async. With `sync: true` the caller gets the first `response` node's reply, or the instance result when the run ends (or 202 after `responseTimeoutMs`).
4. Give user-meaningful ids with `instanceIdFrom` (e.g. `body:orderId`). Duplicates return 409, so retries become idempotent.
5. Give nodes `x`/`y` on a 20 px grid, about 240 px apart horizontally, so the designer renders them cleanly.
6. Validate, save (`POST /api/workflows` creates, `PUT /api/workflows/:id` updates and bumps `version`), then run once with `wait: "finish"`.

`examples/order-approval.json`, `examples/sensor-alert.json` and `examples/payment-saga.json` are good starting points. They cover signals, switch/delay/MQTT, sub-workflows, event receive and listen-mode interrupts.

## Running and debugging

The full endpoint list is in `references/api.md`. The common loop:

```bash
# start and wait for the result
curl -XPOST $M/api/workflows/order-approval/start -H 'content-type: application/json' \
     -d '{"instanceId":"ord-1","input":{"orderId":"ord-1","items":[{"price":10}]},"wait":"finish"}'
curl $M/api/instances/ord-1/status          # status, currentNodes, activity counts, pid
curl $M/api/instances/ord-1/activities      # per node: status, port taken, input/output, error, attempts
curl $M/api/instances/ord-1/logs
curl -XPOST $M/api/instances/ord-1/signal -d '{"name":"approve","payload":{"by":"alice"}}' -H 'content-type: application/json'
```

Debugging order:
1. Run `/status`. `QUEUED` with no progress means no worker is attached, or none matches `settings.workerTags` (check `/api/cluster`). `WAITING` means it is blocked on a delay/timer/signal/event; `currentNodes` says where.
2. Run `/activities` and find the first `FAILED` one and its `error`. A template error names the expression, and for a JS error the node's `input` shows what it saw.
3. Run `/logs` for Function `log()` output and control events.
4. Once fixed, use `POST /api/instances/:id/retry` (same input, new instance, `retryOf` link).

Statuses: `QUEUED → PENDING → RUNNING ⇄ WAITING ⇄ PAUSED → COMPLETED | FAILED | STOPPED | TERMINATED`.
Control actions: `stop` cancels gracefully, `terminate` kills (SIGKILL after the grace period), `pause`/`resume` hold nodes from starting, and `signal` delivers a named signal.

## Operating the cluster

Details, file templates and failure handling are in `references/operations.md`. Essentials:

- **Single box**: `npm start` (`config/single-node.yaml`: file store plus an embedded worker).
- **Cluster**: etcd or MongoDB is required. Order of operations:
  1. `node bin/flowforge.js config push config/cluster.yaml --config config/master-1.yaml` stores the shared settings.
  2. Start the masters (`config/master-*.yaml`), then the workers (`config/worker-*.yaml`).
  3. `curl $M/api/cluster` should show one leader and all workers `connected`.
- **Precedence**: defaults ← shared `common` ← shared `master`/`worker` ← node YAML. `${VAR}` / `${VAR:-default}` are resolved on each node, so keep secrets as env references, never literal values in etcd.
- **Change cluster settings** by editing `config/cluster.yaml` and running `config push` (validated, versioned). Running nodes apply live keys at once and log "restart required" for the rest. Never push a pulled file that shows `***`; pull with `--reveal` or use env references.
- **Credentials rotation**: add the new pair to `common.security.grpc.additionalUsers` and push. Then switch `username`/`password` to the new pair and push again. Then remove the old pair.
- **Never** point a cluster at `store.type: file|memory` or `dataDb.type: file`; config validation refuses it.

## Changing the codebase

Map: `src/master/` (api, runtime = leader scheduler, cluster = gRPC services and follower proxy, triggers, gateway = brokers and data), `src/worker/worker.js`, `src/child/` (engine = graph executor, expr = templates/vm), `src/cluster/` (coordinator, sharedConfig), `src/shared/nodeTypes.js` (node schema = designer forms), `public/` (designer).

- Adding a node type means updating three places: schema in `src/shared/nodeTypes.js`, execution in `Engine.runNode` (`src/child/engine.js`), and validation rules in `src/shared/validate.js` if needed. The designer picks it up from the schema automatically.
- Adding a child → master operation means adding a `case` in `Runtime._onRequest`. Ops that must not be retried after failover go into the worker's `FAIL_ON_FAILOVER` set.
- Always run both suites:
  - `npm test`: 18 e2e tests (master plus a separate worker).
  - `ETCD_BIN=/path/to/etcd npm run test:cluster`: 14 HA tests, including leader kill -9, worker loss, drain and shared-config changes.
