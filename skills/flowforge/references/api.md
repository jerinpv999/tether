# FlowForge REST API

Any master serves every endpoint; followers forward runtime calls to the leader. When `security.api.enabled` is set, send `Authorization: Basic …` (`/api/health` and, unless `protectHooks`, `/hooks/*` stay open).

## Workflows
| Call | Notes |
|---|---|
| `GET /api/workflows` (`?full=1`) | list (summary unless `full`) |
| `GET /api/workflows/:id` | definition |
| `POST /api/workflows` | create (400 with `errors[]` if invalid, 409 if the id exists) |
| `PUT /api/workflows/:id` | create or update; `version` increments; triggers re-armed on every master |
| `DELETE /api/workflows/:id` | running instances keep running (they hold a snapshot) |
| `POST /api/workflows/validate` | `{ valid, errors }` without saving |
| `POST /api/workflows/:id/start` | `{ instanceId?, input?, wait?: true\|"finish", timeoutMs? }`. Without wait → 202 + instance; `true` → the first Response node's reply; `"finish"` → the final instance |
| `ALL /hooks/<path>` | HTTP triggers; response header `x-workflow-instance-id` |

## Instances
| Call | Notes |
|---|---|
| `GET /api/instances?workflowId=&status=A,B&parentInstanceId=&limit=&offset=` | `{ total, items }` |
| `GET /api/instances/:id` (`?include=all`) | adds definition snapshot, activities, request |
| `GET /api/instances/:id/status` | `status, currentNodes, error, durationMs, activities{status:count}, pid` |
| `GET /api/instances/:id/activities[/:activityId]` | `activityId = <nodeId>#<n>`; `status, port, attempt, input, output, error, workerId` |
| `GET /api/instances/:id/request` | stored trigger data (auth headers redacted) |
| `GET /api/instances/:id/logs` | instance logs (`?level=warn,error&limit=`) |
| `GET /api/instances/:id/children` | sub-workflow instances |
| `GET /api/instances/:id/wait?timeoutMs=` | long-poll until final |
| `POST /api/instances/:id/stop` · `/terminate` · `/pause` · `/resume` | body `{ reason? }` |
| `POST /api/instances/:id/signal` | `{ name, payload }`; buffered if nobody waits yet |
| `POST /api/instances/:id/retry` | `{ instanceId? }`; new run with the same input |
| `DELETE /api/instances/:id` | finished instances only |

## Logs, data, events
| Call | Notes |
|---|---|
| `GET /api/logs?instanceId=&workflowId=&level=&since=&until=&limit=` | |
| `GET/PUT /api/settings/log-retention` | `{ retentionHours }`; in cluster mode written to the shared config |
| `POST /api/logs/purge` | `{ before? }` epoch ms |
| `GET /api/data`, `GET /api/data/:collection?limit=` | rows written by `db.write` |
| `POST /api/events/publish` | `{ source: "rabbitmq", exchange, routingKey, queue?, message }` or `{ source: "mqtt", topic, message }` |
| `GET /api/stream` (`?instanceId=`) | Server-Sent Events: `instance`, `activity` |

## Cluster
| Call | Notes |
|---|---|
| `GET /api/health` | `nodeId, leader (bool), leaderId, term, store, coordinator` (unauthenticated: LB probes) |
| `GET /api/cluster` | masters (leader flag), workers (`connected, capacity, assigned, draining, tags, rss`), `running, queued, orphaned` |
| `GET /api/cluster/config` | shared doc (secrets masked) + this node's effective config + `version` / `appliedVersion` |
| `PUT /api/cluster/config` | `{ doc, version }`; requires API auth; 409 when `version` is stale |
| `GET /api/node-types` | node/trigger schema |

Worker health: `GET http://<worker>:<http.port>/health`. Returns 503 while not attached to a leader; the body lists children, buffered messages and the leader term.
