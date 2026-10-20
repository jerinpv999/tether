# FlowForge node reference

Source of truth: `src/shared/nodeTypes.js` (also served at `GET /api/node-types`). Defaults shown after `=`. Fields marked *(if …)* only apply in that mode.

## Flow

| Type | Ports | Params |
|---|---|---|
| `start` | out | `initVars={}`: seeds `vars` (templates allowed) |
| `end` | – | `status="COMPLETED"` (or `FAILED`), `output="{{ vars }}"`, `error` *(if FAILED)*. Ends the instance and cancels other branches |
| `response` | out | `statusCode=200`, `body`, `headers={}`. Replies to the sync HTTP caller; the workflow continues |
| `signal` | out, timeout | `mode="wait"\|"listen"`, `signalName="approve"`, `timeoutMs=0` (0 = forever, wait mode), `outputVar`. Output = signal payload. In listen mode the node has no incoming edge and each arrival starts its branch |
| `subworkflow` | out, error | `workflowId`, `input="{{ vars }}"`, `wait=true`, `timeoutMs=0`, `outputVar`. Child instance id = `<parentId>.<nodeId>.<n>`; terminated if the parent stops or fails |

## Logic

| Type | Ports | Params |
|---|---|---|
| `function` | out, error | `code` (JS body; `return` value; may mutate `vars`; `log()`, `fetch`, `sleep`), `timeoutMs=1000` (sync CPU limit), `outputVar` |
| `decision` | true, false | `condition` (JS expression) |
| `switch` | one per case + `default` | `expression`, `cases=["a","b"]`. Matched with `String(value) === case` |
| `join` | out | `count=0` (0 = number of incoming edges). Continues once when that many tokens have arrived |

## Time

| Type | Ports | Params |
|---|---|---|
| `delay` | out | `duration="5s"`. Accepts `500`, `30s`, `5m`, `1h30m`, `1d`, or a template |
| `timer` | out | `mode="at"`: `at` = ISO date or epoch-ms template. `mode="interval"`: `intervalMs=60000`, `repeat=1`. Ticks 1..n-1 spawn the out branch; the last tick continues normally |

## Activity (`type: "activity"`, choose `kind`)

Common to all kinds: `retries=0`, `retryDelayMs=1000`, `timeoutMs=30000`, `outputVar`. Ports: `out`, `error` (`event.receive` also has `timeout`).

| kind | Params | Output |
|---|---|---|
| `http` | `method="GET"`, `url`, `headers={}`, `body`, `failOnHttpError=true` | `{ status, headers, body }` (JSON parsed when content-type is JSON) |
| `rabbitmq.publish` | `exchange`, `routingKey`, `queue` (direct), `message="{{ vars }}"`, `persistent=true` | `{ published }` |
| `mqtt.publish` | `topic`, `message`, `qos=1`, `retain=false` | `{ published }` |
| `event.receive` | `source="rabbitmq"\|"mqtt"`; rabbit: `rxExchange`, `rxRoutingKey` (`*`/`#`), `rxQueue`; mqtt: `rxTopic` (`+`/`#`); `filter` expression with `event` in scope; `timeoutMs` → `timeout` port | the event payload |
| `db.write` | `collection`, `operation="insert"\|"upsert"\|"update"\|"delete"`, `keyField="id"`, `document="{{ vars }}"` | `{ insertedId }` / `{ matched, upserted }` / `{ matched, modified }` / `{ deleted }` |
| `transform` | `mapping` (JSON template) | the rendered mapping |

`db.write` targets the shared data backend in cluster mode: MongoDB (`dataDb.mongoUrl` or the store's server) or etcd keys `<prefix>data/<collection>/<_id>`. Read the rows with `GET /api/data/:collection`.

## Triggers (`triggers: [...]` on the workflow)

| type | Fields |
|---|---|
| `http` | `method` (`POST`, `GET`, …, `ANY`), `path` (`/orders/:id`, `*`), `sync=true`, `responseTimeoutMs=30000`, `instanceIdFrom` |
| `rabbitmq` | `queue` (default `flowforge.<workflowId>`), `exchange`, `routingKey="#"`, `instanceIdFrom` |
| `mqtt` | `topic`, `qos=1`, `instanceIdFrom` |
| `schedule` | `intervalMs`, `input` |

`instanceIdFrom`: `body:<dot.path>` · `header:<name>` · `query:<name>` · `param:<name>`. Every trigger can also be `enabled: false`. Broker and schedule triggers run on the leader only. HTTP hooks (`/hooks/<path>`) work on every master.

## Workflow `settings`

`maxSteps=10000` (loop guard) · `workerTags=[]` (run only on workers having all tags) · `maxOldSpaceMb` (heap per process).
