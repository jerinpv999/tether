'use strict';

/**
 * Node-type catalogue. Served to the designer at GET /api/node-types so every
 * property panel is generated from this schema (parameters are fully dynamic),
 * and used by the engine for defaults + port names.
 *
 * Field types: string | number | boolean | select | json | code | expression | list | duration
 *  - `showIf: { field: value | [values] }` hides a field unless the condition holds
 *  - any string/json value may contain {{ expression }} templates evaluated at run time
 *    against: input, vars, nodes.<nodeId>, signals, workflow, item (switch), now(), uuid()
 */

const common = {
  retries: { type: 'number', label: 'Retries', default: 0, min: 0 },
  retryDelayMs: { type: 'number', label: 'Retry delay (ms)', default: 1000, min: 0 },
  timeoutMs: { type: 'number', label: 'Timeout (ms)', default: 30000, min: 0 },
  outputVar: { type: 'string', label: 'Save result to vars.', placeholder: 'e.g. customer' },
};

const nodeTypes = {
  start: {
    label: 'Start', color: '#2e7d32', icon: '▶', category: 'Flow',
    description: 'Entry point. The trigger payload is available as `input`.',
    inputs: 0, ports: ['out'],
    params: {
      initVars: { type: 'json', label: 'Initial vars', default: {} },
    },
  },
  end: {
    label: 'End', color: '#455a64', icon: '■', category: 'Flow',
    description: 'Finishes the workflow and sets its output. Other running branches are cancelled.',
    ports: [],
    params: {
      status: { type: 'select', label: 'Final status', options: ['COMPLETED', 'FAILED'], default: 'COMPLETED' },
      output: { type: 'json', label: 'Output', default: '{{ vars }}' },
      error: { type: 'string', label: 'Error message', showIf: { status: 'FAILED' } },
    },
  },
  activity: {
    label: 'Activity', color: '#1565c0', icon: '⚙', category: 'Activities',
    description: 'Calls an API, publishes / receives RabbitMQ or MQTT events, writes to a database or transforms data.',
    ports: ['out', 'error'], dynamicPorts: 'activity',
    params: {
      kind: {
        type: 'select', label: 'Activity', default: 'http',
        options: ['http', 'rabbitmq.publish', 'mqtt.publish', 'event.receive', 'db.write', 'transform'],
      },
      // http
      method: { type: 'select', label: 'Method', options: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'], default: 'GET', showIf: { kind: 'http' } },
      url: { type: 'string', label: 'URL', placeholder: 'https://api.example.com/items/{{ input.id }}', showIf: { kind: 'http' } },
      headers: { type: 'json', label: 'Headers', default: {}, showIf: { kind: 'http' } },
      body: { type: 'json', label: 'Body', showIf: { kind: 'http' } },
      failOnHttpError: { type: 'boolean', label: 'Fail on 4xx/5xx', default: true, showIf: { kind: 'http' } },
      // rabbit publish
      exchange: { type: 'string', label: 'Exchange', placeholder: 'orders (blank = default)', showIf: { kind: ['rabbitmq.publish'] } },
      routingKey: { type: 'string', label: 'Routing key', showIf: { kind: ['rabbitmq.publish'] } },
      queue: { type: 'string', label: 'Queue (direct)', showIf: { kind: ['rabbitmq.publish'] } },
      persistent: { type: 'boolean', label: 'Persistent', default: true, showIf: { kind: 'rabbitmq.publish' } },
      // mqtt publish
      topic: { type: 'string', label: 'Topic', placeholder: 'devices/{{ input.deviceId }}/cmd', showIf: { kind: 'mqtt.publish' } },
      qos: { type: 'select', label: 'QoS', options: [0, 1, 2], default: 1, showIf: { kind: 'mqtt.publish' } },
      retain: { type: 'boolean', label: 'Retain', default: false, showIf: { kind: 'mqtt.publish' } },
      message: { type: 'json', label: 'Message', default: '{{ vars }}', showIf: { kind: ['rabbitmq.publish', 'mqtt.publish'] } },
      // receive
      source: { type: 'select', label: 'Source', options: ['rabbitmq', 'mqtt'], default: 'rabbitmq', showIf: { kind: 'event.receive' } },
      rxExchange: { type: 'string', label: 'Exchange', showIf: { kind: 'event.receive', source: 'rabbitmq' } },
      rxRoutingKey: { type: 'string', label: 'Routing key pattern', placeholder: 'order.*.paid', showIf: { kind: 'event.receive', source: 'rabbitmq' } },
      rxQueue: { type: 'string', label: 'Queue', showIf: { kind: 'event.receive', source: 'rabbitmq' } },
      rxTopic: { type: 'string', label: 'Topic filter', placeholder: 'sensors/+/reading', showIf: { kind: 'event.receive', source: 'mqtt' } },
      filter: { type: 'expression', label: 'Accept when', placeholder: 'event.orderId === input.orderId', showIf: { kind: 'event.receive' } },
      // db
      collection: { type: 'string', label: 'Collection / table', showIf: { kind: 'db.write' } },
      operation: { type: 'select', label: 'Operation', options: ['insert', 'upsert', 'update', 'delete'], default: 'insert', showIf: { kind: 'db.write' } },
      keyField: { type: 'string', label: 'Key field', default: 'id', showIf: { kind: 'db.write', operation: ['upsert', 'update', 'delete'] } },
      document: { type: 'json', label: 'Document', default: '{{ vars }}', showIf: { kind: 'db.write' } },
      // transform
      mapping: { type: 'json', label: 'Output template', default: { id: '{{ input.id }}' }, showIf: { kind: 'transform' } },
      ...common,
    },
  },
  function: {
    label: 'Function', color: '#6a1b9a', icon: 'ƒ', category: 'Logic',
    description: 'Runs JavaScript in a sandbox. Has input, vars, nodes, signals, log(). `return` a value; mutate vars to set variables.',
    ports: ['out', 'error'],
    params: {
      code: { type: 'code', label: 'Code', default: '// vars.total = input.items.reduce((s, i) => s + i.price, 0);\nreturn { ok: true };' },
      timeoutMs: { type: 'number', label: 'CPU timeout (ms)', default: 1000 },
      outputVar: common.outputVar,
    },
  },
  decision: {
    label: 'Decision', color: '#ef6c00', icon: '◆', category: 'Logic',
    description: 'If / else on a boolean expression.',
    ports: ['true', 'false'],
    params: {
      condition: { type: 'expression', label: 'Condition', default: 'input.amount > 100' },
    },
  },
  switch: {
    label: 'Switch', color: '#d84315', icon: '⑂', category: 'Logic',
    description: 'Routes to the port whose case equals the expression value; otherwise `default`.',
    ports: ['default'], dynamicPorts: 'cases',
    params: {
      expression: { type: 'expression', label: 'Value', default: 'input.type' },
      cases: { type: 'list', label: 'Cases (one port each)', default: ['a', 'b'] },
    },
  },
  join: {
    label: 'Join', color: '#00838f', icon: '⧓', category: 'Logic',
    description: 'Waits until all (or N) incoming branches arrive, then continues once.',
    ports: ['out'],
    params: {
      count: { type: 'number', label: 'Wait for N branches (0 = all incoming)', default: 0 },
    },
  },
  response: {
    label: 'Response', color: '#2e7d32', icon: '↩', category: 'Flow',
    description: 'Replies to the synchronous HTTP caller (if any) and continues.',
    ports: ['out'],
    params: {
      statusCode: { type: 'number', label: 'HTTP status', default: 200 },
      body: { type: 'json', label: 'Body', default: { ok: true, instanceId: '{{ workflow.instanceId }}' } },
      headers: { type: 'json', label: 'Headers', default: {} },
    },
  },
  timer: {
    label: 'Timer', color: '#5d4037', icon: '⏰', category: 'Time',
    description: 'Waits until a point in time (ISO date / epoch ms expression) or for a cron-like interval tick.',
    ports: ['out'],
    params: {
      mode: { type: 'select', label: 'Mode', options: ['at', 'interval'], default: 'at' },
      at: { type: 'string', label: 'Fire at', placeholder: '{{ input.dueAt }} or 2026-12-31T09:00:00+05:30', showIf: { mode: 'at' } },
      intervalMs: { type: 'number', label: 'Interval (ms)', default: 60000, showIf: { mode: 'interval' } },
      repeat: { type: 'number', label: 'Ticks (each runs the out branch)', default: 1, showIf: { mode: 'interval' } },
    },
  },
  delay: {
    label: 'Delay', color: '#795548', icon: '⏳', category: 'Time',
    description: 'Pauses this branch for a duration ("500", "30s", "5m", "2h", or an expression).',
    ports: ['out'],
    params: {
      duration: { type: 'duration', label: 'Duration', default: '5s' },
    },
  },
  signal: {
    label: 'Signal', color: '#c2185b', icon: '⚡', category: 'Flow',
    description: 'wait: block until POST /api/instances/:id/signal delivers the named signal. listen: armed for the whole run — every arrival interrupts by starting this branch.',
    inputs: 1, ports: ['out', 'timeout'],
    params: {
      mode: { type: 'select', label: 'Mode', options: ['wait', 'listen'], default: 'wait' },
      signalName: { type: 'string', label: 'Signal name', default: 'approve' },
      timeoutMs: { type: 'number', label: 'Timeout (ms, 0 = none)', default: 0, showIf: { mode: 'wait' } },
      outputVar: common.outputVar,
    },
  },
  subworkflow: {
    label: 'Sub-workflow', color: '#283593', icon: '⧉', category: 'Flow',
    description: 'Starts another workflow as a child instance (its own process), optionally waiting for its result.',
    ports: ['out', 'error'],
    params: {
      workflowId: { type: 'workflow', label: 'Workflow' },
      input: { type: 'json', label: 'Input', default: '{{ vars }}' },
      wait: { type: 'boolean', label: 'Wait for completion', default: true },
      timeoutMs: { type: 'number', label: 'Timeout (ms, 0 = none)', default: 0 },
      outputVar: common.outputVar,
    },
  },
};

const triggerTypes = {
  http: {
    label: 'HTTP webhook',
    params: {
      method: { type: 'select', options: ['POST', 'GET', 'PUT', 'PATCH', 'DELETE', 'ANY'], default: 'POST' },
      path: { type: 'string', placeholder: '/orders/:orderId' },
      sync: { type: 'boolean', label: 'Wait for Response/End node', default: true },
      responseTimeoutMs: { type: 'number', default: 30000 },
      instanceIdFrom: { type: 'string', label: 'Instance id from (header:x / body:x / query:x)', placeholder: 'body:orderId' },
    },
  },
  rabbitmq: {
    label: 'RabbitMQ consumer',
    params: {
      queue: { type: 'string', placeholder: 'orders.created' },
      exchange: { type: 'string' },
      routingKey: { type: 'string', placeholder: 'order.created' },
      instanceIdFrom: { type: 'string', placeholder: 'body:orderId' },
    },
  },
  mqtt: {
    label: 'MQTT subscription',
    params: {
      topic: { type: 'string', placeholder: 'devices/+/alarm' },
      qos: { type: 'select', options: [0, 1, 2], default: 1 },
      instanceIdFrom: { type: 'string', placeholder: 'body:eventId' },
    },
  },
  schedule: {
    label: 'Schedule',
    params: {
      intervalMs: { type: 'number', default: 3600000 },
      input: { type: 'json', default: {} },
    },
  },
};

function defaultsFor(type) {
  const t = nodeTypes[type];
  const out = {};
  if (!t) return out;
  for (const [k, f] of Object.entries(t.params)) if (f.default !== undefined) out[k] = structuredClone(f.default);
  return out;
}

function portsFor(node) {
  const t = nodeTypes[node.type];
  if (!t) return ['out'];
  if (t.dynamicPorts === 'cases') return [...(node.config?.cases || []).map(String), 'default'];
  if (t.dynamicPorts === 'activity' && node.config?.kind === 'event.receive') return ['out', 'timeout', 'error'];
  return t.ports;
}

module.exports = { nodeTypes, triggerTypes, defaultsFor, portsFor };
