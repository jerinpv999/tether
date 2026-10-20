'use strict';
/*
 * End-to-end test: boots the orchestrator (memory store, emulated brokers),
 * exercises every node type and control operation through the public API.
 *   node test/e2e.js
 */
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const assert = require('assert/strict');

const PORT = 3977, GRPC = 50977, WGRPC = 50978, WHTTP = 3978;
const BASE = `http://127.0.0.1:${PORT}`;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flowforge-test-'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, url, body) {
  const r = await fetch(BASE + url, { method, headers: { 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await r.text();
  let json; try { json = JSON.parse(text); } catch { json = text; }
  return { status: r.status, body: json, headers: r.headers };
}
async function until(fn, ms = 8000, label = 'condition') {
  const end = Date.now() + ms;
  while (Date.now() < end) { const v = await fn(); if (v) return v; await sleep(100); }
  throw new Error(`timeout waiting for ${label}`);
}
const statusOf = async (id) => (await api('GET', `/api/instances/${id}/status`)).body.status;
const waitStatus = (id, st, ms) => until(async () => { const s = await statusOf(id); return (Array.isArray(st) ? st.includes(s) : s === st) ? s : null; }, ms, `${id} → ${st}`);

const results = [];
async function test(name, fn) {
  const t = Date.now();
  try { await fn(); results.push([name, true]); console.log(`  ✔ ${name} (${Date.now() - t} ms)`); }
  catch (e) { results.push([name, false]); console.log(`  ✘ ${name}\n    ${e.stack}`); }
}

const wf = (id, nodes, edges, extra = {}) => ({ id, name: id, nodes, edges, ...extra });
const n = (id, type, config = {}, name) => ({ id, type, name: name || id, x: 0, y: 0, config });

const bin = (n) => path.join(__dirname, '..', 'bin', n);
const PASS = 's3cret-' + Date.now();

(async () => {
  // master + a separate worker application, each with its own YAML file
  fs.writeFileSync(path.join(tmp, 'master.yaml'), [
    'node: { id: test-master }',
    `http: { port: ${PORT} }`,
    `grpc: { host: 127.0.0.1, port: ${GRPC} }`,
    'security: { grpc: { username: ff, password: "${TEST_GRPC_PASS}" } }',
    'store: { type: memory }',
    `dataDb: { fileDir: "${path.join(tmp, 'db')}" }`,
    'logs: { level: debug, console: false }',
    'runtime: { terminateGraceMs: 1500 }',
  ].join('\n'));
  fs.writeFileSync(path.join(tmp, 'worker.yaml'), [
    'node: { id: test-worker, tags: [default] }',
    `grpc: { port: ${WGRPC} }`,
    `http: { port: ${WHTTP} }`,
    'security: { grpc: { username: ff, password: "${TEST_GRPC_PASS}" } }',
    `discovery: { type: static, static: { masters: ["127.0.0.1:${GRPC}"] } }`,
    'runtime: { maxConcurrent: 20 }',
    'logs: { level: warn }',
  ].join('\n'));
  const env = { ...process.env, TEST_GRPC_PASS: PASS };
  const srv = spawn(process.execPath, [bin('master.js'), '--config', path.join(tmp, 'master.yaml')], { env, stdio: ['ignore', 'inherit', 'inherit'] });
  const wrk = spawn(process.execPath, [bin('worker.js'), '--config', path.join(tmp, 'worker.yaml')], { env, stdio: ['ignore', 'inherit', 'inherit'] });
  try {
    await until(async () => (await fetch(BASE + '/api/health').catch(() => null))?.ok, 10000, 'server');
    await until(async () => (await api('GET', '/api/cluster')).body.workers?.some((w) => w.connected), 10000, 'worker attach');
    console.log('master + worker up\n');

    await test('worker app attached with Basic auth; health endpoint', async () => {
      const c = (await api('GET', '/api/cluster')).body;
      assert.equal(c.self.isLeader, true);
      const w = c.workers.find((x) => x.id === 'test-worker');
      assert.equal(w.capacity, 20); assert.deepEqual(w.tags, ['default']);
      const h = await (await fetch(`http://127.0.0.1:${WHTTP}/health`)).json();
      assert.equal(h.connected, true); assert.equal(h.leader, 'test-master');
    });

    await test('worker with wrong gRPC password is rejected', async () => {
      const bad = path.join(tmp, 'bad.yaml');
      fs.writeFileSync(bad, ['node: { id: intruder }', 'grpc: { port: 0 }', 'http: { port: 0 }', 'security: { grpc: { username: ff, password: wrong } }',
        `discovery: { type: static, static: { masters: ["127.0.0.1:${GRPC}"] } }`, 'logs: { level: error }'].join('\n'));
      const p = spawn(process.execPath, [bin('worker.js'), '--config', bad], { env, stdio: 'ignore' });
      await sleep(2000);
      const c = (await api('GET', '/api/cluster')).body;
      p.kill('SIGKILL');
      assert.ok(!c.workers.some((w) => w.id === 'intruder'));
    });

    await test('node-type catalogue served for the designer', async () => {
      const r = await api('GET', '/api/node-types');
      for (const t of ['start', 'end', 'activity', 'function', 'decision', 'switch', 'response', 'timer', 'delay', 'signal', 'subworkflow', 'join']) assert.ok(r.body.nodeTypes[t], t);
    });

    await test('validation rejects bad workflows', async () => {
      const r = await api('POST', '/api/workflows', wf('bad', [n('a', 'delay')], [{ from: 'a', to: 'zz' }]));
      assert.equal(r.status, 400);
      assert.ok(r.body.errors.some((e) => e.includes('Start')));
    });

    await test('HTTP trigger, sync response, function + decision (small order)', async () => {
      const r = await api('POST', '/hooks/orders', { orderId: 'T-1', items: [{ price: 5, qty: 2 }] });
      assert.equal(r.status, 200);
      assert.deepEqual(r.body, { orderId: 'T-1', status: 'APPROVED', total: 10 });
      assert.equal(r.headers.get('x-workflow-instance-id'), 'T-1');
      await waitStatus('T-1', 'COMPLETED');
      const inst = (await api('GET', '/api/instances/T-1')).body;
      assert.equal(inst.output.approvedBy, 'auto');
      const db = JSON.parse(fs.readFileSync(path.join(tmp, 'db', 'orders.json'), 'utf8'));
      assert.ok(db.find((o) => o.orderId === 'T-1' && o.total === 10));
    });

    await test('duplicate user-supplied instance id → 409', async () => {
      const r = await api('POST', '/hooks/orders', { orderId: 'T-1', items: [] });
      assert.equal(r.status, 409);
    });

    await test('incoming request data stored (sensitive headers redacted)', async () => {
      const r = await fetch(BASE + '/hooks/orders', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer secret' }, body: JSON.stringify({ orderId: 'T-req', items: [] }) });
      await r.text();
      const req = (await api('GET', '/api/instances/T-req/request')).body;
      assert.equal(req.body.orderId, 'T-req');
      assert.equal(req.trigger.headers.authorization, '***');
    });

    await test('large order waits for signal, then completes', async () => {
      const r = await api('POST', '/hooks/orders', { orderId: 'T-2', items: [{ price: 2000 }] });
      assert.equal(r.status, 202);
      assert.equal(r.body.status, 'PENDING_APPROVAL');
      await waitStatus('T-2', 'WAITING');
      const st = (await api('GET', '/api/instances/T-2/status')).body;
      assert.deepEqual(st.currentNodes, ['approval']);
      assert.ok(st.pid > 0);
      await api('POST', '/api/instances/T-2/signal', { name: 'approve', payload: { by: 'alice' } });
      await waitStatus('T-2', 'COMPLETED');
      const inst = (await api('GET', '/api/instances/T-2')).body;
      assert.equal(inst.output.approvedBy, 'alice');
    });

    await test('terminate a waiting workflow kills its process', async () => {
      await api('POST', '/hooks/orders', { orderId: 'T-3', items: [{ price: 5000 }] });
      await waitStatus('T-3', 'WAITING');
      const pid = (await api('GET', '/api/instances/T-3/status')).body.pid;
      await api('POST', '/api/instances/T-3/terminate', { reason: 'test' });
      await waitStatus('T-3', 'TERMINATED');
      await until(() => { try { process.kill(pid, 0); return false; } catch { return true; } }, 5000, 'process exit');
      const acts = (await api('GET', '/api/instances/T-3/activities')).body;
      assert.equal(acts.find((a) => a.nodeId === 'approval').status, 'CANCELLED');
    });

    await test('stop, pause/resume around a delay', async () => {
      await api('POST', '/api/workflows', wf('delays', [n('s', 'start'), n('d', 'delay', { duration: '1500ms' }), n('f', 'function', { code: 'return 42', outputVar: 'answer' }), n('e', 'end')],
        [{ from: 's', to: 'd' }, { from: 'd', to: 'f' }, { from: 'f', to: 'e' }]));
      // stop
      let r = await api('POST', '/api/workflows/delays/start', { instanceId: 'D-stop' });
      assert.equal(r.status, 202);
      await waitStatus('D-stop', 'WAITING');
      await api('POST', '/api/instances/D-stop/stop');
      await waitStatus('D-stop', 'STOPPED');
      // pause before the function runs, resume afterwards
      await api('POST', '/api/workflows/delays/start', { instanceId: 'D-pause' });
      await waitStatus('D-pause', 'WAITING');
      await api('POST', '/api/instances/D-pause/pause');
      await waitStatus('D-pause', 'PAUSED');
      await sleep(1800); // delay elapses while paused → function must not have run
      const acts = (await api('GET', '/api/instances/D-pause/activities')).body;
      assert.ok(!acts.find((a) => a.nodeId === 'f'), 'function ran while paused');
      await api('POST', '/api/instances/D-pause/resume');
      await waitStatus('D-pause', 'COMPLETED');
      assert.equal((await api('GET', '/api/instances/D-pause')).body.output.answer, 42);
    });

    await test('auto-generated instance id + wait=finish', async () => {
      const r = await api('POST', '/api/workflows/delays/start', { wait: 'finish', input: {} });
      assert.match(r.body.instanceId, /^[0-9a-f-]{36}$/);
      assert.equal(r.body.status, 'COMPLETED');
    });

    await test('switch + MQTT trigger (emulated bus) + mqtt publish', async () => {
      await api('POST', '/api/events/publish', { source: 'mqtt', topic: 'sensors/s9/reading', message: { value: 99 } });
      const list = await until(async () => { const l = (await api('GET', '/api/instances?workflowId=sensor-alert&status=COMPLETED')).body; return l.total ? l : null; }, 8000, 'sensor run');
      const inst = list.items[0];
      assert.equal(inst.output.level, 'critical');
      assert.equal(inst.output.sensor, 's9');
      const acts = (await api('GET', `/api/instances/${inst.instanceId}/activities`)).body;
      assert.equal(acts.find((a) => a.nodeId === 'route').port, 'critical');
      assert.ok(acts.find((a) => a.nodeId === 'alert' && a.status === 'COMPLETED'));
    });

    await test('sub-workflow + RabbitMQ trigger + event.receive with filter', async () => {
      await api('POST', '/api/events/publish', { source: 'rabbitmq', exchange: 'checkout', routingKey: 'checkout.started', message: { sagaId: 'SAGA-1', orderId: 'O-77', items: [{ price: 20 }] } });
      await until(async () => (await api('GET', '/api/instances/SAGA-1/status')).body.currentNodes?.includes('pay'), 8000, 'saga at payment');
      const child = (await api('GET', '/api/instances/SAGA-1/children')).body.items[0];
      assert.equal(child.status, 'COMPLETED');
      assert.equal(child.parentInstanceId, 'SAGA-1');
      // an event for another order must be ignored by the filter
      await api('POST', '/api/events/publish', { source: 'rabbitmq', exchange: 'payments', routingKey: 'payment.confirmed', message: { orderId: 'OTHER', amount: 1 } });
      await sleep(300);
      assert.equal(await statusOf('SAGA-1'), 'WAITING');
      await api('POST', '/api/events/publish', { source: 'rabbitmq', exchange: 'payments', routingKey: 'payment.confirmed', message: { orderId: 'O-77', amount: 20 } });
      await waitStatus('SAGA-1', 'COMPLETED');
      const out = (await api('GET', '/api/instances/SAGA-1')).body.output;
      assert.equal(out.payment.amount, 20);
      assert.equal(out.order.total, 20);
    });

    await test('listen-mode signal interrupts a running workflow', async () => {
      await api('POST', '/api/events/publish', { source: 'rabbitmq', exchange: 'checkout', routingKey: 'checkout.started', message: { sagaId: 'SAGA-2', orderId: 'O-78', items: [{ price: 1 }] } });
      await until(async () => (await api('GET', '/api/instances/SAGA-2/status')).body.currentNodes?.includes('pay'), 8000, 'saga at payment');
      await api('POST', '/api/instances/SAGA-2/signal', { name: 'cancel', payload: { reason: 'customer left' } });
      await waitStatus('SAGA-2', 'FAILED');
      assert.equal((await api('GET', '/api/instances/SAGA-2')).body.error, 'cancelled: customer left');
    });

    await test('error port, retries, timer(at), join of parallel branches', async () => {
      await api('POST', '/api/workflows', wf('misc', [
        n('s', 'start', { initVars: { hits: 0 } }),
        n('boom', 'activity', { kind: 'http', url: 'http://127.0.0.1:1/nothing', retries: 2, retryDelayMs: 50, timeoutMs: 1000 }),
        n('caught', 'function', { code: 'vars.caught = nodes.boom.error; return 1;' }),
        n('timer', 'timer', { mode: 'at', at: '{{ now() + 400 }}' }),
        n('join', 'join'),
        n('e', 'end', { output: { caught: '{{ !!vars.caught }}', t: '{{ !!nodes.timer.firedAt }}' } }),
      ], [{ from: 's', to: 'boom' }, { from: 's', to: 'timer' }, { from: 'boom', to: 'caught', port: 'error' }, { from: 'caught', to: 'join' }, { from: 'timer', to: 'join' }, { from: 'join', to: 'e' }]));
      const r = await api('POST', '/api/workflows/misc/start', { instanceId: 'M-1', wait: 'finish' });
      assert.equal(r.body.status, 'COMPLETED', JSON.stringify(r.body));
      assert.deepEqual(r.body.output, { caught: true, t: true });
      const acts = (await api('GET', '/api/instances/M-1/activities')).body;
      const boom = acts.find((a) => a.nodeId === 'boom');
      assert.equal(boom.status, 'FAILED'); assert.equal(boom.attempt, 3);
    });

    await test('unhandled failure fails the instance; retry re-runs with same input', async () => {
      await api('POST', '/api/workflows', wf('fails', [n('s', 'start'), n('f', 'function', { code: 'if (!input.ok) throw new Error("nope"); return 1;' })], [{ from: 's', to: 'f' }]));
      const r = await api('POST', '/api/workflows/fails/start', { instanceId: 'F-1', wait: 'finish', input: { ok: false } });
      assert.equal(r.body.status, 'FAILED'); assert.match(r.body.error, /nope/);
      const rr = await api('POST', '/api/instances/F-1/retry', { instanceId: 'F-1b' });
      assert.equal(rr.status, 202); assert.equal(rr.body.retryOf, 'F-1');
      await waitStatus('F-1b', 'FAILED');
    });

    await test('logs per instance + retention setting', async () => {
      const logs = (await api('GET', '/api/instances/T-2/logs')).body;
      assert.ok(logs.some((l) => l.message.includes('signal "approve" received')));
      assert.ok(logs.some((l) => l.nodeId === 'total' && l.message.includes('total is')));
      const r = await api('PUT', '/api/settings/log-retention', { retentionHours: 48 });
      assert.equal(r.body.retentionHours, 48);
      const all = (await api('GET', '/api/logs?level=warn,error&limit=5')).body;
      assert.ok(all.length <= 5 && all.every((l) => ['warn', 'error'].includes(l.level)));
    });

    await test('instance listing & filters', async () => {
      const r = (await api('GET', '/api/instances?workflowId=order-approval&status=COMPLETED')).body;
      assert.ok(r.total >= 2 && r.items.every((i) => i.status === 'COMPLETED' && !i.definition));
    });
  } finally {
    wrk.kill('SIGKILL');
    srv.kill('SIGTERM');
    await sleep(500);
    fs.rmSync(tmp, { recursive: true, force: true });
  }
  const failed = results.filter((r) => !r[1]).length;
  console.log(`\n${results.length - failed}/${results.length} passed`);
  process.exit(failed ? 1 : 0);
})();
