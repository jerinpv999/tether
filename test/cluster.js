'use strict';
/*
 * Cluster / HA test: etcd + 2 masters + 2 workers (separate processes, YAML configs).
 *   ETCD_BIN=/path/to/etcd node test/cluster.js
 * Covers: leader election, follower forwarding, workflow sync, scheduling across
 * workers, worker tags, leader failover with adoption of running workflows,
 * master rejoin, worker loss, graceful worker drain.
 */
const { spawn, execFileSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const assert = require('assert/strict');

const ETCD = process.env.ETCD_BIN || 'etcd';
try { execFileSync(ETCD, ['--version'], { stdio: 'ignore' }); } catch { console.log(`etcd binary not found (${ETCD}) – set ETCD_BIN. Skipping.`); process.exit(0); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flowforge-cluster-'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const bin = (n) => path.join(__dirname, '..', 'bin', n);
const ETCD_PORT = 23790, ETCD_PEER = 23800;
const M = { 'master-1': { http: 3961, grpc: 50961 }, 'master-2': { http: 3962, grpc: 50962 } };
const W = { 'worker-1': { grpc: 50971, http: 3971, tags: '[default, eu]', disc: 'etcd' }, 'worker-2': { grpc: 50972, http: 3972, tags: '[default]', disc: 'static' } };
const PASS = 'cluster-pass-' + Date.now();
const procs = {};
const VERBOSE = !!process.env.VERBOSE;

function startEtcd() {
  procs.etcd = spawn(ETCD, ['--data-dir', path.join(tmp, 'etcd'), '--listen-client-urls', `http://127.0.0.1:${ETCD_PORT}`, '--advertise-client-urls', `http://127.0.0.1:${ETCD_PORT}`,
    '--listen-peer-urls', `http://127.0.0.1:${ETCD_PEER}`, '--initial-advertise-peer-urls', `http://127.0.0.1:${ETCD_PEER}`, '--initial-cluster', `default=http://127.0.0.1:${ETCD_PEER}`, '--log-level', 'error'], { stdio: 'ignore' });
}

const SHARED = `shared: { source: etcd, etcd: { hosts: ["127.0.0.1:${ETCD_PORT}"], prefix: /fftest/ } }`;

// Cluster-wide settings live in etcd – pushed with the admin CLI before any node starts.
function sharedDoc(over = {}) {
  return {
    common: { security: { grpc: { username: 'cluster', password: '${CLUSTER_PASS}' } }, logs: { level: VERBOSE ? 'info' : 'warn', ...(over.commonLogs || {}) } },
    master: {
      cluster: { enabled: true, coordinator: 'etcd', leaseTtlMs: 3000, workerLostGraceMs: over.grace || 4000, onWorkerLost: 'fail', syncIntervalMs: 1000, etcd: { hosts: [`127.0.0.1:${ETCD_PORT}`], prefix: '/fftest/' } },
      store: { type: 'etcd', etcd: { hosts: [`127.0.0.1:${ETCD_PORT}`], prefix: '/fftest/' } },
      dataDb: { type: 'auto' },
      logs: { console: VERBOSE, retentionHours: over.retention || 168 },
      runtime: { terminateGraceMs: 1500 },
      security: { api: over.api || { enabled: false } },
    },
    worker: {
      discovery: { type: 'etcd', etcd: { hosts: [`127.0.0.1:${ETCD_PORT}`], prefix: '/fftest/' }, ttlMs: 3000 },
      runtime: { maxConcurrent: over.capacity || 10, reconnectMs: 300, drainTimeoutMs: 8000 },
    },
  };
}
function push(doc, extra = []) {
  const f = path.join(tmp, `cluster-${Date.now()}.yaml`);
  fs.writeFileSync(f, require('yaml').stringify(doc));
  return execFileSync(process.execPath, [bin('flowforge.js'), 'config', 'push', f, '--etcd', `127.0.0.1:${ETCD_PORT}`, '--prefix', '/fftest/', ...extra], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

function startMaster(id) {
  const f = path.join(tmp, `${id}.yaml`);
  fs.writeFileSync(f, `
node: { id: ${id} }
http: { port: ${M[id].http}, advertiseUrl: "http://127.0.0.1:${M[id].http}" }
grpc: { host: 127.0.0.1, port: ${M[id].grpc}, advertiseAddress: "127.0.0.1:${M[id].grpc}" }
${SHARED}
`);
  procs[id] = spawn(process.execPath, [bin('master.js'), '--config', f], { env: { ...process.env, CLUSTER_PASS: PASS }, stdio: ['ignore', VERBOSE ? 'inherit' : 'ignore', 'inherit'] });
}

function startWorker(id) {
  const w = W[id];
  const f = path.join(tmp, `${id}.yaml`);
  fs.writeFileSync(f, `
node: { id: ${id}, tags: ${w.tags} }
grpc: { port: ${w.grpc} }
http: { port: ${w.http} }
${w.disc === 'static' ? `discovery: { type: static, static: { masters: ["127.0.0.1:${M['master-1'].grpc}", "127.0.0.1:${M['master-2'].grpc}"] } }` : ''}
${SHARED}
`);
  procs[id] = spawn(process.execPath, [bin('worker.js'), '--config', f], { env: { ...process.env, CLUSTER_PASS: PASS }, stdio: ['ignore', VERBOSE ? 'inherit' : 'ignore', 'inherit'] });
}

let AUTH = null;
async function api(master, method, url, body) {
  const headers = { 'content-type': 'application/json' };
  if (AUTH) headers.authorization = 'Basic ' + Buffer.from(AUTH).toString('base64');
  const r = await fetch(`http://127.0.0.1:${M[master].http}${url}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await r.text();
  let json; try { json = JSON.parse(text); } catch { json = text; }
  return { status: r.status, body: json, headers: r.headers };
}
async function until(fn, ms = 10000, label = 'condition') {
  const end = Date.now() + ms;
  let last;
  while (Date.now() < end) { try { last = await fn(); if (last) return last; } catch (e) { last = e.message; } await sleep(150); }
  throw new Error(`timeout waiting for ${label} (last: ${JSON.stringify(last)})`);
}
const alive = (pid) => { try { process.kill(pid, 0); return true; } catch { return false; } };
const leaderOf = async (m) => (await api(m, 'GET', '/api/health')).body.leaderId;
const inst = async (m, id) => (await api(m, 'GET', `/api/instances/${id}`)).body;

const results = [];
async function test(name, fn) {
  const t = Date.now();
  try { await fn(); results.push(true); console.log(`  ✔ ${name} (${Date.now() - t} ms)`); }
  catch (e) { results.push(false); console.log(`  ✘ ${name}\n    ${e.stack}`); }
}

(async () => {
  let leader, follower;
  try {
    startEtcd(); await sleep(1500);
    console.log('  ' + push(sharedDoc()));
    startMaster('master-1'); await sleep(800); startMaster('master-2');
    startWorker('worker-1'); startWorker('worker-2');
    await until(async () => (await leaderOf('master-1')) && (await leaderOf('master-1')) === (await leaderOf('master-2')), 15000, 'leader election');
    leader = await leaderOf('master-1'); follower = leader === 'master-1' ? 'master-2' : 'master-1';
    await until(async () => (await api(follower, 'GET', '/api/cluster')).body.workers.filter((w) => w.connected).length === 2, 15000, 'workers attached');
    console.log(`cluster up: leader ${leader}, follower ${follower}, 2 workers\n`);

    await test('exactly one leader; cluster view lists masters and workers', async () => {
      const c = (await api(follower, 'GET', '/api/cluster')).body;
      assert.equal(c.self.isLeader, false);
      assert.equal(c.leader.nodeId, leader);
      assert.equal(c.masters.length, 2);
      assert.deepEqual(c.masters.filter((m) => m.leader).map((m) => m.nodeId), [leader]);
      assert.deepEqual(c.workers.map((w) => w.id).sort(), ['worker-1', 'worker-2']);
    });

    await test('HTTP trigger on a FOLLOWER is forwarded to the leader (sync response)', async () => {
      await until(async () => (await api(follower, 'GET', '/api/workflows')).body.length >= 3, 8000, 'examples seeded');
      const r = await api(follower, 'POST', '/hooks/orders', { orderId: 'C-1', items: [{ price: 7, qty: 3 }] });
      assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.equal(r.body.total, 21);
      assert.equal(r.headers.get('x-flowforge-node'), follower);
    });

    await test('workflow saved on follower is picked up by leader; work spreads over both workers', async () => {
      const wf = { id: 'sleepy', name: 'sleepy', nodes: [{ id: 's', type: 'start', x: 0, y: 0, config: {} }, { id: 'd', type: 'delay', x: 0, y: 0, config: { duration: '{{ input.ms }}' } }, { id: 'e', type: 'end', x: 0, y: 0, config: {} }],
        edges: [{ from: 's', to: 'd' }, { from: 'd', to: 'e' }] };
      assert.equal((await api(follower, 'POST', '/api/workflows', wf)).status, 201);
      for (let i = 0; i < 8; i++) assert.equal((await api(i % 2 ? leader : follower, 'POST', '/api/workflows/sleepy/start', { instanceId: `SL-${i}`, input: { ms: 1500 } })).status, 202);
      await until(async () => { const l = (await api(leader, 'GET', '/api/instances?workflowId=sleepy&status=COMPLETED')).body; return l.total === 8; }, 15000, '8 sleepy completed');
      const workers = new Set((await api(leader, 'GET', '/api/instances?workflowId=sleepy')).body.items.map((i) => i.workerId));
      assert.deepEqual([...workers].sort(), ['worker-1', 'worker-2']);
    });

    await test('settings.workerTags pins a workflow to matching workers', async () => {
      const wf = { id: 'eu-only', name: 'eu-only', settings: { workerTags: ['eu'] }, nodes: [{ id: 's', type: 'start', x: 0, y: 0, config: {} }, { id: 'e', type: 'end', x: 0, y: 0, config: {} }], edges: [{ from: 's', to: 'e' }] };
      await api(leader, 'POST', '/api/workflows', wf);
      for (let i = 0; i < 4; i++) await api(leader, 'POST', '/api/workflows/eu-only/start', { instanceId: `EU-${i}`, wait: 'finish' });
      const items = (await api(leader, 'GET', '/api/instances?workflowId=eu-only')).body.items;
      assert.equal(items.length, 4);
      assert.ok(items.every((i) => i.workerId === 'worker-1' && i.status === 'COMPLETED'), JSON.stringify(items.map((i) => [i.workerId, i.status])));
    });

    await test('LEADER FAILOVER: kill -9 leader; waiting workflow is adopted and completes', async () => {
      const r = await api(follower, 'POST', '/hooks/orders', { orderId: 'FO-1', items: [{ price: 9000 }] });
      assert.equal(r.status, 202);
      await until(async () => (await inst(follower, 'FO-1')).status === 'WAITING', 8000, 'FO-1 waiting');
      const before = await inst(follower, 'FO-1');
      const workerPid = before.pid;
      const t0 = Date.now();
      procs[leader].kill('SIGKILL');
      await until(async () => (await leaderOf(follower)) === follower, 15000, 'follower elected');
      const electedIn = Date.now() - t0;
      await until(async () => { const c = (await api(follower, 'GET', '/api/cluster')).body; return c.workers.filter((w) => w.connected).length === 2 && !c.orphaned.length; }, 15000, 'workers re-attached, nothing orphaned');
      assert.ok(alive(workerPid), 'workflow process survived the failover');
      assert.equal((await inst(follower, 'FO-1')).status, 'WAITING');
      await api(follower, 'POST', '/api/instances/FO-1/signal', { name: 'approve', payload: { by: 'failover-test' } });
      await until(async () => (await inst(follower, 'FO-1')).status === 'COMPLETED', 8000, 'FO-1 completed');
      assert.equal((await inst(follower, 'FO-1')).output.approvedBy, 'failover-test');
      console.log(`    (new leader in ${electedIn} ms, same pid ${workerPid} kept running)`);
      [leader, follower] = [follower, leader]; // follower took over; old leader is dead
    });

    await test('killed master restarts and rejoins as follower', async () => {
      startMaster(follower);
      await until(async () => (await leaderOf(follower)) === leader, 15000, 'rejoined');
      const h = (await api(follower, 'GET', '/api/health')).body;
      assert.equal(h.leader, false);
      const r = await api(follower, 'POST', '/hooks/orders', { orderId: 'RJ-1', items: [{ price: 1 }] });
      assert.equal(r.status, 200);
    });

    await test('every node runs on the shared config from etcd (no local cluster settings)', async () => {
      for (const m of [leader, follower]) {
        const c = (await api(m, 'GET', '/api/cluster/config')).body;
        assert.equal(c.source, 'etcd');
        assert.equal(c.appliedVersion, c.version);
        assert.equal(c.effective.store.type, 'etcd');
        assert.equal(c.effective.dataDb.type, 'etcd');
        assert.equal(c.effective.security.grpc.password, '***'); // secrets redacted
        assert.equal(c.doc.common.security.grpc.password, '${CLUSTER_PASS}'); // env reference kept, resolved per node
      }
    });

    await test('db.write data is shared: rows written by the old leader are readable after failover', async () => {
      const rows = (await api(leader, 'GET', '/api/data/orders')).body;
      const ids = rows.map((r) => r.orderId);
      assert.ok(ids.includes('C-1'), `C-1 (written before failover) missing: ${ids}`);
      assert.ok(ids.includes('FO-1'), 'FO-1 (written after failover) missing');
      assert.equal((await api(follower, 'GET', '/api/data')).body.backend, 'etcd');
    });

    await test('pushing a new shared config is applied live by masters and workers', async () => {
      const before = (await api(leader, 'GET', '/api/cluster/config')).body.version;
      const out = push(sharedDoc({ grace: 5000, capacity: 7, retention: 72 }));
      assert.match(out, /pushed .* as v\d+/);
      await until(async () => {
        const a = (await api(leader, 'GET', '/api/cluster/config')).body, b = (await api(follower, 'GET', '/api/cluster/config')).body;
        return a.appliedVersion > before && b.appliedVersion === a.appliedVersion && a.effective.cluster.workerLostGraceMs === 5000 && b.effective.logs.retentionHours === 72;
      }, 8000, 'masters applied');
      await until(async () => (await api(leader, 'GET', '/api/cluster')).body.workers.filter((w) => w.connected).every((w) => w.capacity === 7), 8000, 'workers capacity 7');
    });

    await test('PUT /api/settings/log-retention on a follower updates the shared config for all masters', async () => {
      const r = await api(follower, 'PUT', '/api/settings/log-retention', { retentionHours: 48 });
      assert.equal(r.status, 200); assert.ok(r.body.sharedVersion > 0);
      await until(async () => (await api(leader, 'GET', '/api/cluster/config')).body.effective.logs.retentionHours === 48, 8000, 'leader retention 48');
    });

    await test('stale push is refused (optimistic concurrency)', async () => {
      const cur = (await api(leader, 'GET', '/api/cluster/config')).body.version;
      let err = '';
      try { push(sharedDoc(), ['--if-version', String(cur - 1)]); } catch (e) { err = String(e.stderr || e.message); }
      assert.match(err, /expected v/);
    });

    await test('WORKER LOSS: kill -9 a worker → its workflows fail after the grace period', async () => {
      await api(leader, 'POST', '/hooks/orders', { orderId: 'WL-1', items: [{ price: 9000 }] });
      await until(async () => (await inst(leader, 'WL-1')).status === 'WAITING', 8000, 'WL-1 waiting');
      const i = await inst(leader, 'WL-1');
      procs[i.workerId].kill('SIGKILL');
      await until(() => !alive(i.pid), 5000, 'orphaned child exits');
      await until(async () => (await inst(leader, 'WL-1')).status === 'FAILED', 12000, 'WL-1 failed');
      assert.match((await inst(leader, 'WL-1')).error, /worker lost/);
      startWorker(i.workerId);
      await until(async () => (await api(leader, 'GET', '/api/cluster')).body.workers.filter((w) => w.connected).length === 2, 15000, 'worker back');
    });

    await test('GRACEFUL DRAIN: SIGTERM a worker – running workflow finishes, no new work assigned', async () => {
      await api(leader, 'POST', '/api/workflows/sleepy/start', { instanceId: 'DR-1', input: { ms: 2500 } });
      await until(async () => (await inst(leader, 'DR-1')).status === 'WAITING', 8000, 'DR-1 running');
      const wid = (await inst(leader, 'DR-1')).workerId;
      procs[wid].kill('SIGTERM');
      await until(async () => (await api(leader, 'GET', '/api/cluster')).body.workers.find((w) => w.id === wid)?.draining, 5000, 'draining');
      await api(leader, 'POST', '/api/workflows/sleepy/start', { instanceId: 'DR-2', input: { ms: 10 }, wait: 'finish' });
      assert.notEqual((await inst(leader, 'DR-2')).workerId, wid);
      await until(async () => (await inst(leader, 'DR-1')).status === 'COMPLETED', 8000, 'DR-1 completed on draining worker');
      await until(() => procs[wid].exitCode !== null, 8000, 'worker exited after drain');
    });
    await test('API Basic auth switched on live through the shared config; shared doc editable over HTTP', async () => {
      push(sharedDoc({ api: { enabled: true, username: 'ops', password: 'pw-1' }, retention: 48 }));
      await until(async () => (await api(follower, 'GET', '/api/workflows')).status === 401, 8000, '401 on follower');
      assert.equal((await api(leader, 'GET', '/api/workflows')).status, 401);
      AUTH = 'ops:pw-1';
      assert.equal((await api(leader, 'GET', '/api/workflows')).status, 200);
      const cur = (await api(follower, 'GET', '/api/cluster/config')).body;
      const doc = sharedDoc({ api: { enabled: true, username: 'ops', password: 'pw-1' }, retention: 24 });
      const r = await api(follower, 'PUT', '/api/cluster/config', { doc, version: cur.version });
      assert.equal(r.status, 200, JSON.stringify(r.body));
      const stale = await api(follower, 'PUT', '/api/cluster/config', { doc, version: cur.version });
      assert.equal(stale.status, 409);
      await until(async () => (await api(leader, 'GET', '/api/cluster/config')).body.effective.logs.retentionHours === 24, 8000, 'retention 24 via HTTP');
    });
  } finally {
    for (const p of Object.values(procs)) try { p.kill('SIGKILL'); } catch {}
    await sleep(300);
    fs.rmSync(tmp, { recursive: true, force: true });
  }
  const failed = results.filter((r) => !r).length;
  console.log(`\n${results.length - failed}/${results.length} passed`);
  process.exit(failed ? 1 : 0);
})();
