'use strict';
/*
 * Worker application: the only job of a worker is to host workflow child processes.
 *
 *   ┌────────── leader master ──────────┐
 *   │  WorkerLink.Attach (bidi stream)  │◀─── Basic auth (cluster credentials)
 *   └───────────────▲───────────────────┘
 *                   │ spawn / to_child / kill        ▲ register / child / spawned / exited / heartbeat
 *   ┌───────────────┴──── worker ───────────────────┐
 *   │ Orchestrator.Connect server (loopback)        │◀─── each child: Basic instanceId:token
 *   │ fork() children · relay · buffer · re-attach  │
 *   └───────────────────────────────────────────────┘
 *
 * Children keep running if the leader dies: the worker buffers their messages,
 * finds the new leader (redirect / discovery) and re-registers its running
 * children so the new leader adopts them.
 */
const os = require('os');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { fork } = require('child_process');
const EventEmitter = require('events');
const C = require('../grpc/common');
const { createCoordinator } = require('../cluster/coordinator');

const CHILD_ENTRY = path.join(__dirname, '..', 'child', 'index.js');
const VERSION = require('../../package.json').version;
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

// Ops whose in-flight requests are failed on leader loss (the node's retry policy handles them).
// workflow.start (wait) is NOT failed: the new leader answers it when the sub-workflow finishes.
const FAIL_ON_FAILOVER = new Set(['rabbit.publish', 'mqtt.publish', 'db.write', 'event.subscribe', 'event.unsubscribe', 'workflow.cancel']);

class Worker extends EventEmitter {
  constructor(config, loaded = {}) {
    super();
    this.cfg = config;
    this.loaded = loaded;        // { shared, raw, file } when the config came from etcd/MongoDB
    this.id = config.node.id;
    this.children = new Map(); // instanceId -> child record
    this.link = null;          // active Attach stream to the leader
    this.linkReady = false;
    this.buffer = [];
    this.dropped = 0;
    this.draining = false;
    this.stopped = false;
    this.preferred = null;     // leader address learnt from a redirect
    this.leaderId = null;
    this.term = 0;
    this.minLevel = LEVELS[config.logs?.level] ?? 20;
  }

  log(level, msg) {
    if ((LEVELS[level] ?? 20) < this.minLevel) return;
    (level === 'error' ? console.error : console.log)(`${new Date().toISOString()} ${level.toUpperCase().padEnd(5)} [worker ${this.id}] ${msg}`);
  }

  async start() {
    const pkg = C.loadProto();
    // 1) local gRPC server for the child processes
    this.server = new C.grpc.Server({ 'grpc.max_receive_message_length': 32 << 20, 'grpc.max_send_message_length': 32 << 20 });
    this.server.addService(pkg.Orchestrator.service, { Connect: (call) => this._onChildConnect(call) });
    const { host, port } = this.cfg.grpc;
    this.localPort = await C.bindServer(this.server, host, port, null);
    this.localAddr = `${host === '0.0.0.0' ? '127.0.0.1' : host}:${this.localPort}`;
    this.log('info', `child gRPC server on ${this.localAddr}, capacity ${this.cfg.runtime.maxConcurrent}, tags [${this.cfg.node.tags.join(', ')}]`);

    // 2) service discovery (optional: registers this worker, tells us the leader)
    const d = this.cfg.discovery;
    if (d.type !== 'static') {
      this.coord = createCoordinator(d.type, { etcd: d.etcd, mongo: d.mongo, leaseTtlMs: d.ttlMs });
      this.coord.on('leader', (l) => {
        if (l && l.nodeId !== this.leaderId) { this.log('info', `discovery: leader is ${l.nodeId} (${l.grpcAddress})`); this.preferred = l.grpcAddress; if (this.link && this.leaderId && this.leaderId !== l.nodeId) this._dropLink('leader changed'); }
      });
      this.coord.on('warn', (e) => this.log('warn', `discovery: ${e.message}`));
      await this.coord.start({ nodeId: this.id, role: 'worker', grpcAddress: this.localAddr, httpUrl: this.cfg.http.advertiseUrl, tags: this.cfg.node.tags, capacity: this.cfg.runtime.maxConcurrent, campaign: false });
    }

    // 3) health / metrics endpoint
    if (this.cfg.http.port !== false && this.cfg.http.port !== null) {
      this.http = http.createServer((req, res) => {
        const body = JSON.stringify(this.status(), null, 2);
        res.writeHead(req.url === '/health' && !this.linkReady ? 503 : 200, { 'content-type': 'application/json' });
        res.end(body);
      });
      await new Promise((r) => this.http.listen(this.cfg.http.port, this.cfg.http.host, r));
      this.httpPort = this.http.address().port;
      this.log('info', `health endpoint on http://${this.cfg.http.host}:${this.httpPort}/health`);
    }

    // 4) follow the cluster-shared config (etcd / MongoDB)
    const shared = this.loaded.shared;
    if (shared) {
      shared.on('change', (rec) => this._applyShared(rec));
      shared.on('error', (e) => this.log('warn', `shared config watch: ${e.message}`));
      await shared.watch();
      this.log('info', `using shared config v${this.cfg.sharedVersion} from ${shared.source}`);
    }

    // 5) attach to the leader
    this._connect();
    this.beat = setInterval(() => this._send({ heartbeat: this._beat() }, true), 5000);
    this.beat.unref();
  }

  status() {
    return {
      nodeId: this.id, role: 'worker', version: VERSION, connected: this.linkReady, leader: this.leaderId, term: this.term,
      draining: this.draining, running: this.children.size, capacity: this.cfg.runtime.maxConcurrent, tags: this.cfg.node.tags,
      buffered: this.buffer.length, dropped: this.dropped, rss: process.memoryUsage().rss, load: os.loadavg()[0],
      children: [...this.children.values()].map((c) => ({ instanceId: c.instanceId, pid: c.proc.pid, startedAt: c.startedAt, connected: !!c.call })),
    };
  }

  _applyShared(rec) {
    if (rec.version === this.cfg.sharedVersion) return;
    const { buildConfig } = require('../config');
    const { diffPaths } = require('../cluster/sharedConfig');
    let next;
    try { next = buildConfig('worker', this.loaded.raw, this.loaded.file, rec.doc); }
    catch (e) { this.log('error', `shared config v${rec.version} rejected: ${e.message}`); return; }
    const strip = ({ root, file, sharedVersion, shared, ...r }) => r;
    const changed = diffPaths(strip(this.cfg), strip(next));
    const LIVE = ['runtime', 'logs', 'security.grpc', 'discovery.static'];
    const live = changed.filter((p) => LIVE.some((k) => p === k || p.startsWith(k + '.')));
    const restart = changed.filter((p) => !live.includes(p));
    Object.assign(this.cfg.runtime, next.runtime);   // capacity, buffer, drain, reconnect
    Object.assign(this.cfg.logs, next.logs);
    Object.assign(this.cfg.security.grpc, next.security.grpc); // used on the next (re)connect
    this.cfg.discovery.static = next.discovery.static;
    this.minLevel = LEVELS[this.cfg.logs.level] ?? 20;
    this.cfg.sharedVersion = rec.version;
    this._send({ heartbeat: this._beat() }, true); // tell the leader about a new capacity now
    this.log('info', `shared config v${rec.version} applied${live.length ? ': ' + live.join(', ') : ''}`);
    if (restart.length) this.log('warn', `shared config v${rec.version}: restart this worker to apply ${restart.join(', ')}`);
  }

  _beat() {
    return { running: this.children.size, capacity: this.cfg.runtime.maxConcurrent, draining: this.draining, load: os.loadavg()[0], rss: process.memoryUsage().rss };
  }

  // ───────────── link to the leader ─────────────
  _candidates() {
    const list = [];
    if (this.preferred) list.push(this.preferred);
    if (this.coord?.leader?.grpcAddress) list.push(this.coord.leader.grpcAddress);
    for (const a of this.cfg.discovery.static?.masters || []) list.push(a);
    return [...new Set(list)];
  }

  _connect() {
    if (this.stopped || this.link) return;
    const cands = this._candidates();
    if (!cands.length) return this._retry();
    this.attempt = (this.attempt || 0) + 1;
    const addr = cands[(this.attempt - 1) % cands.length];
    const pkg = C.loadProto();
    const tls = this.cfg.masters.tls;
    const client = new pkg.WorkerLink(addr, C.channelCredentials(tls), C.clientOptions(tls));
    const md = C.authMetadata(this.cfg.security.grpc.username, this.cfg.security.grpc.password, { 'x-node-id': this.id });
    const call = client.Attach(md);
    this.link = call; this.linkClient = client; this.linkAddr = addr; this.linkReady = false;
    this.log('debug', `attaching to ${addr}`);

    call.on('data', (m) => this._onLeaderMessage(m, call));
    const done = (why) => {
      if (this.link !== call) return;
      this._dropLink(why);
    };
    call.on('error', (e) => done(e.details || e.message));
    call.on('end', () => done('stream ended'));

    call.write({ worker_id: this.id, register: {
      tags: this.cfg.node.tags, capacity: this.cfg.runtime.maxConcurrent, grpc_address: this.localAddr,
      http_url: this.cfg.http.advertiseUrl, version: VERSION, hostname: os.hostname(),
      running: [...this.children.values()].map((c) => ({ instance_id: c.instanceId, pid: c.proc.pid, started: c.started, subscriptions_json: JSON.stringify([...c.subs].map(([id, spec]) => ({ id, spec }))) })),
    } });
  }

  _dropLink(why) {
    const call = this.link;
    if (!call) return;
    const was = this.linkReady;
    this.link = null; this.linkReady = false;
    try { call.cancel(); } catch {}
    try { this.linkClient?.close(); } catch {}
    if (was) this.log('warn', `lost leader ${this.leaderId} (${why}); ${this.children.size} workflow(s) keep running, buffering`);
    else this.log('debug', `attach to ${this.linkAddr} failed: ${why}`);
    // fail in-flight requests that a new leader will not answer
    for (const c of this.children.values()) {
      for (const [rid, p] of c.pending) {
        if (!FAIL_ON_FAILOVER.has(p.op)) continue;
        c.pending.delete(rid);
        this._toChild(c, { reply: { request_id: rid, ok: false, result_json: '', error: `leader failover (${why}) – request not confirmed, retry` } });
      }
    }
    this._retry();
  }

  _retry() {
    if (this.stopped) return;
    clearTimeout(this.retryTimer);
    const ms = Math.min(this.cfg.runtime.reconnectMs * Math.min(this.failures = (this.failures || 0) + 1, 5), 5000);
    this.retryTimer = setTimeout(() => this._connect(), this.linkWasReady ? 50 : ms);
    this.linkWasReady = false;
  }

  _onLeaderMessage(m, call) {
    switch (m.body) {
      case 'redirect': {
        const r = m.redirect;
        this.log('debug', `${this.linkAddr} is not the leader → ${r.leader_id || '?'} ${r.leader_address || ''} (${r.reason})`);
        if (r.leader_address) this.preferred = r.leader_address;
        if (this.link === call) { this.link = null; try { call.cancel(); } catch {} this.linkWasReady = !!r.leader_address; this._retry(); }
        return;
      }
      case 'registered': {
        const r = m.registered;
        if (r.term < this.term) { this.log('warn', `ignoring stale leader ${r.leader_id} term ${r.term} < ${this.term}`); this._dropLink('stale leader'); return; }
        this.term = r.term; this.leaderId = r.leader_id; this.linkReady = true; this.failures = 0; this.preferred = this.linkAddr; this.linkWasReady = true;
        const n = this.buffer.length;
        const buf = this.buffer; this.buffer = [];
        for (const msg of buf) call.write(msg);
        this.log('info', `attached to leader ${r.leader_id} at ${this.linkAddr} (term ${r.term})${n ? `, flushed ${n} buffered message(s)` : ''}${this.dropped ? `, ${this.dropped} dropped` : ''}`);
        this.dropped = 0;
        call.write({ worker_id: this.id, heartbeat: this._beat() });
        return;
      }
      case 'spawn': return this._spawn(m.spawn);
      case 'to_child': {
        const c = this.children.get(m.to_child.instance_id);
        if (!c) return;
        const msg = m.to_child.msg;
        if (msg.body === 'reply') this._trackReply(c, msg.reply);
        if (msg.body === 'start') c.started = true;
        this._toChild(c, msg);
        return;
      }
      case 'kill': {
        const c = this.children.get(m.kill.instance_id);
        if (c && !c.exited) { this.log('info', `kill ${m.kill.signal || 'SIGKILL'} → ${c.instanceId} (pid ${c.proc.pid})`); c.proc.kill(m.kill.signal || 'SIGKILL'); }
        return;
      }
      default:
    }
  }

  /** Queue a message for the leader; buffered while no leader is attached. */
  _send(body, volatileMsg = false) {
    const msg = { worker_id: this.id, ...body };
    if (this.link && this.linkReady) { try { this.link.write(msg); return; } catch {} }
    if (volatileMsg) return; // heartbeats are not worth buffering
    if (this.buffer.length >= this.cfg.runtime.bufferLimit) {
      // drop the oldest log line first, otherwise the oldest message
      const i = this.buffer.findIndex((x) => x.child?.msg?.body === 'log' || x.child?.msg?.body === 'heartbeat');
      this.buffer.splice(i >= 0 ? i : 0, 1); this.dropped++;
    }
    this.buffer.push(msg);
  }

  // ───────────── children ─────────────
  _spawn({ instance_id: instanceId, max_old_space_mb: mem }) {
    if (this.children.has(instanceId)) return;
    if (this.draining || this.children.size >= this.cfg.runtime.maxConcurrent) {
      this._send({ exited: { instance_id: instanceId, code: -1, signal: '', error: this.draining ? 'worker draining' : 'worker at capacity' } });
      return;
    }
    const token = crypto.randomBytes(18).toString('hex');
    const proc = fork(CHILD_ENTRY, [], {
      env: { ...process.env, FLOWFORGE_GRPC: this.localAddr, FLOWFORGE_INSTANCE: instanceId, FLOWFORGE_TOKEN: token, FLOWFORGE_WORKER: this.id },
      execArgv: [`--max-old-space-size=${mem || this.cfg.runtime.childMaxOldSpaceMb}`],
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });
    const c = { instanceId, token, proc, call: null, early: [], pending: new Map(), subs: new Map(), started: false, exited: false, startedAt: Date.now() };
    this.children.set(instanceId, c);
    this.log('info', `spawned ${instanceId} (pid ${proc.pid}) – ${this.children.size}/${this.cfg.runtime.maxConcurrent}`);
    this._send({ spawned: { instance_id: instanceId, pid: proc.pid } });
    proc.on('exit', (code, signal) => {
      c.exited = true;
      this.children.delete(instanceId);
      this.log('debug', `${instanceId} exited code=${code} signal=${signal}`);
      this._send({ exited: { instance_id: instanceId, code: code ?? -1, signal: signal || '', error: '' } });
      this.emit('exit', instanceId);
    });
    proc.on('error', (e) => this.log('error', `${instanceId}: ${e.message}`));
  }

  _onChildConnect(call) {
    const creds = C.parseBasic(call.metadata.get('authorization'));
    const c = creds && this.children.get(creds.username);
    if (!c || !C.safeEqual(c.token, creds.password)) {
      call.emit('error', C.unauthenticated());
      try { call.end(); } catch {}
      return;
    }
    c.call = call;
    for (const m of c.early) call.write(m);
    c.early = [];
    call.on('data', (msg) => {
      if (msg.body === 'request') c.pending.set(msg.request.request_id, { op: msg.request.op, payload: msg.request.payload_json });
      if (msg.body === 'heartbeat') return; // liveness is tracked locally
      msg.token = ''; // the child's secret never leaves this worker
      this._send({ child: { instance_id: c.instanceId, msg } });
    });
    call.on('error', () => {});
    call.on('end', () => { try { call.end(); } catch {} });
  }

  _trackReply(c, reply) {
    const p = c.pending.get(reply.request_id);
    if (!p) return;
    c.pending.delete(reply.request_id);
    if (!reply.ok) return;
    if (p.op === 'event.subscribe') {
      try { c.subs.set(JSON.parse(reply.result_json).subscriptionId, JSON.parse(p.payload || '{}')); } catch {}
    } else if (p.op === 'event.unsubscribe') {
      try { c.subs.delete(JSON.parse(p.payload).subscriptionId); } catch {}
    }
  }

  _toChild(c, msg) {
    if (c.call) { try { c.call.write(msg); } catch {} } else c.early.push(msg);
  }

  // ───────────── shutdown ─────────────
  /** Graceful: stop taking work, wait for running workflows (up to drainTimeoutMs), then kill the rest. */
  async drain() {
    if (this.draining) return;
    this.draining = true;
    this._send({ heartbeat: this._beat() }, true);
    const deadline = Date.now() + this.cfg.runtime.drainTimeoutMs;
    this.log('info', `draining: waiting for ${this.children.size} workflow(s) (max ${this.cfg.runtime.drainTimeoutMs} ms)`);
    while (this.children.size && Date.now() < deadline) await new Promise((r) => setTimeout(r, 250));
  }

  async stop({ graceful = true } = {}) {
    if (graceful) await this.drain();
    for (const c of this.children.values()) c.proc.kill('SIGKILL');
    await new Promise((r) => setTimeout(r, 300)); // let exit notifications reach the leader
    this.stopped = true;
    clearTimeout(this.retryTimer); clearInterval(this.beat);
    try { this.link?.end(); } catch {}
    this.http?.close();
    await this.coord?.stop().catch(() => {});
    await this.loaded.shared?.close().catch(() => {});
    await new Promise((r) => this.server.tryShutdown(() => r()));
  }
}

module.exports = { Worker };
