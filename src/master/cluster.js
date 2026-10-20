'use strict';
/*
 * Master-side cluster plumbing:
 *   MasterGrpcServer – hosts WorkerLink (workers attach) and Peer (followers forward) with Basic auth
 *   RemoteRuntime    – follower-side proxy that forwards runtime calls to the current leader
 *   RuntimeFacade    – what the API / triggers use: local runtime when leader, remote otherwise
 */
const EventEmitter = require('events');
const C = require('../grpc/common');
const { HttpError } = require('./runtime');

const J = (v) => (v === undefined ? '' : JSON.stringify(v));
const P = (s, d = null) => { if (!s) return d; try { return JSON.parse(s); } catch { return d; } };

class MasterGrpcServer {
  constructor({ config, runtime, coordinator, logger }) {
    this.config = config;
    this.runtime = runtime;
    this.coord = coordinator;
    this.log = logger;
  }

  // read live so credential changes pushed through the shared config apply without restart
  verify = (u, p) => C.clusterVerifier(this.config.security.grpc)(u, p);

  async start() {
    const pkg = C.loadProto();
    this.server = new C.grpc.Server({ 'grpc.max_receive_message_length': 32 << 20, 'grpc.max_send_message_length': 32 << 20, 'grpc.keepalive_permit_without_calls': 1, 'grpc.http2.min_ping_interval_without_data_ms': 5000 });
    this.server.addService(pkg.WorkerLink.service, { Attach: (call) => this._attach(call) });
    this.server.addService(pkg.Peer.service, { Invoke: (call, cb) => this._invoke(call, cb), Watch: (call) => this._watch(call) });
    const { host, port, tls } = this.config.grpc;
    this.port = await C.bindServer(this.server, host, port, tls);
    this.log.info(`gRPC listening on ${host}:${this.port}${tls?.enabled ? ' (TLS)' : ''}; advertised as ${this.config.grpc.advertiseAddress}`);
  }

  _deny(call, what) {
    this.log.warn(`rejected ${what}: bad credentials from ${call.getPeer()}`);
  }

  _attach(call) {
    if (!C.authenticate(call, this.verify)) {
      this._deny(call, 'worker attach');
      call.emit('error', C.unauthenticated()); return;
    }
    this.runtime.onWorkerAttach(call, () => this.coord.leader);
  }

  async _invoke(call, cb) {
    if (!C.authenticate(call, this.verify)) { this._deny(call, 'peer invoke'); return cb(C.unauthenticated()); }
    const { op, args_json: argsJson } = call.request;
    const a = P(argsJson, {});
    const rt = this.runtime;
    try {
      if (!rt.active) return cb(null, { ok: false, status: 503, error: 'not the leader', result_json: '' });
      let result;
      switch (op) {
        case 'start': result = await rt.start(a.workflowId, a.opts); break;
        case 'startAndWait': result = await rt.startAndWait(a.workflowId, a.opts, a.wait, a.timeoutMs); break;
        case 'control': result = await rt.control(a.instanceId, a.action, a.opts); break;
        case 'retry': result = await rt.retry(a.instanceId, a.newId); break;
        case 'waitForResponse': result = await rt.waitForResponse(a.instanceId, a.timeoutMs); break;
        case 'waitForFinish': result = await rt.waitForFinish(a.instanceId, a.timeoutMs); break;
        case 'stats': result = rt.stats(); break;
        default: return cb(null, { ok: false, status: 400, error: `unknown op ${op}`, result_json: '' });
      }
      cb(null, { ok: true, status: 200, result_json: J(result), error: '' });
    } catch (e) {
      cb(null, { ok: false, status: e.status || 500, error: e.message, result_json: '' });
    }
  }

  _watch(call) {
    if (!C.authenticate(call, this.verify)) { this._deny(call, 'peer watch'); call.emit('error', C.unauthenticated()); return; }
    const fwd = (type) => (data) => { try { call.write({ type, data_json: J(data) }); } catch {} };
    const handlers = { instance: fwd('instance'), activity: fwd('activity'), worker: fwd('worker') };
    for (const [k, f] of Object.entries(handlers)) this.runtime.on(k, f);
    const stop = () => { for (const [k, f] of Object.entries(handlers)) this.runtime.off(k, f); };
    call.on('cancelled', stop); call.on('close', stop); call.on('error', stop);
  }

  async stop() { await new Promise((r) => this.server.tryShutdown(() => r())); }
}

class RemoteRuntime extends EventEmitter {
  constructor({ config, coordinator, logger }) {
    super();
    this.setMaxListeners(1000);
    this.config = config;
    this.coord = coordinator;
    this.log = logger;
    this.clients = new Map();
  }

  _client(addr) {
    let c = this.clients.get(addr);
    if (!c) {
      const pkg = C.loadProto();
      const tls = this.config.grpc.tls;
      c = new pkg.Peer(addr, C.channelCredentials(tls), C.clientOptions(tls));
      this.clients.set(addr, c);
    }
    return c;
  }

  _md() { return C.authMetadata(this.config.security.grpc.username, this.config.security.grpc.password, { 'x-node-id': this.config.node.id }); }

  async invoke(op, args, timeoutMs = 30000) {
    const leader = this.coord.leader;
    if (!leader?.grpcAddress) throw new HttpError(503, 'no leader elected yet – retry shortly');
    const client = this._client(leader.grpcAddress);
    const reply = await new Promise((res, rej) => client.Invoke({ op, args_json: J(args), from_node: this.config.node.id }, this._md(), { deadline: Date.now() + timeoutMs + 5000 }, (e, r) => (e ? rej(e) : res(r))))
      .catch((e) => { throw new HttpError(e.code === C.grpc.status.UNAUTHENTICATED ? 500 : 503, `leader ${leader.nodeId} unreachable: ${e.details || e.message}`); });
    if (!reply.ok) throw new HttpError(reply.status || 500, reply.error);
    return P(reply.result_json);
  }

  start(workflowId, opts) { return this.invoke('start', { workflowId, opts }); }
  startAndWait(workflowId, opts, wait, timeoutMs) { return this.invoke('startAndWait', { workflowId, opts, wait, timeoutMs }, timeoutMs); }
  control(instanceId, action, opts) { return this.invoke('control', { instanceId, action, opts }); }
  retry(instanceId, newId) { return this.invoke('retry', { instanceId, newId }); }
  waitForResponse(instanceId, timeoutMs) { return this.invoke('waitForResponse', { instanceId, timeoutMs }, timeoutMs); }
  waitForFinish(instanceId, timeoutMs) { return this.invoke('waitForFinish', { instanceId, timeoutMs }, timeoutMs); }
  stats() { return this.invoke('stats', {}); }

  /** Mirror the leader's live events (for SSE on this follower). */
  watch() {
    this.unwatch();
    const leader = this.coord.leader;
    if (!leader?.grpcAddress || leader.nodeId === this.config.node.id) return;
    const call = this._client(leader.grpcAddress).Watch({ from_node: this.config.node.id }, this._md());
    this.watchCall = call;
    call.on('data', (ev) => this.emit(ev.type, P(ev.data_json)));
    call.on('error', () => {});
    call.on('end', () => { if (this.watchCall === call) { this.watchCall = null; setTimeout(() => { if (!this.watchCall) this.watch(); }, 2000); } });
  }
  unwatch() { const c = this.watchCall; this.watchCall = null; if (c) try { c.cancel(); } catch {} }
}

class RuntimeFacade extends EventEmitter {
  constructor(local, remote) {
    super();
    this.setMaxListeners(1000);
    this.local = local; this.remote = remote;
    for (const t of ['instance', 'activity', 'worker']) {
      local.on(t, (d) => { if (local.active) this.emit(t, d); });
      remote.on(t, (d) => { if (!local.active) this.emit(t, d); });
    }
  }
  get t() { return this.local.active ? this.local : this.remote; }
  start(...a) { return this.t.start(...a); }
  startAndWait(...a) { return this.t.startAndWait(...a); }
  control(...a) { return this.t.control(...a); }
  retry(...a) { return this.t.retry(...a); }
  waitForResponse(...a) { return this.t.waitForResponse(...a); }
  waitForFinish(...a) { return this.t.waitForFinish(...a); }
  async stats() { return this.t.stats(); }
}

module.exports = { MasterGrpcServer, RemoteRuntime, RuntimeFacade };
