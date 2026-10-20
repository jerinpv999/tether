'use strict';
const crypto = require('crypto');
const EventEmitter = require('events');

const FINAL = ['COMPLETED', 'FAILED', 'STOPPED', 'TERMINATED'];
const LIVE = ['PENDING', 'RUNNING', 'WAITING', 'PAUSED'];
const INSTANCE_ID_RE = /^[a-zA-Z0-9_.:-]{1,128}$/;

const J = (v) => (v === undefined ? '' : JSON.stringify(v));
const P = (s, d = null) => { if (!s) return d; try { return JSON.parse(s); } catch { return d; } };

class HttpError extends Error { constructor(status, msg) { super(msg); this.status = status; } }

/**
 * Leader runtime. Only the elected leader master runs this "active"; followers
 * forward calls to it (see remote.js).
 *
 *  - keeps the registry of attached workers (capacity, tags, draining)
 *  - schedules QUEUED instances onto workers (least-loaded worker whose tags match
 *    the workflow's settings.workerTags)
 *  - drives every instance's lifecycle through its worker (spawn / start / control / kill)
 *  - on becoming leader, re-creates entries for instances that are still running
 *    on workers and waits `workerLostGraceMs` for their workers to re-attach and
 *    adopt them; otherwise applies cluster.onWorkerLost (fail | restart)
 */
class Runtime extends EventEmitter {
  constructor({ store, logger, gateway, config }) {
    super();
    this.setMaxListeners(1000);
    this.store = store;
    this.log = logger;
    this.gateway = gateway;
    this.config = config;
    this.nodeId = config.node.id;
    this.active = false;
    this.term = 0;
    this._reset();
  }

  _reset() {
    this.workers = new Map();        // workerId -> worker record
    this.procs = new Map();          // instanceId -> entry
    this.queue = [];                 // [{ instanceId, tags }]
    this.responseWaiters = new Map();// instanceId -> [fn]
    this.responseBuffer = new Map(); // instanceId -> { resp, at } (response arrived before anyone waited)
    this.finishWaiters = new Map();  // instanceId -> [fn]
    this.locks = new Map();
  }

  // ───────────── leadership ─────────────
  async activate(term) {
    if (this.active) return;
    this._reset();
    this.active = true;
    this.term = term;
    this.log.info(`leader runtime active (term ${term})`);
    const { items } = await this.store.listInstances({ status: ['QUEUED', ...LIVE].join(','), limit: 100000 });
    let queued = 0, orphaned = 0;
    for (const inst of items.reverse()) {
      if (inst.status === 'QUEUED' || (inst.status === 'PENDING' && !inst.workerId)) {
        if (inst.status !== 'QUEUED') await this.store.saveInstance({ ...inst, status: 'QUEUED' });
        this.queue.push({ instanceId: inst.instanceId, tags: inst.definition?.settings?.workerTags || [] });
        queued++;
      } else {
        // Running somewhere: wait for its worker to re-attach and claim it.
        const e = this._entry(inst);
        this.procs.set(inst.instanceId, e);
        this._orphan(e, `awaiting worker ${inst.workerId}`);
        orphaned++;
      }
    }
    if (queued || orphaned) this.log.info(`takeover: ${queued} queued, ${orphaned} running instance(s) awaiting their workers`);
    this._pumpSoon();
  }

  deactivate(reason) {
    if (!this.active) return;
    this.active = false;
    this.log.warn(`leader runtime inactive: ${reason}`);
    for (const w of this.workers.values()) { try { w.call.write({ redirect: { leader_id: '', leader_address: '', reason: 'leadership lost' } }); w.call.end(); } catch {} }
    for (const e of this.procs.values()) { clearTimeout(e.orphanTimer); clearTimeout(e.killTimer); for (const s of e.subs) this.gateway.unsubscribe(s).catch(() => {}); }
    for (const [id, ws] of this.responseWaiters) for (const f of ws) f({ statusCode: 503, body: { instanceId: id, error: 'leadership changed; poll the instance status' } });
    for (const [id, ws] of this.finishWaiters) for (const f of ws) f({ instanceId: id, status: 'UNKNOWN', error: 'leadership changed' });
    this._reset();
  }

  async shutdown() { this.deactivate('shutdown'); }

  // ───────────── workers ─────────────
  /** Called by the WorkerLink service for an authenticated Attach stream. */
  onWorkerAttach(call, leaderAddress) {
    if (!this.active) {
      const l = leaderAddress();
      call.write({ redirect: { leader_id: l?.nodeId || '', leader_address: l?.grpcAddress || '', reason: 'not the leader' } });
      call.end();
      return;
    }
    let w = null;
    let pendingRegister = null;
    call.on('data', (m) => {
      if (!w) {
        if (m.body !== 'register' || pendingRegister) return;
        pendingRegister = this._registerWorker(m.worker_id, m.register, call).then((rec) => { w = rec; }).catch((e) => {
          this.log.error(`worker ${m.worker_id} registration failed: ${e.message}`);
          try { call.end(); } catch {}
        });
        return;
      }
      if (this.workers.get(w.id) !== w) return; // replaced by a newer connection
      this._onWorkerMessage(w, m).catch((e) => this.log.error(`worker ${w.id} message failed: ${e.message}`));
    });
    const gone = (why) => { if (w) this._workerGone(w, why); };
    call.on('error', (e) => gone(e.details || e.message));
    call.on('end', () => { gone('stream ended'); try { call.end(); } catch {} });
  }

  async _registerWorker(id, reg, call) {
    const old = this.workers.get(id);
    if (old) { old.replaced = true; try { old.call.end(); } catch {} }
    const w = {
      id, call, tags: reg.tags || [], capacity: reg.capacity || 1, address: reg.grpc_address, httpUrl: reg.http_url,
      version: reg.version, hostname: reg.hostname, connectedAt: Date.now(), lastBeat: Date.now(), running: 0, draining: false, rss: 0, load: 0,
    };
    this.workers.set(id, w);
    let adopted = 0, killed = 0;
    for (const rc of reg.running || []) {
      const entry = this.procs.get(rc.instance_id);
      const inst = await this.store.getInstance(rc.instance_id);
      if (!entry || !inst || FINAL.includes(inst.status) || (entry.workerId && entry.workerId !== id)) {
        call.write({ kill: { instance_id: rc.instance_id, signal: 'SIGKILL' } }); killed++;
        continue;
      }
      clearTimeout(entry.orphanTimer); entry.orphanTimer = null;
      entry.workerId = id; entry.pid = rc.pid; entry.orphan = false;
      for (const { id: subId, spec } of P(rc.subscriptions_json, [])) {
        try { await this.gateway.subscribe(spec, this._eventHandler(entry, subId, spec), subId); entry.subs.add(subId); }
        catch (e) { this.log.warn(`could not restore subscription ${subId}: ${e.message}`, { instanceId: entry.instanceId }); }
      }
      if (!rc.started && inst.status === 'PENDING') this._sendStart(entry, inst);
      adopted++;
    }
    call.write({ registered: { leader_id: this.nodeId, term: this.term } });
    // deliver whatever queued up for adopted instances while their worker was away
    for (const e of this.procs.values()) if (e.workerId === id && e.outbox.length) { const box = e.outbox; e.outbox = []; for (const m of box) this._send(e, m); }
    this.log.info(`worker ${id} attached (${w.hostname}, capacity ${w.capacity}, tags [${w.tags.join(', ')}])${adopted ? `, adopted ${adopted}` : ''}${killed ? `, killed ${killed} unknown` : ''}`);
    this.emit('worker', this._workerInfo(w));
    this._pumpSoon();
    return w;
  }

  _workerGone(w, why) {
    if (this.workers.get(w.id) !== w || !this.active) return;
    this.workers.delete(w.id);
    const mine = [...this.procs.values()].filter((e) => e.workerId === w.id && !e.finalized);
    this.log.warn(`worker ${w.id} disconnected (${why}); ${mine.length} instance(s) wait ${this.config.cluster.workerLostGraceMs} ms for it to re-attach`);
    for (const e of mine) this._orphan(e, `worker ${w.id} disconnected`);
    this.emit('worker', { id: w.id, connected: false });
  }

  _orphan(entry, why) {
    entry.orphan = true;
    clearTimeout(entry.orphanTimer);
    entry.orphanTimer = setTimeout(() => this._onWorkerLost(entry, why).catch((e) => this.log.error(e.message)), this.config.cluster.workerLostGraceMs);
  }

  async _onWorkerLost(entry, why) {
    if (!this.active || entry.finalized || !entry.orphan) return;
    const inst = await this.store.getInstance(entry.instanceId);
    if (!inst || FINAL.includes(inst.status)) { this.procs.delete(entry.instanceId); return; }
    if (this.config.cluster.onWorkerLost === 'restart' && !entry.terminating) {
      this.procs.delete(entry.instanceId);
      for (const s of entry.subs) this.gateway.unsubscribe(s).catch(() => {});
      const restarts = (inst.restarts || 0) + 1;
      await this._update(entry.instanceId, { status: 'QUEUED', workerId: null, pid: null, currentNodes: [], restarts, error: `restarted after ${why}` });
      this.log.warn(`worker lost (${why}) – restarting from the beginning (restart #${restarts})`, { instanceId: entry.instanceId, workflowId: entry.workflowId });
      this.queue.unshift({ instanceId: entry.instanceId, tags: inst.definition?.settings?.workerTags || [] });
      this._pumpSoon();
    } else {
      await this._finalize(entry, { status: entry.terminating ? 'TERMINATED' : 'FAILED', error: entry.terminating ? entry.reason : `worker lost: ${why}` });
      this.procs.delete(entry.instanceId);
    }
  }

  async _onWorkerMessage(w, m) {
    switch (m.body) {
      case 'heartbeat': {
        const h = m.heartbeat;
        const wasDraining = w.draining;
        Object.assign(w, { lastBeat: Date.now(), running: h.running, capacity: h.capacity || w.capacity, draining: h.draining, load: h.load, rss: h.rss });
        if (h.draining && !wasDraining) this.log.info(`worker ${w.id} is draining – no new work`);
        return;
      }
      case 'spawned': {
        const e = this.procs.get(m.spawned.instance_id);
        if (!e || e.workerId !== w.id) return;
        e.pid = m.spawned.pid;
        await this._update(e.instanceId, { pid: e.pid });
        this.log.info(`started on ${w.id} (pid ${e.pid})`, { instanceId: e.instanceId, workflowId: e.workflowId });
        return;
      }
      case 'exited': return this._onExit(w, m.exited);
      case 'child': {
        const e = this.procs.get(m.child.instance_id);
        if (!e || e.workerId !== w.id) return;
        return this._onChildMessage(e, m.child.msg);
      }
      default:
    }
  }

  async _onExit(w, x) {
    const e = this.procs.get(x.instance_id);
    if (!e || e.workerId !== w.id) return;
    e.exited = true;
    clearTimeout(e.killTimer);
    if (x.code === -1 && x.error && !e.started) {
      // worker refused (capacity / draining): put it back in the queue
      this.procs.delete(e.instanceId);
      await this._update(e.instanceId, { status: 'QUEUED', workerId: null });
      this.queue.unshift({ instanceId: e.instanceId, tags: e.tags });
      this.log.warn(`worker ${w.id} refused instance (${x.error}) – requeued`, { instanceId: e.instanceId });
    } else {
      if (!e.finalized) {
        const status = e.terminating ? 'TERMINATED' : 'FAILED';
        await this._finalize(e, { status, error: e.terminating ? (e.reason || 'terminated') : `workflow process exited unexpectedly (code ${x.code}, signal ${x.signal || 'none'})` });
      }
      this.procs.delete(e.instanceId);
    }
    this._pumpSoon();
  }

  _workerInfo(w) {
    const assigned = [...this.procs.values()].filter((e) => e.workerId === w.id).length;
    return { id: w.id, connected: true, hostname: w.hostname, address: w.address, httpUrl: w.httpUrl, tags: w.tags, capacity: w.capacity,
      assigned, running: w.running, draining: w.draining, load: w.load, rss: w.rss, connectedAt: w.connectedAt, lastBeat: w.lastBeat, version: w.version };
  }

  // ───────────── scheduling ─────────────
  _pumpSoon() { if (!this.pumpScheduled) { this.pumpScheduled = true; setImmediate(() => { this.pumpScheduled = false; this._pump(); }); } }

  _pump() {
    if (!this.active || !this.queue.length) return;
    const load = new Map();
    for (const e of this.procs.values()) if (e.workerId) load.set(e.workerId, (load.get(e.workerId) || 0) + 1);
    const rest = [];
    for (const q of this.queue) {
      let best = null, bestFree = 0;
      for (const w of this.workers.values()) {
        if (w.draining || w.replaced) continue;
        if (q.tags.length && !q.tags.every((t) => w.tags.includes(t))) continue;
        const free = w.capacity - Math.max(load.get(w.id) || 0, 0);
        if (free > bestFree || (free === bestFree && best && free > 0 && (w.load || 0) < (best.load || 0))) { best = w; bestFree = free; }
      }
      if (!best || bestFree <= 0) { rest.push(q); continue; }
      load.set(best.id, (load.get(best.id) || 0) + 1);
      this._launch(q, best);
    }
    this.queue = rest;
  }

  _entry(inst) {
    return { instanceId: inst.instanceId, workflowId: inst.workflowId, workerId: inst.workerId || null, pid: inst.pid || null, tags: inst.definition?.settings?.workerTags || [],
      subs: new Set(), outbox: [], finalized: false, terminating: false, started: inst.status !== 'PENDING' && inst.status !== 'QUEUED', exited: false, orphan: false };
  }

  _launch(q, w) {
    const entry = this._entry({ instanceId: q.instanceId, workflowId: null, status: 'QUEUED', workerId: w.id, definition: { settings: { workerTags: q.tags } } });
    this.procs.set(q.instanceId, entry);
    (async () => {
      const inst = await this.store.getInstance(q.instanceId);
      if (!inst || inst.status !== 'QUEUED') { this.procs.delete(q.instanceId); return; }
      entry.workflowId = inst.workflowId;
      await this._update(q.instanceId, { status: 'PENDING', workerId: w.id, startedAt: Date.now() });
      w.call.write({ spawn: { instance_id: q.instanceId, max_old_space_mb: inst.definition?.settings?.maxOldSpaceMb || this.config.runtime.defaultMaxOldSpaceMb } });
    })().catch((e) => { this.log.error(`launch failed: ${e.message}`, { instanceId: q.instanceId }); this.procs.delete(q.instanceId); });
  }

  // ───────────── start ─────────────
  async start(workflowId, opts = {}) {
    this._assertActive();
    const wf = await this.store.getWorkflow(workflowId);
    if (!wf) throw new HttpError(404, `workflow "${workflowId}" not found`);
    if (wf.enabled === false) throw new HttpError(409, `workflow "${workflowId}" is disabled`);
    const instanceId = opts.instanceId ? String(opts.instanceId) : crypto.randomUUID();
    if (!INSTANCE_ID_RE.test(instanceId)) throw new HttpError(400, 'instanceId may contain letters, digits and _ . : - (max 128)');
    if (this.procs.has(instanceId) || this.queue.some((q) => q.instanceId === instanceId) || (await this.store.getInstance(instanceId))) {
      throw new HttpError(409, `instance "${instanceId}" already exists`);
    }
    const parent = opts.parentInstanceId ? await this.store.getInstance(opts.parentInstanceId) : null;
    const now = Date.now();
    const inst = {
      instanceId, workflowId: wf.id, workflowName: wf.name || wf.id, workflowVersion: wf.version || 1,
      status: 'QUEUED', input: opts.input ?? {}, output: null, error: null, vars: {}, currentNodes: [],
      trigger: opts.trigger || { type: 'api' },
      parentInstanceId: parent?.instanceId || null,
      rootInstanceId: parent ? (parent.rootInstanceId || parent.instanceId) : instanceId,
      parentWait: opts.parentWait || null,  // persisted so a new leader can still answer the parent
      retryOf: opts.retryOf || null, restarts: 0,
      workerId: null, pid: null, acceptedBy: opts.acceptedBy || this.nodeId,
      createdAt: now, startedAt: null, updatedAt: now, endedAt: null,
      definition: wf,
    };
    await this.store.saveInstance(inst);
    await this.store.saveRequest({ instanceId, workflowId: wf.id, receivedAt: now, trigger: opts.trigger || { type: 'api' }, ...(opts.request || {}), body: opts.request?.body ?? opts.input ?? {} });
    this.log.info(`instance queued (trigger: ${inst.trigger.type})`, { instanceId, workflowId: wf.id });
    this.queue.push({ instanceId, tags: wf.settings?.workerTags || [] });
    this._pumpSoon();
    if (!this.workers.size) this.log.warn('no worker attached – instance stays QUEUED until one connects', { instanceId, workflowId: wf.id });
    return this._public(inst);
  }

  async startAndWait(workflowId, opts, wait, timeoutMs) {
    const inst = await this.start(workflowId, opts);
    if (!wait) return { instance: inst };
    if (wait === 'finish') return { instance: inst, finished: await this.waitForFinish(inst.instanceId, timeoutMs) };
    return { instance: inst, response: await this.waitForResponse(inst.instanceId, timeoutMs) };
  }

  async retry(instanceId, newId) {
    this._assertActive();
    const old = await this.store.getInstance(instanceId);
    if (!old) throw new HttpError(404, 'instance not found');
    if (!FINAL.includes(old.status)) throw new HttpError(409, `instance is ${old.status}`);
    const req = await this.store.getRequest(instanceId);
    return this.start(old.workflowId, {
      instanceId: newId, input: old.input, trigger: { ...(old.trigger || {}), retry: true },
      parentInstanceId: old.parentInstanceId, retryOf: instanceId,
      request: req ? { ...req, instanceId: undefined } : undefined,
    });
  }

  _assertActive() { if (!this.active) throw new HttpError(503, 'this master is not the leader'); }

  // ───────────── child traffic (relayed by workers) ─────────────
  _send(entry, msg) {
    const w = entry.workerId && this.workers.get(entry.workerId);
    if (!w || entry.orphan) { entry.outbox.push(msg); return false; }
    try { w.call.write({ to_child: { instance_id: entry.instanceId, msg } }); return true; } catch { entry.outbox.push(msg); return false; }
  }

  _kill(entry, signal = 'SIGKILL') {
    const w = entry.workerId && this.workers.get(entry.workerId);
    if (w) try { w.call.write({ kill: { instance_id: entry.instanceId, signal } }); } catch {}
  }

  _sendStart(entry, inst) {
    entry.started = true;
    this._send(entry, { start: {
      definition_json: J(inst.definition), input_json: J(inst.input),
      meta_json: J({ instanceId: inst.instanceId, workflowId: inst.workflowId, workflowName: inst.workflowName, parentInstanceId: inst.parentInstanceId, rootInstanceId: inst.rootInstanceId, trigger: inst.trigger, startedAt: inst.startedAt, workerId: entry.workerId }),
    } });
  }

  async _onChildMessage(entry, msg) {
    const { instanceId, workflowId } = entry;
    switch (msg.body) {
      case 'hello': {
        const inst = await this.store.getInstance(instanceId);
        if (!inst || inst.status !== 'PENDING') return; // already started (re-attach) – ignore
        this._sendStart(entry, inst);
        for (const c of inst.pendingControls || []) this._send(entry, { control: c });
        if (inst.pendingControls?.length) await this._update(instanceId, { pendingControls: [] });
        break;
      }
      case 'status': {
        const s = msg.status;
        const patch = { status: s.status, currentNodes: s.current_nodes || [] };
        if (s.context_json) patch.vars = P(s.context_json, {});
        if (FINAL.includes(s.status)) {
          if (entry.terminating && s.status !== 'TERMINATED') patch.status = 'TERMINATED';
          return this._finalize(entry, { ...patch, output: P(s.output_json), error: s.error || null });
        }
        await this._update(instanceId, patch);
        break;
      }
      case 'activity': {
        const a = msg.activity;
        await this.store.saveActivity({
          instanceId, workflowId, workerId: entry.workerId,
          activityId: a.activity_id, nodeId: a.node_id, nodeType: a.node_type, nodeName: a.node_name,
          status: a.status, attempt: a.attempt, port: a.port || null,
          input: a.input_json ? P(a.input_json) : undefined,
          output: a.output_json ? P(a.output_json) : undefined,
          error: a.error || null, startedAt: a.started_at || undefined, endedAt: a.ended_at || null,
        });
        this.emit('activity', { instanceId, ...a });
        break;
      }
      case 'log': {
        const l = msg.log;
        this.log.log(l.level || 'info', l.message, { instanceId, workflowId, nodeId: l.node_id || null, source: `workflow@${entry.workerId}`, data: l.data_json ? P(l.data_json) : undefined });
        break;
      }
      case 'request': return this._onRequest(entry, msg.request);
      case 'response': {
        this._resolveResponse(instanceId, { statusCode: msg.response.status_code || 200, body: P(msg.response.body_json), headers: P(msg.response.headers_json, {}) }, true);
        break;
      }
      default:
    }
  }

  _eventHandler(entry, subId, spec) {
    return (payload, meta) => this._send(entry, { event: { subscription_id: subId, payload_json: J(payload), source: spec.source, topic: meta.topic || meta.routingKey || meta.queue || '' } });
  }

  async _onRequest(entry, req) {
    const reply = (ok, result, error) => this._send(entry, { reply: { request_id: req.request_id, ok, result_json: J(result), error: error || '' } });
    const p = P(req.payload_json, {});
    try {
      switch (req.op) {
        case 'rabbit.publish': return reply(true, await this.gateway.publishRabbit(p));
        case 'mqtt.publish': return reply(true, await this.gateway.publishMqtt(p));
        case 'db.write': return reply(true, await this.gateway.dbWrite(p));
        case 'event.subscribe': {
          const subId = crypto.randomUUID();
          await this.gateway.subscribe(p, this._eventHandler(entry, subId, p), subId);
          entry.subs.add(subId);
          return reply(true, { subscriptionId: subId });
        }
        case 'event.unsubscribe': {
          entry.subs.delete(p.subscriptionId);
          await this.gateway.unsubscribe(p.subscriptionId);
          return reply(true, {});
        }
        case 'workflow.start': {
          const child = await this.start(p.workflowId, {
            instanceId: p.instanceId || undefined, input: p.input, parentInstanceId: entry.instanceId,
            trigger: { type: 'subworkflow', parentInstanceId: entry.instanceId, nodeId: p.nodeId },
            parentWait: p.wait ? { parentId: entry.instanceId, requestId: req.request_id } : null,
          });
          if (!p.wait) return reply(true, { instanceId: child.instanceId, status: child.status });
          return; // answered in _finalize of the child
        }
        case 'workflow.cancel': {
          await this.control(p.instanceId, 'TERMINATE', { reason: p.reason || 'cancelled by parent' }).catch(() => {});
          return reply(true, {});
        }
        default: return reply(false, null, `unknown op ${req.op}`);
      }
    } catch (e) {
      reply(false, null, e.message);
    }
  }

  // ───────────── state ─────────────
  async _update(instanceId, patch) {
    if (!this.active) return null; // fencing: a deposed leader stops writing
    const prev = this.locks.get(instanceId) || Promise.resolve();
    const run = prev.then(async () => {
      const inst = await this.store.getInstance(instanceId);
      if (!inst) return null;
      if (FINAL.includes(inst.status) && patch.status && !FINAL.includes(patch.status)) return inst;
      Object.assign(inst, patch, { updatedAt: Date.now() });
      await this.store.saveInstance(inst);
      this.emit('instance', this._public(inst));
      return inst;
    });
    this.locks.set(instanceId, run.catch(() => {}));
    const r = await run;
    if (this.locks.get(instanceId) === run) this.locks.delete(instanceId);
    return r;
  }

  async _finalize(entry, patch) {
    if (entry.finalized) return;
    entry.finalized = true;
    clearTimeout(entry.orphanTimer);
    const { instanceId } = entry;
    for (const s of entry.subs) await this.gateway.unsubscribe(s).catch(() => {});
    entry.subs.clear();
    const inst = await this._update(instanceId, { ...patch, currentNodes: [], endedAt: Date.now() });
    if (!inst) return;
    this.log.log(inst.status === 'COMPLETED' ? 'info' : 'warn', `instance ${inst.status}${inst.error ? ': ' + inst.error : ''} (${inst.endedAt - (inst.startedAt || inst.createdAt)} ms on ${entry.workerId || '-'})`, { instanceId, workflowId: inst.workflowId });

    this._resolveResponse(instanceId, {
      statusCode: inst.status === 'COMPLETED' ? 200 : inst.status === 'FAILED' ? 500 : 409,
      body: { instanceId, status: inst.status, output: inst.output, error: inst.error },
    }, false);
    for (const r of this.finishWaiters.get(instanceId) || []) r(this._public(inst));
    this.finishWaiters.delete(instanceId);

    // A parent waiting on this sub-workflow (persisted, survives leader failover)
    if (inst.parentWait) {
      const parent = this.procs.get(inst.parentWait.parentId);
      if (parent) this._send(parent, { reply: { request_id: inst.parentWait.requestId, ok: inst.status === 'COMPLETED', result_json: J({ instanceId, status: inst.status, output: inst.output }), error: inst.error || (inst.status === 'COMPLETED' ? '' : `sub-workflow ${inst.status}`) } });
    }
    // Cascade stop/terminate/failure to running sub-workflows
    if (['STOPPED', 'TERMINATED', 'FAILED'].includes(inst.status)) {
      const kids = await this.store.listInstances({ parentInstanceId: instanceId, status: ['QUEUED', ...LIVE].join(','), limit: 1000 });
      for (const k of kids.items) this.control(k.instanceId, 'TERMINATE', { reason: `parent ${inst.status}` }).catch(() => {});
    }
    if (!entry.exited) entry.killTimer = setTimeout(() => { if (!entry.exited) this._kill(entry); }, this.config.runtime.terminateGraceMs);
    this.emit('finished', this._public(inst));
    this._pumpSoon();
  }

  _resolveResponse(instanceId, resp, fromNode) {
    const ws = this.responseWaiters.get(instanceId);
    if (!ws) {
      // Response node fired before anyone waited (e.g. forwarded from a follower) – keep it briefly.
      if (fromNode) { this.responseBuffer.set(instanceId, { resp, at: Date.now() }); this._gcBuffer(); }
      return;
    }
    this.responseWaiters.delete(instanceId);
    for (const r of ws) r(resp);
  }

  _gcBuffer() {
    const cut = Date.now() - 120000;
    for (const [k, v] of this.responseBuffer) if (v.at < cut) this.responseBuffer.delete(k);
  }

  waitForResponse(instanceId, timeoutMs) {
    const buffered = this.responseBuffer.get(instanceId);
    if (buffered) { this.responseBuffer.delete(instanceId); return Promise.resolve(buffered.resp); }
    return new Promise((resolve) => {
      (async () => {
        const inst = await this.store.getInstance(instanceId);
        if (inst && FINAL.includes(inst.status)) return resolve({ statusCode: inst.status === 'COMPLETED' ? 200 : 500, body: { instanceId, status: inst.status, output: inst.output, error: inst.error } });
        const list = this.responseWaiters.get(instanceId) || [];
        let done = false;
        const fn = (r) => { if (!done) { done = true; clearTimeout(t); resolve(r); } };
        list.push(fn); this.responseWaiters.set(instanceId, list);
        const t = setTimeout(() => {
          const l = this.responseWaiters.get(instanceId); if (l) { l.splice(l.indexOf(fn), 1); if (!l.length) this.responseWaiters.delete(instanceId); }
          fn({ statusCode: 202, body: { instanceId, status: 'RUNNING', message: 'workflow still running; poll GET /api/instances/' + instanceId } });
        }, timeoutMs);
      })().catch((e) => resolve({ statusCode: 500, body: { instanceId, error: e.message } }));
    });
  }

  async waitForFinish(instanceId, timeoutMs) {
    const inst = await this.store.getInstance(instanceId);
    if (!inst) throw new HttpError(404, 'instance not found');
    if (FINAL.includes(inst.status) || !this.active) return this._public(inst);
    return new Promise((resolve) => {
      const list = this.finishWaiters.get(instanceId) || [];
      const t = setTimeout(async () => resolve(this._public(await this.store.getInstance(instanceId))), timeoutMs);
      list.push((i) => { clearTimeout(t); resolve(i); });
      this.finishWaiters.set(instanceId, list);
    });
  }

  // ───────────── control ─────────────
  async control(instanceId, action, { signalName, payload, reason } = {}) {
    this._assertActive();
    action = String(action).toUpperCase();
    if (!['STOP', 'TERMINATE', 'PAUSE', 'RESUME', 'SIGNAL'].includes(action)) throw new HttpError(400, `unknown action ${action}`);
    const inst = await this.store.getInstance(instanceId);
    if (!inst) throw new HttpError(404, 'instance not found');
    if (FINAL.includes(inst.status)) throw new HttpError(409, `instance already ${inst.status}`);
    if (action === 'SIGNAL' && !signalName) throw new HttpError(400, 'signal name required');
    const ctl = { action, signal_name: signalName || '', payload_json: J(payload), reason: reason || '' };
    this.log.info(`control ${action}${signalName ? ' ' + signalName : ''}${reason ? ' (' + reason + ')' : ''}`, { instanceId, workflowId: inst.workflowId });
    const entry = this.procs.get(instanceId);

    if (!entry) { // queued, not on any worker yet
      if (action === 'STOP' || action === 'TERMINATE') {
        this.queue = this.queue.filter((q) => q.instanceId !== instanceId);
        const i = await this._update(instanceId, { status: action === 'STOP' ? 'STOPPED' : 'TERMINATED', error: reason || null, endedAt: Date.now() });
        this._resolveResponse(instanceId, { statusCode: 409, body: { instanceId, status: i.status } }, false);
        for (const r of this.finishWaiters.get(instanceId) || []) r(this._public(i));
        this.finishWaiters.delete(instanceId);
        return this._public(i);
      }
      await this._update(instanceId, { pendingControls: [...(inst.pendingControls || []), ctl] });
      return this._public(await this.store.getInstance(instanceId));
    }

    if (!entry.started) { // spawned but not yet started: deliver after StartCommand
      if (action === 'TERMINATE') { entry.terminating = true; entry.reason = reason || 'terminated by user'; this._kill(entry); }
      else await this._update(instanceId, { pendingControls: [...(inst.pendingControls || []), ctl] });
      return this._public(await this.store.getInstance(instanceId));
    }

    if (action === 'TERMINATE') {
      entry.terminating = true; entry.reason = reason || 'terminated by user';
      this._send(entry, { control: ctl });
      clearTimeout(entry.killTimer);
      entry.killTimer = setTimeout(() => { if (!entry.exited) { this.log.warn('grace period over – SIGKILL', { instanceId }); this._kill(entry); } }, this.config.runtime.terminateGraceMs);
      if (entry.orphan) await this._finalize(entry, { status: 'TERMINATED', error: entry.reason }); // worker unreachable: record it now
    } else if (action === 'STOP') {
      this._send(entry, { control: ctl });
      clearTimeout(entry.killTimer);
      entry.killTimer = setTimeout(() => { if (!entry.exited) { entry.terminating = true; entry.reason = 'stop timed out'; this._kill(entry); } }, this.config.runtime.terminateGraceMs * 5);
    } else {
      this._send(entry, { control: ctl });
    }
    return this._public(await this.store.getInstance(instanceId));
  }

  stats() {
    return {
      leader: this.nodeId, term: this.term, active: this.active,
      running: this.procs.size, queued: this.queue.length,
      workers: [...this.workers.values()].map((w) => this._workerInfo(w)),
      orphaned: [...this.procs.values()].filter((e) => e.orphan).map((e) => ({ instanceId: e.instanceId, workerId: e.workerId })),
    };
  }

  _public(inst) {
    if (!inst) return inst;
    const { definition, ...rest } = inst;
    return rest;
  }
}

module.exports = { Runtime, HttpError, FINAL, LIVE, INSTANCE_ID_RE };
