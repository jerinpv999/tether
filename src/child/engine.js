'use strict';
const crypto = require('crypto');
const { evalExpr, runCode, template, parseDuration, plain } = require('./expr');
const { defaultsFor } = require('../shared/nodeTypes');

const MAX_RECORD = 64 * 1024;

class Aborted extends Error { constructor(reason) { super(`aborted: ${reason}`); this.aborted = true; } }

/** setTimeout that rejects when the signal aborts. Long waits are chunked (setTimeout max ≈ 24.8 days). */
function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(new Aborted(signal.reason));
    let t;
    const end = Date.now() + Math.max(0, ms);
    const tick = () => { const left = end - Date.now(); if (left <= 0) { signal.removeEventListener('abort', onAbort); resolve(); } else t = setTimeout(tick, Math.min(left, 2 ** 31 - 1)); };
    const onAbort = () => { clearTimeout(t); reject(new Aborted(signal.reason)); };
    signal.addEventListener('abort', onAbort, { once: true });
    tick();
  });
}

const clip = (v) => {
  if (v === undefined) return undefined;
  try { const s = JSON.stringify(v); return s && s.length > MAX_RECORD ? { truncated: true, preview: s.slice(0, 2000) } : JSON.parse(s); } catch { return String(v); }
};

/**
 * Token-based graph executor. Each outgoing edge spawns a branch that runs
 * concurrently; the workflow finishes when an End node fires, a node fails
 * without an `error` edge, a STOP/TERMINATE arrives, or every branch drains.
 */
class Engine {
  constructor({ definition, input, meta, bridge }) {
    this.def = definition;
    this.meta = meta;
    this.bridge = bridge;
    this.input = input;
    this.nodes = new Map(definition.nodes.map((n) => [n.id, { ...n, config: { ...defaultsFor(n.type), ...(n.config || {}) } }]));
    this.edges = new Map(); this.incoming = new Map();
    for (const e of definition.edges || []) {
      const port = e.port || 'out';
      if (!this.edges.has(e.from)) this.edges.set(e.from, []);
      this.edges.get(e.from).push({ to: e.to, port });
      this.incoming.set(e.to, (this.incoming.get(e.to) || 0) + 1);
    }
    this.vars = {};
    this.outputs = {};
    this.signals = {};
    this.ac = new AbortController();
    this.inflight = 0;
    this.current = new Map();   // nodeId -> active count
    this.waiting = new Map();   // nodeId -> waiting count
    this.joins = new Map();
    this.counter = 0;
    this.steps = 0;
    this.maxSteps = Number(definition.settings?.maxSteps) || 10000;
    this.paused = false; this.resumeWaiters = [];
    this.signalWaiters = new Map(); // name -> Set(fn)
    this.signalBuffer = new Map();  // name -> [payload]
    this.listeners = [...this.nodes.values()].filter((n) => n.type === 'signal' && n.config.mode === 'listen');
    this.finished = false;
    this.env = Object.fromEntries(Object.entries(process.env).filter(([k]) => k.startsWith('WF_')));
  }

  // ───────────── lifecycle ─────────────
  run() {
    return new Promise((resolve) => {
      this.resolve = resolve;
      const start = [...this.nodes.values()].find((n) => n.type === 'start');
      if (!start) return this.finish('FAILED', null, 'no Start node');
      this.bridge.log('info', `workflow started (${this.nodes.size} nodes)`);
      this.report('RUNNING', true);
      this.spawn(start.id);
    });
  }

  /** Resolves once every branch has unwound (so CANCELLED activities are reported before the final status). */
  drain(maxMs = 1000) {
    if (this.inflight === 0) return Promise.resolve();
    return new Promise((r) => { (this.drainWaiters = this.drainWaiters || []).push(r); setTimeout(r, maxMs).unref(); });
  }

  finish(status, output, error) {
    if (this.finished) return;
    this.finished = true;
    this.status = status;
    if (!this.ac.signal.aborted) this.ac.abort(status);
    for (const w of this.resumeWaiters) w();
    this.resolve({ status, output: clip(output === undefined ? this.vars : output), error: error || null, vars: clip(this.vars) });
  }

  spawn(nodeId, ctx) {
    if (this.finished) return;
    this.inflight++;
    this.exec(nodeId, ctx)
      .catch((e) => { if (!e.aborted) this.finish('FAILED', null, e.message); })
      .finally(() => {
        this.inflight--;
        if (this.inflight === 0) { for (const d of this.drainWaiters || []) d(); this.drainWaiters = []; }
        if (this.inflight === 0 && !this.finished) this.finish('COMPLETED');
        else this.report();
      });
  }

  // ───────────── control from master ─────────────
  control({ action, signal_name: name, payload_json, reason }) {
    const payload = payload_json ? JSON.parse(payload_json) : undefined;
    switch (action) {
      case 'STOP': this.bridge.log('warn', `stop requested${reason ? ': ' + reason : ''}`); return this.finish('STOPPED', undefined, reason || 'stopped by user');
      case 'TERMINATE': this.bridge.log('warn', `terminate requested${reason ? ': ' + reason : ''}`); return this.finish('TERMINATED', undefined, reason || 'terminated by user');
      case 'PAUSE': if (!this.paused) { this.paused = true; this.bridge.log('info', 'paused'); this.report('PAUSED', true); } return;
      case 'RESUME': if (this.paused) { this.paused = false; this.bridge.log('info', 'resumed'); const w = this.resumeWaiters; this.resumeWaiters = []; w.forEach((f) => f()); this.report(undefined, true); } return;
      case 'SIGNAL': return this.deliverSignal(name, payload);
      default: this.bridge.log('warn', `unknown control ${action}`);
    }
  }

  deliverSignal(name, payload) {
    this.signals[name] = payload === undefined ? true : payload;
    this.bridge.log('info', `signal "${name}" received`, null, payload);
    const waiters = this.signalWaiters.get(name);
    const listeners = this.listeners.filter((n) => n.config.signalName === name);
    if (waiters?.size) { const first = [...waiters][0]; waiters.delete(first); first(payload); }
    else if (!listeners.length) { const b = this.signalBuffer.get(name) || []; b.push(payload); this.signalBuffer.set(name, b); }
    for (const l of listeners) this.spawn(l.id, { signalPayload: payload });
  }

  async gate() {
    while (this.paused && !this.finished) await new Promise((r) => this.resumeWaiters.push(r));
    if (this.finished) throw new Aborted(this.status);
  }

  // ───────────── status reporting ─────────────
  report(status, force) {
    if (this.finished) return;
    if (status) this.status = status;
    const now = Date.now();
    if (!force && now - (this.lastReport || 0) < 150) {
      clearTimeout(this.reportTimer);
      this.reportTimer = setTimeout(() => this.report(undefined, true), 150);
      return;
    }
    this.lastReport = now;
    let st = this.paused ? 'PAUSED' : 'RUNNING';
    const active = [...this.current.keys()];
    if (!this.paused && active.length && active.every((id) => (this.waiting.get(id) || 0) >= (this.current.get(id) || 0))) st = 'WAITING';
    this.bridge.status({ status: st, current_nodes: active, context_json: JSON.stringify(clip(this.vars)) });
  }

  bump(map, id, d) { const v = (map.get(id) || 0) + d; if (v <= 0) map.delete(id); else map.set(id, v); }

  scope(extra = {}) {
    return {
      input: this.input, vars: this.vars, nodes: this.outputs, signals: this.signals,
      workflow: { instanceId: this.meta.instanceId, workflowId: this.meta.workflowId, name: this.meta.workflowName, parentInstanceId: this.meta.parentInstanceId, startedAt: this.meta.startedAt },
      trigger: this.meta.trigger, env: this.env,
      now: () => Date.now(), uuid: () => crypto.randomUUID(),
      ...extra,
    };
  }

  // ───────────── node execution ─────────────
  async exec(nodeId, ctx = {}) {
    await this.gate();
    const node = this.nodes.get(nodeId);
    if (!node) throw new Error(`edge points to missing node ${nodeId}`);
    if (++this.steps > this.maxSteps) throw new Error(`step limit ${this.maxSteps} exceeded (infinite loop?) – raise settings.maxSteps`);

    if (node.type === 'join') {
      const need = Number(node.config.count) || this.incoming.get(node.id) || 1;
      const got = (this.joins.get(node.id) || 0) + 1;
      if (got < need) { this.joins.set(node.id, got); return; }
      this.joins.set(node.id, 0);
    }

    const activityId = `${node.id}#${++this.counter}`;
    const startedAt = Date.now();
    const act = { activity_id: activityId, node_id: node.id, node_type: node.type, node_name: node.name || node.id, started_at: startedAt };
    const setWaiting = (on) => { this.bump(this.waiting, node.id, on ? 1 : -1); this.report(); };
    this.bump(this.current, node.id, 1);
    this.bridge.activity({ ...act, status: 'RUNNING', attempt: 1 });
    this.report();

    let port, output, endWith, attempt = 1;
    const retries = node.type === 'activity' ? Math.max(0, Number(node.config.retries) || 0) : 0;
    try {
      for (;;) {
        try {
          ({ port, output, endWith } = await this.runNode(node, { ...ctx, act, setWaiting, attempt }));
          break;
        } catch (e) {
          if (e.aborted || this.finished || attempt > retries) throw e;
          this.bridge.log('warn', `attempt ${attempt} failed: ${e.message} – retrying`, node.id);
          this.bridge.activity({ ...act, status: 'RETRYING', attempt, error: e.message });
          await sleep(Number(node.config.retryDelayMs) || 0, this.ac.signal);
          attempt++;
        }
      }
    } catch (e) {
      const aborted = e.aborted || this.finished;
      this.bridge.activity({ ...act, status: aborted ? 'CANCELLED' : 'FAILED', attempt, error: e.message, ended_at: Date.now() });
      this.bump(this.current, node.id, -1);
      if (aborted) throw new Aborted(this.status);
      const errEdges = (this.edges.get(node.id) || []).filter((x) => x.port === 'error');
      if (errEdges.length) {
        this.outputs[node.id] = { error: e.message };
        this.bridge.log('warn', `failed, following error branch: ${e.message}`, node.id);
        for (const ed of errEdges) this.spawn(ed.to);
        return;
      }
      this.bridge.log('error', `failed: ${e.message}`, node.id);
      throw e;
    }

    this.outputs[node.id] = output;
    if (node.config.outputVar && output !== undefined) this.vars[node.config.outputVar] = output;
    this.bridge.activity({ ...act, status: 'COMPLETED', attempt, output_json: output === undefined ? '' : JSON.stringify(clip(output)), port: port || '', ended_at: Date.now() });
    this.bump(this.current, node.id, -1);
    if (endWith) return this.finish(endWith.status, endWith.output, endWith.error);
    if (this.finished) return;
    for (const ed of (this.edges.get(node.id) || []).filter((x) => x.port === (port || 'out'))) this.spawn(ed.to);
  }

  /** Executes one node and returns { port, output }. */
  async runNode(node, ctx) {
    const c = node.config;
    const signal = this.ac.signal;
    const sc = () => this.scope(ctx.extraScope);
    const recordInput = (input) => this.bridge.activity({ ...ctx.act, status: 'RUNNING', attempt: ctx.attempt, input_json: JSON.stringify(clip(input)) });

    switch (node.type) {
      case 'start': {
        Object.assign(this.vars, plain(template(c.initVars || {}, sc())));
        return { port: 'out', output: this.input };
      }

      case 'end': {
        const output = plain(template(c.output, sc()));
        const err = c.status === 'FAILED' ? String(template(c.error || 'workflow ended with FAILED', sc())) : null;
        return { port: null, output, endWith: { status: c.status || 'COMPLETED', output, error: err } };
      }

      case 'function': {
        const logs = [];
        const log = (...a) => { const m = a.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))).join(' '); logs.push(m); this.bridge.log('info', m, node.id); };
        const out = await withTimeout(runCode(c.code || '', { ...sc(), log, console: { log, info: log, warn: log, error: log }, fetch, setTimeout, sleep: (ms) => sleep(ms, signal) }, Number(c.timeoutMs) || 1000), 0, signal);
        return { port: 'out', output: plain(out) };
      }

      case 'decision': {
        const v = !!evalExpr(c.condition || 'false', sc());
        return { port: v ? 'true' : 'false', output: { result: v } };
      }

      case 'switch': {
        const v = evalExpr(c.expression || 'undefined', sc());
        const hit = (c.cases || []).map(String).find((k) => k === String(v));
        return { port: hit ?? 'default', output: { value: plain(v), matched: hit ?? 'default' } };
      }

      case 'join': return { port: 'out', output: { joined: true } };

      case 'response': {
        const body = plain(template(c.body, sc()));
        const headers = plain(template(c.headers || {}, sc()));
        const statusCode = Number(template(c.statusCode, sc())) || 200;
        this.bridge.response({ status_code: statusCode, body_json: JSON.stringify(body ?? null), headers_json: JSON.stringify(headers || {}) });
        return { port: 'out', output: { statusCode, body } };
      }

      case 'delay': {
        const ms = parseDuration(template(c.duration, sc()));
        recordInput({ ms });
        ctx.setWaiting(true);
        try { await sleep(ms, signal); } finally { ctx.setWaiting(false); }
        return { port: 'out', output: { delayedMs: ms } };
      }

      case 'timer': {
        if (c.mode === 'interval') {
          const every = Math.max(10, Number(c.intervalMs) || 1000), times = Math.max(1, Number(c.repeat) || 1);
          ctx.setWaiting(true);
          try {
            for (let i = 1; i < times; i++) { // ticks 1..n-1 spawn the branch; the last tick continues normally
              await sleep(every, signal); await this.gate();
              this.outputs[node.id] = { tick: i, of: times, firedAt: Date.now() };
              for (const ed of (this.edges.get(node.id) || []).filter((x) => x.port === 'out')) this.spawn(ed.to);
            }
            await sleep(every, signal);
          } finally { ctx.setWaiting(false); }
          return { port: 'out', output: { tick: times, of: times, firedAt: Date.now() } };
        }
        const atRaw = template(c.at, sc());
        const at = typeof atRaw === 'number' || /^\d+$/.test(String(atRaw)) ? Number(atRaw) : Date.parse(atRaw);
        if (!Number.isFinite(at)) throw new Error(`timer: cannot parse "${atRaw}" as a date`);
        recordInput({ at: new Date(at).toISOString() });
        ctx.setWaiting(true);
        try { await sleep(at - Date.now(), signal); } finally { ctx.setWaiting(false); }
        return { port: 'out', output: { firedAt: Date.now(), scheduledFor: at } };
      }

      case 'signal': {
        if (c.mode === 'listen') return { port: 'out', output: ctx.signalPayload ?? null };
        const name = c.signalName;
        const buf = this.signalBuffer.get(name);
        if (buf?.length) return { port: 'out', output: buf.shift() ?? null };
        ctx.setWaiting(true);
        this.bridge.log('info', `waiting for signal "${name}"`, node.id);
        try {
          const res = await new Promise((resolve, reject) => {
            const set = this.signalWaiters.get(name) || new Set(); this.signalWaiters.set(name, set);
            let t;
            const fn = (payload) => { clearTimeout(t); signal.removeEventListener('abort', onAbort); resolve({ port: 'out', output: payload ?? null }); };
            const onAbort = () => { clearTimeout(t); set.delete(fn); reject(new Aborted(signal.reason)); };
            signal.addEventListener('abort', onAbort, { once: true });
            if (Number(c.timeoutMs) > 0) t = setTimeout(() => { set.delete(fn); signal.removeEventListener('abort', onAbort); resolve({ port: 'timeout', output: { timedOut: true } }); }, Number(c.timeoutMs));
            set.add(fn);
          });
          return res;
        } finally { ctx.setWaiting(false); }
      }

      case 'subworkflow': {
        const input = plain(template(c.input, sc()));
        const childId = `${this.meta.instanceId}.${node.id}.${this.counter}`;
        recordInput({ workflowId: c.workflowId, instanceId: childId, input });
        if (!c.wait) {
          const r = await this.bridge.request('workflow.start', { workflowId: c.workflowId, input, wait: false, instanceId: childId, nodeId: node.id });
          return { port: 'out', output: r };
        }
        ctx.setWaiting(true);
        const cancel = () => this.bridge.request('workflow.cancel', { instanceId: childId, reason: 'parent cancelled' }).catch(() => {});
        try {
          const p = this.bridge.request('workflow.start', { workflowId: c.workflowId, input, wait: true, instanceId: childId, nodeId: node.id });
          const r = await withTimeout(p, Number(c.timeoutMs) || 0, signal);
          return { port: 'out', output: r.output };
        } catch (e) {
          if (e.aborted || e.timeout) cancel();
          throw e;
        } finally { ctx.setWaiting(false); }
      }

      case 'activity': return this.runActivity(node, ctx, sc, recordInput);

      default: throw new Error(`unsupported node type ${node.type}`);
    }
  }

  async runActivity(node, ctx, sc, recordInput) {
    const c = node.config;
    const signal = this.ac.signal;
    const timeout = Number(c.timeoutMs) || 0;
    const s = sc();

    switch (c.kind) {
      case 'http': {
        const url = String(template(c.url, s) || '');
        if (!/^https?:\/\//.test(url)) throw new Error(`http: invalid url "${url}"`);
        const method = (c.method || 'GET').toUpperCase();
        const headers = plain(template(c.headers || {}, s)) || {};
        let body = c.body === undefined || c.body === '' || method === 'GET' ? undefined : plain(template(c.body, s));
        if (body !== undefined && typeof body !== 'string') { body = JSON.stringify(body); if (!Object.keys(headers).some((h) => h.toLowerCase() === 'content-type')) headers['content-type'] = 'application/json'; }
        recordInput({ method, url, headers: redact(headers), body: body && body.length < 4000 ? safeParse(body) : body ? '(large)' : undefined });
        const signals = [signal]; if (timeout) signals.push(AbortSignal.timeout(timeout));
        const res = await fetch(url, { method, headers, body, signal: AbortSignal.any(signals) }).catch((e) => {
          if (signal.aborted) throw new Aborted(signal.reason);
          throw new Error(`http ${method} ${url}: ${e.name === 'TimeoutError' ? `timed out after ${timeout} ms` : e.cause?.message || e.message}`);
        });
        const text = await res.text();
        const ct = res.headers.get('content-type') || '';
        const out = { status: res.status, headers: Object.fromEntries(res.headers), body: ct.includes('json') ? safeParse(text) : text };
        if (c.failOnHttpError !== false && res.status >= 400) throw new Error(`http ${method} ${url} → ${res.status}: ${text.slice(0, 300)}`);
        return { port: 'out', output: out };
      }

      case 'rabbitmq.publish': {
        const p = { exchange: template(c.exchange || '', s), routingKey: template(c.routingKey || '', s), queue: template(c.queue || '', s), message: plain(template(c.message, s)), persistent: c.persistent !== false };
        recordInput(p);
        return { port: 'out', output: await withTimeout(this.bridge.request('rabbit.publish', p), timeout, signal) };
      }

      case 'mqtt.publish': {
        const p = { topic: template(c.topic, s), message: plain(template(c.message, s)), qos: Number(c.qos ?? 1), retain: !!c.retain };
        recordInput(p);
        return { port: 'out', output: await withTimeout(this.bridge.request('mqtt.publish', p), timeout, signal) };
      }

      case 'db.write': {
        const p = { collection: template(c.collection, s), operation: c.operation || 'insert', keyField: c.keyField || 'id', document: plain(template(c.document, s)) };
        recordInput(p);
        return { port: 'out', output: await withTimeout(this.bridge.request('db.write', p), timeout, signal) };
      }

      case 'transform': {
        const out = plain(template(c.mapping, s));
        return { port: 'out', output: out };
      }

      case 'event.receive': {
        const spec = c.source === 'mqtt'
          ? { source: 'mqtt', topic: template(c.rxTopic, s) }
          : { source: 'rabbitmq', exchange: template(c.rxExchange || '', s), routingKey: template(c.rxRoutingKey || '#', s), queue: template(c.rxQueue || '', s), durable: !!c.rxQueue };
        recordInput({ ...spec, filter: c.filter || null });
        ctx.setWaiting(true);
        let subId;
        try {
          const result = await new Promise((resolve, reject) => {
            let t;
            const onAbort = () => { clearTimeout(t); reject(new Aborted(signal.reason)); };
            signal.addEventListener('abort', onAbort, { once: true });
            if (timeout) t = setTimeout(() => { signal.removeEventListener('abort', onAbort); resolve({ port: 'timeout', output: { timedOut: true } }); }, timeout);
            this.bridge.request('event.subscribe', spec).then((r) => {
              subId = r.subscriptionId;
              this.bridge.onEvent(subId, (payload, meta) => {
                let ok = true;
                if (c.filter) { try { ok = !!evalExpr(c.filter, this.scope({ event: payload, meta })); } catch (e) { this.bridge.log('warn', `filter error: ${e.message}`, node.id); ok = false; } }
                if (!ok) return;
                clearTimeout(t); signal.removeEventListener('abort', onAbort);
                resolve({ port: 'out', output: payload });
              });
            }, (e) => { clearTimeout(t); reject(e); });
          });
          return result;
        } finally {
          ctx.setWaiting(false);
          if (subId) { this.bridge.offEvent(subId); this.bridge.request('event.unsubscribe', { subscriptionId: subId }).catch(() => {}); }
        }
      }

      default: throw new Error(`unknown activity kind "${c.kind}"`);
    }
  }
}

function withTimeout(promise, ms, signal) {
  return new Promise((resolve, reject) => {
    let t;
    const onAbort = () => { clearTimeout(t); reject(new Aborted(signal.reason)); };
    if (signal) { if (signal.aborted) return onAbort(); signal.addEventListener('abort', onAbort, { once: true }); }
    if (ms > 0) t = setTimeout(() => { signal?.removeEventListener('abort', onAbort); const e = new Error(`timed out after ${ms} ms`); e.timeout = true; reject(e); }, ms);
    promise.then((v) => { clearTimeout(t); signal?.removeEventListener('abort', onAbort); resolve(v); },
      (e) => { clearTimeout(t); signal?.removeEventListener('abort', onAbort); reject(e); });
  });
}

const safeParse = (s) => { try { return JSON.parse(s); } catch { return s; } };
const redact = (h) => Object.fromEntries(Object.entries(h).map(([k, v]) => [k, /authorization|api-key|token|secret|cookie/i.test(k) ? '***' : v]));

module.exports = { Engine, sleep };
