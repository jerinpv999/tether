'use strict';

const getPath = (obj, p) => String(p).split('.').reduce((o, k) => (o == null ? undefined : o[k]), obj);

/** "body:order.id" | "header:x-request-id" | "query:id" | "param:orderId" → value */
function resolveInstanceId(spec, src) {
  if (!spec) return undefined;
  const [where, ...rest] = String(spec).split(':');
  const key = rest.join(':');
  let v;
  if (where === 'body') v = getPath(src.body, key);
  else if (where === 'header') v = src.headers?.[key.toLowerCase()];
  else if (where === 'query') v = src.query?.[key];
  else if (where === 'param') v = src.params?.[key];
  return v === undefined || v === null || v === '' ? undefined : String(v);
}

/** Compile "/orders/:id/*" into a matcher returning params or null. */
function compilePath(p) {
  const keys = [];
  const re = new RegExp('^' + String(p).replace(/\/+$/, '').split('/').map((seg) => {
    if (seg.startsWith(':')) { keys.push(seg.slice(1)); return '([^/]+)'; }
    if (seg === '*') { keys.push('wild'); return '(.*)'; }
    return seg.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
  }).join('/') + '/?$');
  return (path) => {
    const m = re.exec(path);
    if (!m) return null;
    const params = {};
    keys.forEach((k, i) => { params[k] = decodeURIComponent(m[i + 1]); });
    return params;
  };
}

const SENSITIVE = ['authorization', 'cookie', 'proxy-authorization', 'x-api-key'];
const redactHeaders = (h = {}) => Object.fromEntries(Object.entries(h).map(([k, v]) => [k, SENSITIVE.includes(k.toLowerCase()) ? '***' : v]));

/**
 * Registers every enabled workflow trigger:
 *   http     → matched by the Express /hooks/* route (see api.js)
 *   rabbitmq → durable queue consumer (ack after the instance is persisted)
 *   mqtt     → topic subscription
 *   schedule → interval timer
 * Re-registers a workflow's triggers whenever it is saved or deleted.
 */
class Triggers {
  constructor({ runtime, gateway, logger, sharedGroup }) {
    this.sharedGroup = sharedGroup || '';
    this.runtime = runtime;
    this.gateway = gateway;
    this.log = logger;
    this.http = new Map();  // workflowId -> [{ method, match, trigger }]
    this.active = new Map(); // workflowId -> [cleanup fn]
    this.defs = new Map();   // workflowId -> definition currently armed
    this.isLeader = false;   // broker + schedule triggers run on the leader only
  }

  async registerAll(workflows) { for (const wf of workflows) await this.register(wf); }

  /** Leadership changed: arm or disarm broker/schedule triggers. HTTP triggers stay on every master. */
  async setLeader(flag) {
    if (this.isLeader === flag) return;
    this.isLeader = flag;
    for (const wf of [...this.defs.values()]) await this.register(wf);
  }

  /** Pick up workflow changes saved on other masters. */
  async sync(store) {
    const list = await store.listWorkflows();
    const seen = new Set();
    for (const wf of list) {
      seen.add(wf.id);
      const cur = this.defs.get(wf.id);
      if (!cur || cur.version !== wf.version || cur.updatedAt !== wf.updatedAt) {
        await this.register(wf);
        if (cur) this.log.info(`workflow v${wf.version} picked up from cluster`, { workflowId: wf.id });
      }
    }
    for (const id of [...this.defs.keys()]) if (!seen.has(id)) await this.remove(id);
  }

  /** Workflow deleted: disarm and forget it. */
  async remove(workflowId) { await this.unregister(workflowId); this.defs.delete(workflowId); }

  async unregister(workflowId) {
    this.http.delete(workflowId);
    for (const fn of this.active.get(workflowId) || []) await fn().catch(() => {});
    this.active.delete(workflowId);
  }

  async register(wf) {
    await this.unregister(wf.id);
    this.defs.set(wf.id, wf);
    if (wf.enabled === false) return;
    const cleanups = [];
    const httpList = [];
    for (const t of wf.triggers || []) {
      if (t.enabled === false) continue;
      try {
        if (t.type === 'http') {
          httpList.push({ method: (t.method || 'POST').toUpperCase(), match: compilePath(t.path.startsWith('/') ? t.path : '/' + t.path), trigger: t });
        } else if (!this.isLeader) {
          continue; // only the leader consumes broker events / runs schedules
        } else if (t.type === 'rabbitmq' || t.type === 'mqtt') {
          const spec = t.type === 'rabbitmq'
            ? { source: 'rabbitmq', queue: t.queue || `flowforge.${wf.id}`, exchange: t.exchange || '', routingKey: t.routingKey || '#', durable: true }
            : { source: 'mqtt', topic: this.sharedGroup ? `$share/${this.sharedGroup}/${t.topic}` : t.topic, qos: t.qos ?? 1 };
          const subId = await this.gateway.subscribe(spec, async (payload, meta) => {
            const instanceId = resolveInstanceId(t.instanceIdFrom, { body: payload, headers: meta.headers || {} });
            try {
              await this.runtime.start(wf.id, {
                instanceId, input: payload,
                trigger: { type: t.type, ...meta },
                request: { trigger: { type: t.type, ...meta }, body: payload },
              });
            } catch (e) {
              if (e.status === 409) this.log.warn(`duplicate event ignored: ${e.message}`, { workflowId: wf.id });
              else throw e;
            }
          });
          cleanups.push(() => this.gateway.unsubscribe(subId));
          this.log.info(`trigger armed: ${t.type} ${t.type === 'mqtt' ? t.topic : spec.queue + (spec.exchange ? ` ⇐ ${spec.exchange}/${spec.routingKey}` : '')}`, { workflowId: wf.id });
        } else if (t.type === 'schedule') {
          const ms = Math.max(1000, Number(t.intervalMs) || 3600000);
          const timer = setInterval(() => {
            this.runtime.start(wf.id, { input: t.input || {}, trigger: { type: 'schedule', firedAt: Date.now() } })
              .catch((e) => this.log.error(`schedule start failed: ${e.message}`, { workflowId: wf.id }));
          }, ms);
          cleanups.push(async () => clearInterval(timer));
          this.log.info(`trigger armed: schedule every ${ms} ms`, { workflowId: wf.id });
        }
      } catch (e) {
        this.log.error(`could not arm ${t.type} trigger: ${e.message}`, { workflowId: wf.id });
      }
    }
    if (httpList.length) this.http.set(wf.id, httpList);
    this.active.set(wf.id, cleanups);
  }

  /** Find the workflow whose HTTP trigger matches. */
  matchHttp(method, path) {
    for (const [workflowId, list] of this.http) {
      for (const h of list) {
        if (h.method !== 'ANY' && h.method !== method) continue;
        const params = h.match(path);
        if (params) return { workflowId, params, trigger: h.trigger };
      }
    }
    return null;
  }

  async stop() { for (const id of [...this.active.keys()]) await this.unregister(id); this.defs.clear(); }
}

module.exports = { Triggers, resolveInstanceId, redactHeaders, compilePath };
