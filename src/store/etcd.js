'use strict';
const { matchInstance, matchLog } = require('./memory');

/**
 * etcd v3 store. Key layout (prefix defaults to /flowforge/):
 *   workflows/<id>
 *   instances/<instanceId>
 *   activities/<instanceId>/<activityId>
 *   requests/<instanceId>
 *   logs/<ts>-<seq>             (attached to an hourly lease → etcd expires them = retention)
 * etcd is best for workflow metadata/coordination; use Mongo when you expect
 * very large instance or log volumes.
 */
class EtcdStore {
  constructor(opts, retentionHours) {
    this.name = 'etcd';
    this.opts = opts;
    this.p = opts.prefix.endsWith('/') ? opts.prefix : opts.prefix + '/';
    this.retentionSec = Math.max(60, Math.round(retentionHours * 3600));
    this.leases = new Map(); // hourBucket -> lease
    this.seq = 0;
  }

  async init() {
    const { Etcd3 } = require('etcd3');
    this.client = new Etcd3({ hosts: this.opts.hosts });
    await this.client.get(this.p + '__ping').string(); // fail fast if unreachable
  }
  async close() { this.client?.close(); }

  async _put(k, v) { await this.client.put(this.p + k).value(JSON.stringify(v)); }
  async _get(k) { const s = await this.client.get(this.p + k).string(); return s ? JSON.parse(s) : null; }
  async _all(prefix) { const o = await this.client.getAll().prefix(this.p + prefix).strings(); return Object.values(o).map((s) => JSON.parse(s)); }

  async saveWorkflow(w) { await this._put(`workflows/${w.id}`, w); return w; }
  async getWorkflow(id) { return this._get(`workflows/${id}`); }
  async listWorkflows() { return (await this._all('workflows/')).sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id)); }
  async deleteWorkflow(id) { const r = await this.client.delete().key(this.p + `workflows/${id}`); return Number(r.deleted) > 0; }

  async saveInstance(i) { await this._put(`instances/${i.instanceId}`, i); return i; }
  async getInstance(id) { return this._get(`instances/${id}`); }
  async listInstances(f = {}) {
    const all = (await this._all('instances/')).filter((i) => matchInstance(i, f)).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    const off = Number(f.offset || 0);
    return { total: all.length, items: all.slice(off, off + Number(f.limit || 100)) };
  }
  async deleteInstance(id) {
    await this.client.delete().key(this.p + `instances/${id}`);
    await this.client.delete().prefix(this.p + `activities/${id}/`);
    await this.client.delete().key(this.p + `requests/${id}`);
  }

  async saveActivity(a) {
    const k = `activities/${a.instanceId}/${a.activityId}`;
    const prev = await this._get(k);
    await this._put(k, { ...(prev || {}), ...a });
    return a;
  }
  async listActivities(instanceId) { return (await this._all(`activities/${instanceId}/`)).sort((a, b) => (a.startedAt || 0) - (b.startedAt || 0)); }

  async saveRequest(r) { await this._put(`requests/${r.instanceId}`, r); return r; }
  async getRequest(id) { return this._get(`requests/${id}`); }

  async _lease(ts) {
    const bucket = Math.floor(ts / 3600000);
    let l = this.leases.get(bucket);
    if (!l) {
      l = this.client.lease(this.retentionSec + 3600, { autoKeepAlive: false });
      this.leases.set(bucket, l);
      for (const b of this.leases.keys()) if (b < bucket - 2) this.leases.delete(b);
    }
    return l;
  }
  async appendLog(e) {
    const key = `${this.p}logs/${String(e.ts).padStart(15, '0')}-${(this.seq++ % 1e6).toString().padStart(6, '0')}`;
    const lease = await this._lease(e.ts);
    await lease.put(key).value(JSON.stringify(e));
  }
  async queryLogs(f = {}) {
    const limit = Number(f.limit || 500);
    const rows = (await this._all('logs/')).filter((l) => matchLog(l, f)).sort((a, b) => a.ts - b.ts);
    return rows.slice(-limit);
  }
  async purgeLogs(beforeTs) {
    const keys = await this.client.getAll().prefix(this.p + 'logs/').keys();
    let n = 0;
    for (const k of keys) {
      const ts = Number(k.slice((this.p + 'logs/').length).split('-')[0]);
      if (ts < beforeTs) { await this.client.delete().key(k); n++; }
    }
    return n;
  }
  async purgeInstances(beforeTs, finalStatuses) {
    let n = 0;
    for (const i of await this._all('instances/')) {
      if (finalStatuses.includes(i.status) && (i.endedAt || 0) < beforeTs) { await this.deleteInstance(i.instanceId); n++; }
    }
    return n;
  }
}

module.exports = { EtcdStore };
