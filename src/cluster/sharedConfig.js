'use strict';
/*
 * Cluster-wide shared configuration kept in etcd or MongoDB.
 *
 * One document (normally pushed from config/cluster.yaml) with three sections:
 *   common: settings for every node (gRPC credentials, log level …)
 *   master: settings for every master (store, brokers, data DB, cluster tuning …)
 *   worker: settings for every worker (runtime limits, discovery …)
 *
 * Each node's own YAML then only holds what is unique to it (node id, ports,
 * advertised address) plus a `shared:` block saying where the document lives.
 * Effective config = built-in defaults ← common ← master|worker ← local YAML.
 *
 * Documents are versioned (etcd: mod revision, mongo: counter) and watched, so
 * a push reaches every running node; settings that can change live are applied
 * immediately, the rest are reported as "restart required".
 */
const EventEmitter = require('events');

class EtcdShared extends EventEmitter {
  constructor(o) {
    super();
    this.o = o;
    this.key = (o.prefix || '/flowforge/').replace(/\/?$/, '/') + 'config/cluster';
  }
  async connect() {
    const { Etcd3 } = require('etcd3');
    this.client = new Etcd3({ hosts: o2arr(this.o.hosts), auth: this.o.auth, dialTimeout: 5000 });
  }
  async get() {
    const r = await this.client.get(this.key).exec();
    const kv = r.kvs[0];
    if (!kv) return null;
    const rec = JSON.parse(kv.value.toString());
    return { ...rec, version: Number(kv.mod_revision) };
  }
  async put(doc, by, expectVersion) {
    const value = JSON.stringify({ doc, updatedAt: Date.now(), updatedBy: by || 'unknown' });
    if (expectVersion !== undefined && expectVersion !== null) {
      const cmp = expectVersion === 0 ? this.client.if(this.key, 'Create', '==', 0) : this.client.if(this.key, 'Mod', '==', expectVersion);
      const r = await cmp.then(this.client.put(this.key).value(value)).commit();
      if (!r.succeeded) throw Object.assign(new Error('shared config was changed by someone else – reload and retry'), { status: 409 });
      return Number(r.header.revision);
    }
    const r = await this.client.put(this.key).value(value).exec();
    return Number(r.header.revision);
  }
  async watch() {
    this.watcher = await this.client.watch().key(this.key).create();
    this.watcher.on('put', (kv) => {
      try { const rec = JSON.parse(kv.value.toString()); this.emit('change', { ...rec, version: Number(kv.mod_revision) }); } catch (e) { this.emit('error', e); }
    });
    this.watcher.on('error', (e) => this.emit('error', e));
  }
  async close() { await this.watcher?.cancel().catch(() => {}); this.client?.close(); }
}

class MongoShared extends EventEmitter {
  constructor(o) { super(); this.o = o; }
  async connect() {
    const { MongoClient } = require('mongodb');
    this.client = new MongoClient(this.o.url, { serverSelectionTimeoutMS: 5000 });
    await this.client.connect();
    this.col = this.client.db(this.o.db || 'flowforge').collection('cluster_config');
  }
  async get() {
    const d = await this.col.findOne({ _id: 'cluster' });
    return d ? { doc: d.doc, version: d.version, updatedAt: d.updatedAt, updatedBy: d.updatedBy } : null;
  }
  async put(doc, by, expectVersion) {
    const set = { doc, updatedAt: Date.now(), updatedBy: by || 'unknown' };
    if (expectVersion === 0) {
      try { await this.col.insertOne({ _id: 'cluster', ...set, version: 1 }); return 1; }
      catch (e) { if (e.code === 11000) throw Object.assign(new Error('shared config already exists'), { status: 409 }); throw e; }
    }
    const filter = { _id: 'cluster' };
    if (expectVersion !== undefined && expectVersion !== null) filter.version = expectVersion;
    const r = await this.col.findOneAndUpdate(filter, { $set: set, $inc: { version: 1 } }, { upsert: expectVersion == null, returnDocument: 'after' });
    const d = r?.value !== undefined ? r.value : r;
    if (!d) throw Object.assign(new Error('shared config was changed by someone else – reload and retry'), { status: 409 });
    return d.version;
  }
  async watch() {
    // Polling works on standalone servers too (change streams need a replica set).
    let last = (await this.get())?.version || 0;
    this.timer = setInterval(async () => {
      try { const cur = await this.get(); if (cur && cur.version !== last) { last = cur.version; this.emit('change', cur); } }
      catch (e) { this.emit('error', e); }
    }, this.o.pollMs || 3000);
    this.timer.unref();
  }
  async close() { clearInterval(this.timer); await this.client?.close(); }
}

const o2arr = (h) => (Array.isArray(h) ? h : String(h || '127.0.0.1:2379').split(','));

/** @param {{source:'etcd'|'mongo', etcd?, mongo?}} spec  – the `shared:` block of a node YAML */
async function openShared(spec) {
  if (!spec || !spec.source) throw new Error('shared.source must be etcd or mongo');
  const s = spec.source === 'etcd' ? new EtcdShared(spec.etcd || {}) : spec.source === 'mongo' ? new MongoShared(spec.mongo || {}) : null;
  if (!s) throw new Error(`unknown shared.source "${spec.source}" (etcd | mongo)`);
  s.source = spec.source;
  await s.connect();
  return s;
}

const SECRET_RE = /pass(word)?|secret|token|private|credential/i;
/** Deep copy with secret-looking values masked (for API responses / logs). */
function redact(v, key = '') {
  if (Array.isArray(v)) return v.map((x) => redact(x));
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, redact(x, k)]));
  if (SECRET_RE.test(key) && v !== '' && v != null) return typeof v === 'string' && /^\$\{/.test(v) ? v : '***';
  if (typeof v === 'string') return v.replace(/\/\/([^:@/]+):([^@/]+)@/, '//$1:***@'); // URLs with credentials
  return v;
}

/** Paths that differ between two plain objects (for "what changed" logs). */
function diffPaths(a, b, base = '') {
  const out = [];
  const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})]);
  for (const k of keys) {
    const p = base ? `${base}.${k}` : k;
    const x = a?.[k], y = b?.[k];
    if (x && y && typeof x === 'object' && typeof y === 'object' && !Array.isArray(x) && !Array.isArray(y)) out.push(...diffPaths(x, y, p));
    else if (JSON.stringify(x) !== JSON.stringify(y)) out.push(p);
  }
  return out;
}

module.exports = { openShared, redact, diffPaths };
