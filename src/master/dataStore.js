'use strict';
/*
 * Storage behind the `db.write` activity.
 *   mongo – real collections (dataDb.mongoUrl, or the store's Mongo server in cluster mode)
 *   etcd  – one key per document: <prefix>data/<collection>/<_id>  (small/medium data sets)
 *   file  – JSON file per collection (single-node only; rejected in cluster mode)
 * mongo and etcd are shared by every master, so data written before a leader
 * failover is visible to the new leader.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const NAME_RE = /^[a-zA-Z0-9_.-]{1,120}$/;

function check(collection, operation, doc, keyField) {
  if (!collection || !NAME_RE.test(collection)) throw new Error('valid collection name is required (letters, digits, _ . -)');
  if (!['insert', 'upsert', 'update', 'delete'].includes(operation)) throw new Error(`unknown operation ${operation}`);
  if (operation !== 'insert' && doc[keyField] === undefined) throw new Error(`document.${keyField} is required for ${operation}`);
}

/** Pure array implementation shared by the file and etcd backends. Returns { result, changed: [{op, row}] } */
function applyToRows(rows, operation, doc, keyField) {
  const idx = operation === 'insert' ? -1 : rows.findIndex((r) => r[keyField] === doc[keyField]);
  if (operation === 'insert') { const row = { _id: crypto.randomUUID(), ...doc }; return { result: { insertedId: row._id }, changed: [{ op: 'put', row }] }; }
  if (operation === 'upsert') {
    const row = idx >= 0 ? { ...rows[idx], ...doc, _id: rows[idx]._id } : { _id: crypto.randomUUID(), ...doc };
    return { result: { matched: idx >= 0 ? 1 : 0, upserted: idx >= 0 ? 0 : 1 }, changed: [{ op: 'put', row }] };
  }
  if (operation === 'update') {
    if (idx < 0) return { result: { matched: 0, modified: 0 }, changed: [] };
    return { result: { matched: 1, modified: 1 }, changed: [{ op: 'put', row: { ...rows[idx], ...doc, _id: rows[idx]._id } }] };
  }
  if (idx < 0) return { result: { deleted: 0 }, changed: [] };
  return { result: { deleted: 1 }, changed: [{ op: 'del', row: rows[idx] }] };
}

class Serial {
  constructor() { this.q = new Map(); }
  run(key, fn) {
    const prev = this.q.get(key) || Promise.resolve();
    const p = prev.then(fn);
    this.q.set(key, p.catch(() => {}));
    return p;
  }
}

class FileData {
  constructor(o) { this.name = 'file'; this.dir = o.fileDir; this.serial = new Serial(); }
  async init() { fs.mkdirSync(this.dir, { recursive: true }); }
  _file(c) { return path.join(this.dir, `${c}.json`); }
  _read(c) { const f = this._file(c); return fs.existsSync(f) ? JSON.parse(fs.readFileSync(f, 'utf8')) : []; }
  write({ collection, operation = 'insert', document, keyField = 'id' }) {
    const doc = norm(document);
    check(collection, operation, doc, keyField);
    return this.serial.run(collection, () => {
      const rows = this._read(collection);
      const { result, changed } = applyToRows(rows, operation, doc, keyField);
      for (const ch of changed) {
        const i = rows.findIndex((r) => r._id === ch.row._id);
        if (ch.op === 'del') rows.splice(i, 1); else if (i >= 0) rows[i] = ch.row; else rows.push(ch.row);
      }
      fs.writeFileSync(this._file(collection), JSON.stringify(rows, null, 2));
      return result;
    });
  }
  async list(collection, { limit = 100 } = {}) { check(collection, 'insert', {}, 'id'); return this._read(collection).slice(-limit); }
  async collections() { return fs.readdirSync(this.dir).filter((f) => f.endsWith('.json')).map((f) => f.slice(0, -5)); }
  async close() {}
}

class EtcdData {
  constructor(o) { this.name = 'etcd'; this.o = o; this.p = o.etcdPrefix; this.serial = new Serial(); }
  async init() {
    const { Etcd3 } = require('etcd3');
    this.client = new Etcd3({ hosts: this.o.etcd.hosts, auth: this.o.etcd.auth });
    await this.client.get(this.p + '__ping').string();
  }
  async _rows(collection) {
    const all = await this.client.getAll().prefix(`${this.p}${collection}/`).strings();
    return Object.values(all).map((v) => JSON.parse(v));
  }
  write({ collection, operation = 'insert', document, keyField = 'id' }) {
    const doc = norm(document);
    check(collection, operation, doc, keyField);
    return this.serial.run(collection, async () => {
      const rows = operation === 'insert' ? [] : await this._rows(collection);
      const { result, changed } = applyToRows(rows, operation, doc, keyField);
      for (const ch of changed) {
        const key = `${this.p}${collection}/${ch.row._id}`;
        if (ch.op === 'del') await this.client.delete().key(key);
        else await this.client.put(key).value(JSON.stringify({ ...ch.row, _updatedAt: Date.now() }));
      }
      return result;
    });
  }
  async list(collection, { limit = 100 } = {}) {
    check(collection, 'insert', {}, 'id');
    return (await this._rows(collection)).sort((a, b) => (a._updatedAt || 0) - (b._updatedAt || 0)).slice(-limit);
  }
  async collections() {
    const keys = await this.client.getAll().prefix(this.p).keys();
    return [...new Set(keys.map((k) => k.slice(this.p.length).split('/')[0]))];
  }
  async close() { this.client?.close(); }
}

class MongoData {
  constructor(o) { this.name = 'mongo'; this.o = o; }
  async init() {
    const { MongoClient } = require('mongodb');
    this.client = new MongoClient(this.o.mongoUrl);
    await this.client.connect();
    this.db = this.client.db(this.o.mongoDb);
  }
  async write({ collection, operation = 'insert', document, keyField = 'id' }) {
    const doc = norm(document);
    check(collection, operation, doc, keyField);
    const c = this.db.collection(collection);
    if (operation === 'insert') { const r = await c.insertOne({ ...doc }); return { insertedId: String(r.insertedId) }; }
    if (operation === 'upsert') { const r = await c.updateOne({ [keyField]: doc[keyField] }, { $set: doc }, { upsert: true }); return { matched: r.matchedCount, upserted: r.upsertedCount }; }
    if (operation === 'update') { const r = await c.updateOne({ [keyField]: doc[keyField] }, { $set: doc }); return { matched: r.matchedCount, modified: r.modifiedCount }; }
    const r = await c.deleteOne({ [keyField]: doc[keyField] }); return { deleted: r.deletedCount };
  }
  async list(collection, { limit = 100 } = {}) {
    check(collection, 'insert', {}, 'id');
    const rows = await this.db.collection(collection).find().sort({ _id: -1 }).limit(Number(limit)).toArray();
    return rows.reverse().map((r) => ({ ...r, _id: String(r._id) }));
  }
  async collections() { return (await this.db.listCollections().toArray()).map((c) => c.name); }
  async close() { await this.client?.close(); }
}

const norm = (d) => (d && typeof d === 'object' && !Array.isArray(d) ? d : { value: d });

async function createDataStore(dataDb) {
  const s = dataDb.type === 'mongo' ? new MongoData(dataDb) : dataDb.type === 'etcd' ? new EtcdData(dataDb) : new FileData(dataDb);
  await s.init();
  return s;
}

module.exports = { createDataStore, applyToRows };
