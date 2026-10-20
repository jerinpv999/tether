'use strict';
const EventEmitter = require('events');

/**
 * Coordinator = leader election + service discovery.
 *
 *   start({ nodeId, role, grpcAddress, httpUrl, tags, campaign })
 *   isLeader / leader → { nodeId, grpcAddress, httpUrl, term }
 *   listNodes(role?) → registered masters / workers (heartbeat-refreshed, TTL-expired)
 *   events: 'elected' (term), 'demoted' (reason), 'leader' (info|null)
 *
 * Backends
 *   none  – single master, always leader (dev / single box)
 *   etcd  – leader key created in a transaction under a lease; node keys on leases
 *   mongo – leader lease document updated with conditional writes; node docs with TTL
 *
 * The leader term is a monotonically increasing fencing number (etcd: key revision,
 * mongo: incremented counter) handed to workers so they can ignore stale leaders.
 */
class BaseCoordinator extends EventEmitter {
  constructor(opts) {
    super();
    this.opts = opts;
    this.isLeader = false;
    this.leader = null;
    this.self = null;
    this.timer = null;
    this.stopped = false;
  }
  _setLeader(info) {
    const prev = this.leader;
    const changed = (prev?.nodeId || null) !== (info?.nodeId || null) || (prev?.term || 0) !== (info?.term || 0);
    this.leader = info;
    if (changed) this.emit('leader', info);
  }
  _elected(term) {
    if (this.isLeader) return;
    this.isLeader = true;
    this._setLeader({ ...this._selfInfo(), term });
    this.emit('elected', term);
  }
  _demoted(reason) {
    if (!this.isLeader) return;
    this.isLeader = false;
    this.emit('demoted', reason);
  }
  _selfInfo() {
    const s = this.self;
    return { nodeId: s.nodeId, grpcAddress: s.grpcAddress, httpUrl: s.httpUrl };
  }
}

class NoneCoordinator extends BaseCoordinator {
  constructor(opts) { super(opts); this.name = 'none'; this.nodes = new Map(); }
  async start(self) {
    this.self = self;
    if (self.campaign) setImmediate(() => this._elected(1));
  }
  async register(node) { this.nodes.set(node.nodeId, { ...node, lastSeen: Date.now() }); }
  async unregister(nodeId) { this.nodes.delete(nodeId); }
  async listNodes(role) {
    const me = { ...this.self, role: this.self.role, lastSeen: Date.now(), leader: this.isLeader };
    return [me, ...this.nodes.values()].filter((n) => !role || n.role === role);
  }
  async stop() { this.stopped = true; }
}

// ───────────────────────────────── etcd ─────────────────────────────────
class EtcdCoordinator extends BaseCoordinator {
  constructor(opts) {
    super(opts);
    this.name = 'etcd';
    this.p = (opts.etcd.prefix || '/flowforge/').replace(/\/?$/, '/') + 'cluster/';
    this.ttlSec = Math.max(2, Math.round((opts.leaseTtlMs || 10000) / 1000));
  }

  async start(self) {
    const { Etcd3 } = require('etcd3');
    this.self = self;
    this.client = new Etcd3({ hosts: this.opts.etcd.hosts, auth: this.opts.etcd.auth });
    await this._newLease();
    await this._tick();
    this.timer = setInterval(() => this._tick().catch((e) => this.emit('warn', e)), Math.max(500, (this.ttlSec * 1000) / 3));
  }

  async _newLease() {
    if (this.lease) { this.lease.removeAllListeners(); this.lease.revoke().catch(() => {}); }
    this.lease = this.client.lease(this.ttlSec);
    this.lease.on('lost', () => {
      this._demoted('lease lost');
      if (!this.stopped) this._newLease().then(() => this._tick()).catch((e) => this.emit('warn', e));
    });
    this.leaseId = await this.lease.grant();
  }

  async _tick() {
    if (this.stopped) return;
    const s = this.self;
    const nodeKey = `${this.p}nodes/${s.role}/${s.nodeId}`;
    await this.lease.put(nodeKey).value(JSON.stringify({ ...s, campaign: undefined, lastSeen: Date.now(), leader: this.isLeader }));
    const leaderKey = `${this.p}leader`;
    if (s.campaign && !this.isLeader) {
      const value = JSON.stringify(this._selfInfo());
      const r = await this.client.if(leaderKey, 'Create', '==', 0).then(this.client.put(leaderKey).value(value).lease(this.leaseId)).commit();
      if (r.succeeded) return this._observe(true, Number(r.header.revision));
    }
    await this._observe(false);
  }

  async _observe(justElected, rev) {
    const res = await this.client.get(`${this.p}leader`).exec();
    const kv = res.kvs[0];
    if (!kv) { this._demoted('leader key vanished'); this._setLeader(null); return; }
    const info = { ...JSON.parse(kv.value.toString()), term: Number(kv.create_revision) };
    const mine = info.nodeId === this.self.nodeId && String(kv.lease) === String(this.leaseId);
    if (mine) { if (justElected || !this.isLeader) this._elected(info.term || rev); else this._setLeader(info); }
    else { this._demoted('another node holds the leader key'); this._setLeader(info); }
  }

  async register(node) { /* masters/workers register themselves via start(); nothing to do */ void node; }

  async listNodes(role) {
    const all = await this.client.getAll().prefix(`${this.p}nodes/${role ? role + '/' : ''}`).strings();
    return Object.values(all).map((v) => JSON.parse(v)).map((n) => ({ ...n, leader: n.nodeId === this.leader?.nodeId && n.role === 'master' }));
  }

  async resign() {
    if (!this.isLeader) return;
    await this.client.if(`${this.p}leader`, 'Lease', '==', this.leaseId).then(this.client.delete().key(`${this.p}leader`)).commit().catch(() => {});
    this._demoted('resigned');
  }

  async stop() {
    this.stopped = true;
    clearInterval(this.timer);
    await this.resign();
    if (this.lease) { this.lease.removeAllListeners(); await this.lease.revoke().catch(() => {}); }
    this.client?.close();
  }
}

// ───────────────────────────────── MongoDB ─────────────────────────────────
class MongoCoordinator extends BaseCoordinator {
  constructor(opts) { super(opts); this.name = 'mongo'; this.ttl = opts.leaseTtlMs || 10000; }

  async start(self) {
    const { MongoClient } = require('mongodb');
    this.self = self;
    this.client = new MongoClient(this.opts.mongo.url);
    await this.client.connect();
    const db = this.client.db(this.opts.mongo.db);
    this.leaderCol = db.collection('cluster_leader');
    this.nodesCol = db.collection('cluster_nodes');
    await this.nodesCol.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }).catch(() => {});
    await this._tick();
    this.timer = setInterval(() => this._tick().catch((e) => { this.emit('warn', e); this._checkStale(); }), Math.max(500, this.ttl / 3));
  }

  _checkStale() {
    // could not talk to Mongo: give up leadership once our lease must have expired
    if (this.isLeader && Date.now() - (this.lastRenew || 0) > this.ttl) this._demoted('lost contact with MongoDB');
  }

  async _tick() {
    if (this.stopped) return;
    const s = this.self;
    const now = new Date();
    await this.nodesCol.updateOne({ _id: `${s.role}:${s.nodeId}` },
      { $set: { ...s, campaign: undefined, lastSeen: now.getTime(), leader: this.isLeader, expiresAt: new Date(now.getTime() + this.ttl * 1.5) } }, { upsert: true });
    if (s.campaign) {
      const exp = new Date(now.getTime() + this.ttl);
      if (this.isLeader) {
        const r = await this.leaderCol.updateOne({ _id: 'leader', nodeId: s.nodeId, term: this.term }, { $set: { expiresAt: exp } });
        if (r.matchedCount) this.lastRenew = Date.now(); else this._demoted('leader lease taken over');
      } else {
        try {
          const r = await this.leaderCol.findOneAndUpdate(
            { _id: 'leader', $or: [{ expiresAt: { $lt: now } }, { nodeId: s.nodeId }] },
            { $set: { ...this._selfInfo(), expiresAt: exp }, $inc: { term: 1 } },
            { upsert: true, returnDocument: 'after' });
          const doc = r?.value !== undefined ? r.value : r;
          if (doc && doc.nodeId === s.nodeId) { this.term = doc.term; this.lastRenew = Date.now(); this._elected(doc.term); }
        } catch (e) {
          if (e.code !== 11000) throw e; // duplicate key = someone else holds a live lease
        }
      }
    }
    const cur = await this.leaderCol.findOne({ _id: 'leader' });
    if (!cur || cur.expiresAt < now) this._setLeader(null);
    else this._setLeader({ nodeId: cur.nodeId, grpcAddress: cur.grpcAddress, httpUrl: cur.httpUrl, term: cur.term });
  }

  async register() {}

  async listNodes(role) {
    const q = { expiresAt: { $gt: new Date() } };
    if (role) q.role = role;
    const rows = await this.nodesCol.find(q, { projection: { _id: 0, expiresAt: 0 } }).toArray();
    return rows.map((n) => ({ ...n, leader: n.role === 'master' && n.nodeId === this.leader?.nodeId }));
  }

  async resign() {
    if (!this.isLeader) return;
    await this.leaderCol.updateOne({ _id: 'leader', nodeId: this.self.nodeId }, { $set: { expiresAt: new Date(0) } }).catch(() => {});
    this._demoted('resigned');
  }

  async stop() {
    this.stopped = true;
    clearInterval(this.timer);
    await this.resign();
    await this.nodesCol?.deleteOne({ _id: `${this.self.role}:${this.self.nodeId}` }).catch(() => {});
    await this.client?.close();
  }
}

/**
 * @param {'none'|'etcd'|'mongo'} type
 * @param {{ etcd?, mongo?, leaseTtlMs? }} opts
 */
function createCoordinator(type, opts = {}) {
  if (type === 'etcd') return new EtcdCoordinator(opts);
  if (type === 'mongo') return new MongoCoordinator(opts);
  return new NoneCoordinator(opts);
}

module.exports = { createCoordinator };
