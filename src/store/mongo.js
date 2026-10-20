'use strict';

/**
 * MongoDB store. Logs use a TTL index so MongoDB enforces the retention period
 * itself; purgeLogs() is still provided for immediate clean-up.
 */
class MongoStore {
  constructor(opts, retentionHours) {
    this.name = 'mongo';
    this.opts = opts;
    this.retentionHours = retentionHours;
  }

  async init() {
    const { MongoClient } = require('mongodb');
    this.client = new MongoClient(this.opts.url);
    await this.client.connect();
    const db = this.client.db(this.opts.db);
    this.wf = db.collection('workflows');
    this.inst = db.collection('instances');
    this.act = db.collection('activities');
    this.req = db.collection('requests');
    this.log = db.collection('logs');
    await this.inst.createIndex({ workflowId: 1, createdAt: -1 });
    await this.inst.createIndex({ status: 1 });
    await this.act.createIndex({ instanceId: 1, startedAt: 1 });
    await this.log.createIndex({ instanceId: 1, ts: 1 });
    // TTL index on a Date field; drop & recreate if the retention changed.
    const ttl = Math.max(60, Math.round(this.retentionHours * 3600));
    try { await this.log.createIndex({ at: 1 }, { expireAfterSeconds: ttl, name: 'ttl_at' }); }
    catch (e) {
      await this.log.dropIndex('ttl_at').catch(() => {});
      await this.log.createIndex({ at: 1 }, { expireAfterSeconds: ttl, name: 'ttl_at' });
    }
  }
  async close() { await this.client?.close(); }

  strip(d) { if (d) delete d._id; return d; }

  async saveWorkflow(w) { await this.wf.replaceOne({ id: w.id }, w, { upsert: true }); return w; }
  async getWorkflow(id) { return this.strip(await this.wf.findOne({ id })); }
  async listWorkflows() { return (await this.wf.find().sort({ name: 1 }).toArray()).map((d) => this.strip(d)); }
  async deleteWorkflow(id) { return (await this.wf.deleteOne({ id })).deletedCount > 0; }

  async saveInstance(i) { await this.inst.replaceOne({ instanceId: i.instanceId }, i, { upsert: true }); return i; }
  async getInstance(id) { return this.strip(await this.inst.findOne({ instanceId: id })); }
  async listInstances(f = {}) {
    const q = {};
    if (f.workflowId) q.workflowId = f.workflowId;
    if (f.status) q.status = { $in: String(f.status).split(',') };
    if (f.parentInstanceId) q.parentInstanceId = f.parentInstanceId;
    const total = await this.inst.countDocuments(q);
    const items = await this.inst.find(q).sort({ createdAt: -1 })
      .skip(Number(f.offset || 0)).limit(Number(f.limit || 100)).toArray();
    return { total, items: items.map((d) => this.strip(d)) };
  }
  async deleteInstance(id) {
    await Promise.all([this.inst.deleteOne({ instanceId: id }), this.act.deleteMany({ instanceId: id }), this.req.deleteOne({ instanceId: id })]);
  }

  async saveActivity(a) {
    await this.act.updateOne({ instanceId: a.instanceId, activityId: a.activityId }, { $set: a }, { upsert: true });
    return a;
  }
  async listActivities(instanceId) {
    return (await this.act.find({ instanceId }).sort({ startedAt: 1 }).toArray()).map((d) => this.strip(d));
  }

  async saveRequest(r) { await this.req.replaceOne({ instanceId: r.instanceId }, r, { upsert: true }); return r; }
  async getRequest(id) { return this.strip(await this.req.findOne({ instanceId: id })); }

  async appendLog(e) { await this.log.insertOne({ ...e, at: new Date(e.ts) }); }
  async queryLogs(f = {}) {
    const q = {};
    if (f.instanceId) q.instanceId = f.instanceId;
    if (f.workflowId) q.workflowId = f.workflowId;
    if (f.level) q.level = { $in: String(f.level).split(',') };
    if (f.since || f.until) q.ts = {};
    if (f.since) q.ts.$gte = Number(f.since);
    if (f.until) q.ts.$lte = Number(f.until);
    const rows = await this.log.find(q, { projection: { _id: 0, at: 0 } }).sort({ ts: -1 }).limit(Number(f.limit || 500)).toArray();
    return rows.reverse();
  }
  async purgeLogs(beforeTs) { return (await this.log.deleteMany({ ts: { $lt: beforeTs } })).deletedCount; }
  async purgeInstances(beforeTs, finalStatuses) {
    const old = await this.inst.find({ status: { $in: finalStatuses }, endedAt: { $lt: beforeTs } }, { projection: { instanceId: 1 } }).toArray();
    for (const o of old) await this.deleteInstance(o.instanceId);
    return old.length;
  }
}

module.exports = { MongoStore };
