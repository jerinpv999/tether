'use strict';

const clone = (v) => (v === undefined ? undefined : structuredClone(v));

function matchInstance(i, f) {
  if (f.workflowId && i.workflowId !== f.workflowId) return false;
  if (f.status) {
    const set = Array.isArray(f.status) ? f.status : String(f.status).split(',');
    if (!set.includes(i.status)) return false;
  }
  if (f.parentInstanceId && i.parentInstanceId !== f.parentInstanceId) return false;
  return true;
}

function matchLog(l, f) {
  if (f.instanceId && l.instanceId !== f.instanceId) return false;
  if (f.workflowId && l.workflowId !== f.workflowId) return false;
  if (f.level) {
    const set = String(f.level).split(',');
    if (!set.includes(l.level)) return false;
  }
  if (f.since && l.ts < Number(f.since)) return false;
  if (f.until && l.ts > Number(f.until)) return false;
  return true;
}

/**
 * In-memory store. Also the base class for the JSON file store.
 * Every store implements the same async interface:
 *   workflows : saveWorkflow, getWorkflow, listWorkflows, deleteWorkflow
 *   instances : saveInstance, getInstance, listInstances, deleteInstance
 *   activities: saveActivity, listActivities
 *   requests  : saveRequest, getRequest
 *   logs      : appendLog, queryLogs, purgeLogs
 */
class MemoryStore {
  constructor() {
    this.name = 'memory';
    this.workflows = new Map();
    this.instances = new Map();
    this.activities = new Map(); // instanceId -> Map(activityId -> activity)
    this.requests = new Map();   // instanceId -> request
    this.logs = [];
  }

  async init() {}
  async close() {}
  _changed(_collection) {}

  // workflows
  async saveWorkflow(wf) { this.workflows.set(wf.id, clone(wf)); this._changed('workflows'); return wf; }
  async getWorkflow(id) { return clone(this.workflows.get(id)) || null; }
  async listWorkflows() {
    return [...this.workflows.values()].map(clone).sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id));
  }
  async deleteWorkflow(id) { const ok = this.workflows.delete(id); this._changed('workflows'); return ok; }

  // instances
  async saveInstance(inst) { this.instances.set(inst.instanceId, clone(inst)); this._changed('instances'); return inst; }
  async getInstance(id) { return clone(this.instances.get(id)) || null; }
  async listInstances(f = {}) {
    const limit = Number(f.limit || 100), offset = Number(f.offset || 0);
    const all = [...this.instances.values()].filter((i) => matchInstance(i, f))
      .sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
    return { total: all.length, items: all.slice(offset, offset + limit).map(clone) };
  }
  async deleteInstance(id) {
    this.instances.delete(id); this.activities.delete(id); this.requests.delete(id);
    this._changed('instances'); this._changed('activities'); this._changed('requests');
  }

  // activities
  async saveActivity(a) {
    if (!this.activities.has(a.instanceId)) this.activities.set(a.instanceId, new Map());
    const m = this.activities.get(a.instanceId);
    m.set(a.activityId, { ...(m.get(a.activityId) || {}), ...clone(a) });
    this._changed('activities');
    return a;
  }
  async listActivities(instanceId) {
    const m = this.activities.get(instanceId);
    if (!m) return [];
    return [...m.values()].map(clone).sort((a, b) => (a.startedAt || 0) - (b.startedAt || 0));
  }

  // incoming request payloads
  async saveRequest(r) { this.requests.set(r.instanceId, clone(r)); this._changed('requests'); return r; }
  async getRequest(instanceId) { return clone(this.requests.get(instanceId)) || null; }

  // logs
  async appendLog(entry) { this.logs.push(entry); this._changed('logs'); }
  async queryLogs(f = {}) {
    const limit = Number(f.limit || 500);
    const out = [];
    for (let i = this.logs.length - 1; i >= 0 && out.length < limit; i--) {
      if (matchLog(this.logs[i], f)) out.push(this.logs[i]);
    }
    return out.reverse();
  }
  async purgeLogs(beforeTs) {
    const n = this.logs.length;
    this.logs = this.logs.filter((l) => l.ts >= beforeTs);
    if (n !== this.logs.length) this._changed('logs');
    return n - this.logs.length;
  }
  async purgeInstances(beforeTs, finalStatuses) {
    let n = 0;
    for (const i of [...this.instances.values()]) {
      if (finalStatuses.includes(i.status) && (i.endedAt || 0) < beforeTs) { await this.deleteInstance(i.instanceId); n++; }
    }
    return n;
  }
}

module.exports = { MemoryStore, matchInstance, matchLog };
