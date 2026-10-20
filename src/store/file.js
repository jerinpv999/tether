'use strict';
const fs = require('fs');
const path = require('path');
const { MemoryStore } = require('./memory');

/**
 * JSON-file store: keeps everything in memory and flushes each collection to
 * <dir>/<collection>.json (debounced, atomic rename). Workflow definitions are
 * additionally written one-per-file in <dir>/workflows/<id>.json so they can be
 * versioned in git or edited by hand.
 */
class FileStore extends MemoryStore {
  constructor(opts) {
    super();
    this.name = 'file';
    this.dir = opts.dir;
    this.dirty = new Set();
    this.timer = null;
  }

  async init() {
    fs.mkdirSync(path.join(this.dir, 'workflows'), { recursive: true });
    const wfDir = path.join(this.dir, 'workflows');
    for (const f of fs.readdirSync(wfDir)) {
      if (!f.endsWith('.json')) continue;
      try {
        const wf = JSON.parse(fs.readFileSync(path.join(wfDir, f), 'utf8'));
        if (wf.id) this.workflows.set(wf.id, wf);
      } catch (e) { console.error(`[store] bad workflow file ${f}: ${e.message}`); }
    }
    const load = (name) => {
      const p = path.join(this.dir, `${name}.json`);
      return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null;
    };
    for (const i of load('instances') || []) this.instances.set(i.instanceId, i);
    for (const [iid, acts] of Object.entries(load('activities') || {})) {
      this.activities.set(iid, new Map(acts.map((a) => [a.activityId, a])));
    }
    for (const r of load('requests') || []) this.requests.set(r.instanceId, r);
    this.logs = load('logs') || [];
  }

  _changed(collection) {
    this.dirty.add(collection);
    if (!this.timer) this.timer = setTimeout(() => this.flush(), 250);
  }

  _write(name, data) {
    const p = path.join(this.dir, `${name}.json`);
    fs.writeFileSync(p + '.tmp', JSON.stringify(data));
    fs.renameSync(p + '.tmp', p);
  }

  flush() {
    clearTimeout(this.timer); this.timer = null;
    const dirty = [...this.dirty]; this.dirty.clear();
    for (const c of dirty) {
      if (c === 'workflows') {
        const wfDir = path.join(this.dir, 'workflows');
        const keep = new Set();
        for (const wf of this.workflows.values()) {
          const file = `${safe(wf.id)}.json`; keep.add(file);
          fs.writeFileSync(path.join(wfDir, file), JSON.stringify(wf, null, 2));
        }
        for (const f of fs.readdirSync(wfDir)) if (f.endsWith('.json') && !keep.has(f)) fs.unlinkSync(path.join(wfDir, f));
      } else if (c === 'instances') this._write('instances', [...this.instances.values()]);
      else if (c === 'activities') {
        const o = {}; for (const [k, m] of this.activities) o[k] = [...m.values()];
        this._write('activities', o);
      } else if (c === 'requests') this._write('requests', [...this.requests.values()]);
      else if (c === 'logs') this._write('logs', this.logs);
    }
  }

  async close() { this.flush(); }
}

const safe = (s) => String(s).replace(/[^a-zA-Z0-9_.-]/g, '_');

module.exports = { FileStore };
