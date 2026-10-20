'use strict';

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

/**
 * Structured logger: writes to the configured store (so logs are queryable per
 * workflow / instance through the API) and optionally to the console.
 * A purge job enforces the configurable retention period.
 */
class Logger {
  constructor(store, cfg, nodeId) {
    this.nodeId = nodeId || null;
    this.store = store;
    this.cfg = cfg;
    this.min = LEVELS[cfg.level] ?? 20;
    this.timer = null;
  }

  log(level, message, ctx = {}) {
    if ((LEVELS[level] ?? 20) < this.min) return;
    const entry = {
      ts: Date.now(),
      level,
      message: String(message),
      workflowId: ctx.workflowId || null,
      instanceId: ctx.instanceId || null,
      nodeId: ctx.nodeId || null,
      source: ctx.source || 'master',
      node: this.nodeId,
    };
    if (ctx.data !== undefined) entry.data = ctx.data;
    if (this.cfg.console) {
      const tag = [entry.workflowId, entry.instanceId && entry.instanceId.slice(0, 8), entry.nodeId].filter(Boolean).join('/');
      const line = `${new Date(entry.ts).toISOString()} ${level.toUpperCase().padEnd(5)} <${this.nodeId}> ${tag ? '[' + tag + '] ' : ''}${entry.message}`;
      (level === 'error' ? console.error : console.log)(line);
    }
    this.store.appendLog(entry).catch((e) => console.error('[logger] store write failed:', e.message));
  }
  debug(m, c) { this.log('debug', m, c); }
  info(m, c) { this.log('info', m, c); }
  warn(m, c) { this.log('warn', m, c); }
  error(m, c) { this.log('error', m, c); }

  startRetention(runtimeCfg) {
    const run = async () => {
      const cutoff = Date.now() - this.cfg.retentionHours * 3600 * 1000;
      try {
        const n = await this.store.purgeLogs(cutoff);
        if (n) this.info(`retention: purged ${n} log entries older than ${this.cfg.retentionHours}h`);
        if (runtimeCfg?.instanceRetentionHours > 0) {
          const ic = Date.now() - runtimeCfg.instanceRetentionHours * 3600 * 1000;
          const m = await this.store.purgeInstances(ic, ['COMPLETED', 'FAILED', 'STOPPED', 'TERMINATED']);
          if (m) this.info(`retention: purged ${m} finished instances`);
        }
      } catch (e) { console.error('[logger] retention purge failed:', e.message); }
    };
    run();
    this.timer = setInterval(run, this.cfg.purgeIntervalMs);
    this.timer.unref();
  }

  setRetention(hours) { this.cfg.retentionHours = Number(hours); }
  stop() { clearInterval(this.timer); }
}

module.exports = { Logger };
