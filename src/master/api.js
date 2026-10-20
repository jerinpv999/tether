'use strict';
const express = require('express');
const path = require('path');
const { validateWorkflow } = require('../shared/validate');
const { nodeTypes, triggerTypes } = require('../shared/nodeTypes');
const { resolveInstanceId, redactHeaders } = require('./triggers');
const { FINAL } = require('./runtime');
const { parseBasic, safeEqual } = require('../grpc/common');

const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

function buildApp({ store, runtime, triggers, gateway, logger, config, coordinator, shared }) {
  const app = express();
  app.disable('x-powered-by');

  // ───────────── optional Basic auth for the REST API + designer ─────────────
  // installed always and read live: enabling it through the shared config takes effect immediately
  {
    app.use((req, res, next) => {
      const apiAuth = config.security.api;
      if (!apiAuth?.enabled) return next();
      if (req.path.startsWith('/hooks/') && !apiAuth.protectHooks) return next();
      if (req.path === '/api/health') return next(); // load-balancer probes
      const c = parseBasic(req.headers.authorization);
      if (c && safeEqual(c.username, apiAuth.username) & safeEqual(c.password, apiAuth.password)) { req.user = c.username; return next(); }
      res.set('WWW-Authenticate', 'Basic realm="FlowForge", charset="UTF-8"').status(401).json({ error: 'authentication required' });
    });
  }
  app.use(express.json({ limit: '10mb' }));
  app.use(express.text({ type: ['text/*', 'application/xml'], limit: '10mb' }));

  // ───────────── designer UI ─────────────
  app.use('/vendor/fabric.min.js', (req, res) => res.sendFile(require.resolve('fabric/dist/fabric.min.js')));
  app.use(express.static(path.join(config.root, 'public')));

  const api = express.Router();

  // ───────────── meta ─────────────
  api.get('/health', wrap(async (req, res) => res.json({
    ok: true, nodeId: config.node.id, role: 'master', leader: coordinator.isLeader, leaderId: coordinator.leader?.nodeId || null,
    term: coordinator.leader?.term || 0, store: store.name, coordinator: coordinator.name, uptimeSec: Math.round(process.uptime()),
  })));

  // Cluster view: masters (from discovery) and workers (live, from the leader)
  api.get('/cluster', wrap(async (req, res) => {
    const [masters, workerNodes] = await Promise.all([coordinator.listNodes('master'), coordinator.listNodes('worker')]);
    let live = null, liveError = null;
    try { live = await runtime.stats(); } catch (e) { liveError = e.message; }
    const liveWorkers = new Map((live?.workers || []).map((w) => [w.id, w]));
    const workers = [...new Set([...liveWorkers.keys(), ...workerNodes.map((w) => w.nodeId)])].map((id) => {
      const d = workerNodes.find((w) => w.nodeId === id) || {};
      return { id, ...d, ...(liveWorkers.get(id) || { connected: false }) };
    });
    res.json({
      self: { nodeId: config.node.id, isLeader: coordinator.isLeader, grpcAddress: config.grpc.advertiseAddress, httpUrl: config.http.advertiseUrl },
      coordinator: coordinator.name, leader: coordinator.leader, term: live?.term ?? coordinator.leader?.term ?? 0,
      masters: masters.map((m) => ({ ...m, leader: m.nodeId === coordinator.leader?.nodeId })),
      workers, running: live?.running ?? null, queued: live?.queued ?? null, orphaned: live?.orphaned ?? [], error: liveError,
    });
  }));
  api.get('/node-types', (req, res) => res.json({ nodeTypes, triggerTypes }));

  // ───────────── workflows ─────────────
  api.get('/workflows', wrap(async (req, res) => {
    const list = await store.listWorkflows();
    res.json(req.query.full ? list : list.map(({ nodes, edges, ...m }) => ({ ...m, nodeCount: nodes?.length || 0 })));
  }));
  api.get('/workflows/:id', wrap(async (req, res) => {
    const wf = await store.getWorkflow(req.params.id);
    if (!wf) return res.status(404).json({ error: 'workflow not found' });
    res.json(wf);
  }));
  api.post('/workflows/validate', (req, res) => { const errors = validateWorkflow(req.body); res.json({ valid: !errors.length, errors }); });

  const save = async (req, res, creating) => {
    const body = { ...req.body };
    if (!creating) body.id = req.params.id;
    const errors = validateWorkflow(body);
    if (errors.length) return res.status(400).json({ error: 'invalid workflow', errors });
    const prev = await store.getWorkflow(body.id);
    if (creating && prev) return res.status(409).json({ error: `workflow "${body.id}" already exists` });
    const now = Date.now();
    const wf = {
      id: body.id, name: body.name || body.id, description: body.description || '',
      enabled: body.enabled !== false, triggers: body.triggers || [], settings: body.settings || {},
      nodes: body.nodes, edges: body.edges || [],
      version: (prev?.version || 0) + 1, createdAt: prev?.createdAt || now, updatedAt: now,
    };
    await store.saveWorkflow(wf);
    await triggers.register(wf);
    logger.info(`workflow saved (v${wf.version})`, { workflowId: wf.id });
    res.status(creating ? 201 : 200).json(wf);
  };
  api.post('/workflows', wrap((req, res) => save(req, res, true)));
  api.put('/workflows/:id', wrap((req, res) => save(req, res, false)));
  api.delete('/workflows/:id', wrap(async (req, res) => {
    await triggers.remove(req.params.id);
    const ok = await store.deleteWorkflow(req.params.id);
    if (ok) logger.info('workflow deleted', { workflowId: req.params.id });
    res.status(ok ? 204 : 404).end();
  }));

  // Start a workflow. Body: { instanceId?, input?, wait?: 'response'|'finish'|true, timeoutMs? }
  api.post('/workflows/:id/start', wrap(async (req, res) => {
    const { instanceId, input, wait, timeoutMs } = req.body || {};
    const t = Number(timeoutMs) || config.http.syncTimeoutMs;
    const r = await runtime.startAndWait(req.params.id, {
      instanceId, input: input ?? {}, acceptedBy: config.node.id,
      trigger: { type: 'api' },
      request: { trigger: { type: 'api', method: req.method, path: req.originalUrl, headers: redactHeaders(req.headers), ip: req.ip, via: config.node.id }, body: input ?? {} },
    }, wait === 'finish' ? 'finish' : wait ? 'response' : null, t);
    if (r.finished) return res.json(r.finished);
    if (r.response) { res.set(r.response.headers || {}); return res.status(r.response.statusCode).json(r.response.body); }
    res.status(202).json(r.instance);
  }));

  // ───────────── instances ─────────────
  api.get('/instances', wrap(async (req, res) => {
    const r = await store.listInstances(req.query);
    res.json({ total: r.total, items: r.items.map(({ definition, ...i }) => i) });
  }));
  const getInst = async (req, res) => {
    const i = await store.getInstance(req.params.id);
    if (!i) { res.status(404).json({ error: 'instance not found' }); return null; }
    return i;
  };
  api.get('/instances/:id', wrap(async (req, res) => {
    const i = await getInst(req, res); if (!i) return;
    const { definition, ...rest } = i;
    if (req.query.include === 'definition') rest.definition = definition;
    if (req.query.include === 'all') {
      rest.definition = definition;
      rest.activities = await store.listActivities(i.instanceId);
      rest.request = await store.getRequest(i.instanceId);
    }
    res.json(rest);
  }));
  api.get('/instances/:id/status', wrap(async (req, res) => {
    const i = await getInst(req, res); if (!i) return;
    const acts = await store.listActivities(i.instanceId);
    const counts = acts.reduce((m, a) => ({ ...m, [a.status]: (m[a.status] || 0) + 1 }), {});
    res.json({
      instanceId: i.instanceId, workflowId: i.workflowId, status: i.status, currentNodes: i.currentNodes,
      error: i.error, createdAt: i.createdAt, startedAt: i.startedAt, endedAt: i.endedAt, updatedAt: i.updatedAt,
      durationMs: i.startedAt ? (i.endedAt || Date.now()) - i.startedAt : 0, activities: counts, pid: i.pid,
    });
  }));
  api.get('/instances/:id/activities', wrap(async (req, res) => {
    const i = await getInst(req, res); if (!i) return;
    res.json(await store.listActivities(i.instanceId));
  }));
  api.get('/instances/:id/activities/:activityId', wrap(async (req, res) => {
    const a = (await store.listActivities(req.params.id)).find((x) => x.activityId === req.params.activityId);
    a ? res.json(a) : res.status(404).json({ error: 'activity not found' });
  }));
  api.get('/instances/:id/request', wrap(async (req, res) => {
    const r = await store.getRequest(req.params.id);
    r ? res.json(r) : res.status(404).json({ error: 'no request recorded' });
  }));
  api.get('/instances/:id/logs', wrap(async (req, res) => res.json(await store.queryLogs({ ...req.query, instanceId: req.params.id }))));
  api.get('/instances/:id/children', wrap(async (req, res) => {
    const r = await store.listInstances({ parentInstanceId: req.params.id, limit: 1000 });
    res.json({ total: r.total, items: r.items.map(({ definition, ...i }) => i) });
  }));
  api.get('/instances/:id/wait', wrap(async (req, res) => res.json(await runtime.waitForFinish(req.params.id, Number(req.query.timeoutMs) || config.http.syncTimeoutMs))));

  for (const action of ['stop', 'terminate', 'pause', 'resume']) {
    api.post(`/instances/:id/${action}`, wrap(async (req, res) => res.json(await runtime.control(req.params.id, action, { reason: req.body?.reason }))));
  }
  api.post('/instances/:id/signal', wrap(async (req, res) => {
    const { name, payload } = req.body || {};
    res.json(await runtime.control(req.params.id, 'SIGNAL', { signalName: name, payload }));
  }));
  api.post('/instances/:id/retry', wrap(async (req, res) => res.status(202).json(await runtime.retry(req.params.id, req.body?.instanceId))));
  api.delete('/instances/:id', wrap(async (req, res) => {
    const i = await getInst(req, res); if (!i) return;
    if (!FINAL.includes(i.status)) return res.status(409).json({ error: `instance is ${i.status}; stop or terminate it first` });
    await store.deleteInstance(i.instanceId);
    res.status(204).end();
  }));

  // ───────────── logs ─────────────
  api.get('/logs', wrap(async (req, res) => res.json(await store.queryLogs(req.query))));
  api.get('/settings/log-retention', (req, res) => res.json({ retentionHours: config.logs.retentionHours }));
  api.put('/settings/log-retention', wrap(async (req, res) => {
    const h = Number(req.body?.retentionHours);
    if (!(h > 0)) return res.status(400).json({ error: 'retentionHours must be > 0' });
    if (shared) {
      // cluster: persist in the shared config so every master applies it (via the watch)
      const rec = await shared.get();
      const doc = structuredClone(rec?.doc || {});
      doc.master = { ...(doc.master || {}), logs: { ...(doc.master?.logs || {}), retentionHours: h } };
      const version = await shared.put(doc, `${req.user || 'api'}@${config.node.id}`, rec?.version ?? 0);
      logger.setRetention(h);
      const purged = await store.purgeLogs(Date.now() - h * 3600 * 1000);
      logger.info(`log retention set to ${h}h in shared config v${version}`);
      return res.json({ retentionHours: h, purged, sharedVersion: version });
    }
    logger.setRetention(h);
    const purged = await store.purgeLogs(Date.now() - h * 3600 * 1000);
    logger.info(`log retention set to ${h}h`);
    res.json({ retentionHours: h, purged });
  }));
  api.post('/logs/purge', wrap(async (req, res) => {
    const before = Number(req.body?.before) || Date.now() - config.logs.retentionHours * 3600 * 1000;
    res.json({ purged: await store.purgeLogs(before) });
  }));

  // ───────────── cluster-shared configuration (etcd / MongoDB) ─────────────
  api.get('/cluster/config', wrap(async (req, res) => {
    const { redact } = require('../cluster/sharedConfig');
    const { root, file, ...effective } = config;
    if (!shared) return res.json({ source: null, message: 'this node has no shared: block – configuration is local YAML only', effective: redact(effective) });
    const rec = await shared.get();
    res.json({ source: shared.source, version: rec?.version || 0, appliedVersion: config.sharedVersion || 0, updatedAt: rec?.updatedAt, updatedBy: rec?.updatedBy, doc: redact(rec?.doc || null), effective: redact(effective) });
  }));
  api.put('/cluster/config', wrap(async (req, res) => {
    if (!shared) return res.status(409).json({ error: 'this node has no shared: block' });
    if (!config.security.api?.enabled) return res.status(403).json({ error: 'enable security.api (Basic auth) to change the shared config over HTTP, or use: node bin/flowforge.js config push' });
    const { doc, version } = req.body || {};
    if (!doc || typeof doc !== 'object') return res.status(400).json({ error: 'body must be { doc: { common, master, worker }, version }' });
    const { validateSharedDoc } = require('../cluster/sharedTools');
    const errors = validateSharedDoc(doc);
    if (errors.length) return res.status(400).json({ error: 'invalid shared config', errors });
    const v = await shared.put(doc, `${req.user || 'api'}@${config.node.id}`, version);
    logger.info(`shared config v${v} written by ${req.user || 'api'}`);
    res.json({ version: v });
  }));

  // ───────────── data written by db.write (shared store in cluster mode) ─────────────
  api.get('/data', wrap(async (req, res) => res.json({ backend: config.dataDb.type, collections: await gateway.dbCollections() })));
  api.get('/data/:collection', wrap(async (req, res) => res.json(await gateway.dbList(req.params.collection, { limit: Number(req.query.limit) || 100 }))));

  // ───────────── events (inject / test) ─────────────
  api.post('/events/publish', wrap(async (req, res) => {
    const { source, ...rest } = req.body || {};
    if (source === 'rabbitmq') return res.json(await gateway.publishRabbit(rest));
    if (source === 'mqtt') return res.json(await gateway.publishMqtt(rest));
    res.status(400).json({ error: 'source must be rabbitmq or mqtt' });
  }));

  // ───────────── live updates (Server-Sent Events) ─────────────
  api.get('/stream', (req, res) => {
    res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.flushHeaders();
    const filter = req.query.instanceId;
    const send = (type) => (data) => { if (!filter || data.instanceId === filter || data.instance_id === filter) res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`); };
    const onI = send('instance'), onA = send('activity');
    runtime.on('instance', onI); runtime.on('activity', onA);
    const ping = setInterval(() => res.write(': ping\n\n'), 20000);
    req.on('close', () => { clearInterval(ping); runtime.off('instance', onI); runtime.off('activity', onA); });
  });

  app.use('/api', api);

  // ───────────── HTTP triggers ─────────────
  app.all('/hooks/*', wrap(async (req, res) => {
    const sub = req.path.replace(/^\/hooks/, '') || '/';
    const m = triggers.matchHttp(req.method, sub);
    if (!m) return res.status(404).json({ error: `no workflow trigger for ${req.method} ${sub}` });
    const t = m.trigger;
    const body = req.method === 'GET' ? {} : (req.body ?? {});
    const src = { body, headers: req.headers, query: req.query, params: m.params };
    const instanceId = resolveInstanceId(t.instanceIdFrom, src) || req.get('x-workflow-instance-id') || req.query.instanceId || undefined;
    const trig = { type: 'http', method: req.method, path: sub, params: m.params, query: req.query };
    const input = req.method === 'GET' ? { ...req.query, ...m.params } : (typeof body === 'object' && !Array.isArray(body) ? { ...m.params, ...body } : body);
    const sync = t.sync !== false;
    const out = await runtime.startAndWait(m.workflowId, {
      instanceId, input, trigger: trig, acceptedBy: config.node.id,
      request: { trigger: { ...trig, headers: redactHeaders(req.headers), ip: req.ip, via: config.node.id }, body, query: req.query, params: m.params },
    }, sync ? 'response' : null, Number(t.responseTimeoutMs) || config.http.syncTimeoutMs);
    const inst = out.instance;
    res.set('x-workflow-instance-id', inst.instanceId);
    res.set('x-flowforge-node', config.node.id);
    if (!sync) return res.status(202).json({ instanceId: inst.instanceId, status: inst.status });
    const r = out.response;
    res.set(r.headers || {});
    if (!res.get('content-type')) res.type(typeof r.body === 'string' ? 'text' : 'json');
    res.status(r.statusCode).send(typeof r.body === 'string' ? r.body : JSON.stringify(r.body ?? null));
  }));

  // ───────────── errors ─────────────
  // eslint-disable-next-line no-unused-vars
  app.use((err, req, res, next) => {
    const status = err.status || err.statusCode || 500;
    if (status >= 500) logger.error(`${req.method} ${req.originalUrl}: ${err.stack || err.message}`);
    res.status(status).json({ error: err.message });
  });

  return app;
}

module.exports = { buildApp };
