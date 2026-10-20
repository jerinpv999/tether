'use strict';
const fs = require('fs');
const path = require('path');
const { loadConfig, loadConfigAsync, buildConfig } = require('../config');
const { diffPaths } = require('../cluster/sharedConfig');
const { createStore } = require('../store');
const { createCoordinator } = require('../cluster/coordinator');
const { Logger } = require('./logger');
const { Gateway } = require('./gateway');
const { Runtime } = require('./runtime');
const { Triggers } = require('./triggers');
const { MasterGrpcServer, RemoteRuntime, RuntimeFacade } = require('./cluster');
const { buildApp } = require('./api');

/**
 * Master node: REST API + designer, triggers, broker gateway, gRPC for workers
 * and peers. In cluster mode several masters run behind a load balancer; one is
 * elected leader (schedules work, owns worker links, consumes broker triggers),
 * the others serve the API/hooks and forward runtime calls to it.
 */
async function startMaster(configOrPath) {
  let loaded;
  if (configOrPath && configOrPath.node && configOrPath.root) loaded = { config: configOrPath, shared: null };
  else loaded = await loadConfigAsync('master', configOrPath === undefined ? undefined : configOrPath);
  const { config, shared } = loaded;
  const nodeId = config.node.id;

  const store = await createStore(config);
  const logger = new Logger(store, config.logs, nodeId);
  logger.info(`master ${nodeId} starting – store ${store.name}, cluster ${config.cluster.enabled ? config.cluster.coordinator : 'off'}${config.file ? ', config ' + path.relative(process.cwd(), config.file) : ''}${shared ? `, shared config v${config.sharedVersion} from ${shared.source}` : ''}`);
  logger.startRetention(config.runtime);

  const gateway = new Gateway(config, logger);
  await gateway.init();

  const coordinator = createCoordinator(config.cluster.enabled ? config.cluster.coordinator : 'none', {
    etcd: config.cluster.etcd, mongo: config.cluster.mongo, leaseTtlMs: config.cluster.leaseTtlMs,
  });
  const runtime = new Runtime({ store, logger, gateway, config });
  const remote = new RemoteRuntime({ config, coordinator, logger });
  const facade = new RuntimeFacade(runtime, remote);
  const triggers = new Triggers({ runtime: facade, gateway, logger, sharedGroup: config.mqtt.sharedGroup });

  const grpcServer = new MasterGrpcServer({ config, runtime, coordinator, logger });
  await grpcServer.start();
  if (config.grpc.port === 0) config.grpc.advertiseAddress = config.grpc.advertiseAddress.replace(/:0$/, `:${grpcServer.port}`);

  // ───────────── leadership ─────────────
  coordinator.on('elected', async (term) => {
    logger.info(`elected LEADER (term ${term})`);
    remote.unwatch();
    try {
      await runtime.activate(term);
      await seedExamples(config, store, logger);
      await triggers.sync(store);
      await triggers.setLeader(true);
    } catch (e) { logger.error(`leader activation failed: ${e.message}`); }
  });
  coordinator.on('demoted', async (reason) => {
    logger.warn(`stepped down as leader: ${reason}`);
    runtime.deactivate(reason);
    await triggers.setLeader(false);
    remote.watch();
  });
  coordinator.on('leader', (l) => {
    if (!l) { logger.warn('no leader currently elected'); return; }
    if (l.nodeId !== nodeId) { logger.info(`following leader ${l.nodeId} (${l.grpcAddress}, term ${l.term})`); remote.watch(); }
  });
  coordinator.on('warn', (e) => logger.warn(`coordinator: ${e.message}`));

  await triggers.registerAll(await store.listWorkflows());
  const syncTimer = setInterval(() => triggers.sync(store).catch((e) => logger.warn(`workflow sync failed: ${e.message}`)), config.cluster.syncIntervalMs);
  syncTimer.unref();

  await coordinator.start({ nodeId, role: 'master', grpcAddress: config.grpc.advertiseAddress, httpUrl: config.http.advertiseUrl, campaign: true });

  // ───────────── HTTP ─────────────
  const app = buildApp({ store, runtime: facade, triggers, gateway, logger, config, coordinator, shared });
  const server = await new Promise((r) => { const s = app.listen(config.http.port, config.http.host, () => r(s)); });
  const httpPort = server.address().port;
  logger.info(`HTTP API + designer on http://${config.http.host}:${httpPort}${config.security.api.enabled ? ' (Basic auth)' : ''}`);

  // ───────────── optional in-process worker (single-box installs) ─────────────
  let embedded = null;
  if (config.runtime.embeddedWorker?.enabled) {
    const { Worker } = require('../worker/worker');
    const e = config.runtime.embeddedWorker;
    const wcfg = loadConfig('worker', {
      node: { id: e.id || `${nodeId}-worker`, tags: e.tags || [] },
      grpc: { host: '127.0.0.1', port: e.grpcPort || 0 },
      http: { port: false },
      security: { grpc: config.security.grpc },
      masters: { tls: config.grpc.tls },
      discovery: { type: 'static', static: { masters: [config.grpc.advertiseAddress] } },
      runtime: { maxConcurrent: e.maxConcurrent || 50, childMaxOldSpaceMb: e.childMaxOldSpaceMb || config.runtime.defaultMaxOldSpaceMb },
      logs: { level: config.logs.level },
    });
    embedded = new Worker(wcfg);
    await embedded.start();
  }

  // ───────────── live updates of the cluster-shared config ─────────────
  let syncTimerRef = syncTimer;
  if (shared) {
    shared.on('change', (rec) => {
      if (rec.version === config.sharedVersion) return;
      let next;
      try { next = buildConfig('master', loaded.raw, loaded.file, rec.doc); }
      catch (e) { logger.error(`shared config v${rec.version} rejected – keeping v${config.sharedVersion}: ${e.message}`); return; }
      const changed = diffPaths(strip(config), strip(next));
      const live = [], restart = [];
      for (const p of changed) (LIVE_KEYS.some((k) => p === k || p.startsWith(k + '.')) ? live : restart).push(p);
      // mutate in place – every module holds a reference to these objects
      Object.assign(config.logs, next.logs);
      logger.min = { debug: 10, info: 20, warn: 30, error: 40 }[config.logs.level] ?? 20;
      Object.assign(config.security.grpc, next.security.grpc);
      Object.assign(config.security.api, next.security.api);
      for (const k of ['workerLostGraceMs', 'onWorkerLost', 'syncIntervalMs']) config.cluster[k] = next.cluster[k];
      for (const k of ['terminateGraceMs', 'defaultMaxOldSpaceMb', 'instanceRetentionHours']) config.runtime[k] = next.runtime[k];
      if (changed.includes('cluster.syncIntervalMs')) {
        clearInterval(syncTimerRef);
        syncTimerRef = setInterval(() => triggers.sync(store).catch(() => {}), config.cluster.syncIntervalMs); syncTimerRef.unref();
      }
      config.sharedVersion = rec.version;
      logger.info(`shared config v${rec.version} (by ${rec.updatedBy}) applied${live.length ? ': ' + live.join(', ') : ' – no live changes'}`);
      if (restart.length) logger.warn(`shared config v${rec.version}: restart this master to apply ${restart.join(', ')}`);
    });
    shared.on('error', (e) => logger.warn(`shared config watch: ${e.message}`));
    await shared.watch();
  }

  let closing = false;
  async function stop() {
    if (closing) return; closing = true;
    logger.info('shutting down');
    clearInterval(syncTimerRef);
    await shared?.close().catch(() => {});
    server.close();
    if (embedded) await embedded.stop({ graceful: false });
    await triggers.stop();
    await coordinator.stop().catch(() => {});   // resign → a peer takes over quickly
    runtime.deactivate('shutdown');
    remote.unwatch();
    await grpcServer.stop();
    await gateway.close();
    logger.stop();
    await store.close();
  }
  return { config, app, server, httpPort, grpcPort: grpcServer.port, runtime, coordinator, stop };
}

// settings a running master can change without a restart
const LIVE_KEYS = ['logs', 'security.grpc', 'security.api', 'cluster.workerLostGraceMs', 'cluster.onWorkerLost', 'cluster.syncIntervalMs',
  'runtime.terminateGraceMs', 'runtime.defaultMaxOldSpaceMb', 'runtime.instanceRetentionHours'];
const strip = ({ root, file, sharedVersion, shared, ...rest }) => rest;

async function seedExamples(config, store, logger) {
  if (!config.seedExamples || (await store.listWorkflows()).length) return;
  const dir = path.join(config.root, 'examples');
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith('.json'))) {
    const wf = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8'));
    await store.saveWorkflow({ ...wf, version: 1, createdAt: Date.now(), updatedAt: Date.now() });
    logger.info(`seeded example workflow ${wf.id}`);
  }
}

module.exports = { startMaster };
