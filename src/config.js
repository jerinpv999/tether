'use strict';
/*
 * YAML configuration for master and worker nodes.
 *
 *   node bin/master.js --config config/master-1.yaml
 *   node bin/worker.js --config config/worker-1.yaml
 *
 * Any string value may reference environment variables: ${VAR} or ${VAR:-default}
 * (keep passwords out of the files: password: ${FLOWFORGE_GRPC_PASSWORD}).
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const YAML = require('yaml');

const root = path.resolve(__dirname, '..');

const MASTER_DEFAULTS = {
  node: { id: `master-${os.hostname()}`, role: 'master' },
  http: { host: '0.0.0.0', port: 3000, advertiseUrl: '', syncTimeoutMs: 30000 },
  grpc: { host: '0.0.0.0', port: 50051, advertiseAddress: '', tls: { enabled: false } },
  security: {
    grpc: { username: 'flowforge', password: 'change-me', additionalUsers: [] },
    api: { enabled: false, username: 'admin', password: 'admin', protectHooks: false },
  },
  cluster: {
    enabled: false,
    coordinator: 'none',        // none | etcd | mongo
    leaseTtlMs: 10000,
    workerLostGraceMs: 15000,   // how long a disconnected worker may take to re-attach
    onWorkerLost: 'fail',       // fail | restart  (restart = re-run from the beginning on another worker)
    syncIntervalMs: 5000,       // how often masters reload workflow definitions (trigger sync)
    etcd: null,                 // defaults to store.etcd
    mongo: null,                // defaults to store.mongo
  },
  store: {
    type: 'file',               // memory | file | mongo | etcd  (cluster mode needs mongo or etcd)
    file: { dir: './data' },
    mongo: { url: 'mongodb://127.0.0.1:27017', db: 'flowforge' },
    etcd: { hosts: ['127.0.0.1:2379'], prefix: '/flowforge/' },
  },
  rabbitmq: { url: '', prefetch: 10 },
  mqtt: { url: '', sharedGroup: '', options: {} },
  // Target of the db.write activity. auto = mongoUrl if set, else the shared store
  // (mongo → same server, etcd → keys under <prefix>data/), else JSON files (single node only).
  dataDb: { type: 'auto', mongoUrl: '', mongoDb: 'flowforge_data', fileDir: './data/db', etcdPrefix: '' },
  logs: { retentionHours: 168, purgeIntervalMs: 3600000, level: 'info', console: true },
  runtime: {
    instanceRetentionHours: 0,
    defaultMaxOldSpaceMb: 256,
    terminateGraceMs: 3000,
    embeddedWorker: { enabled: false }, // run a worker inside the master process (single-box installs)
  },
  seedExamples: true,
};

const WORKER_DEFAULTS = {
  node: { id: `worker-${os.hostname()}`, role: 'worker', tags: [] },
  grpc: { host: '127.0.0.1', port: 50061 },        // local server for this worker's child processes
  http: { host: '0.0.0.0', port: 3101, advertiseUrl: '' }, // health / metrics
  security: { grpc: { username: 'flowforge', password: 'change-me' } },
  masters: { tls: { enabled: false } },            // TLS used when dialling masters
  discovery: {
    type: 'static',                                  // static | etcd | mongo
    static: { masters: ['127.0.0.1:50051'] },
    etcd: { hosts: ['127.0.0.1:2379'], prefix: '/flowforge/' },
    mongo: { url: 'mongodb://127.0.0.1:27017', db: 'flowforge' },
    heartbeatMs: 5000,
    ttlMs: 15000,
  },
  runtime: {
    maxConcurrent: 50,
    childMaxOldSpaceMb: 256,
    terminateGraceMs: 3000,
    reconnectMs: 1000,
    bufferLimit: 50000,        // messages kept while no leader is reachable
    drainTimeoutMs: 60000,     // on SIGTERM: stop taking work, wait for running workflows
  },
  logs: { level: 'info' },
};

const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v);
function merge(base, over) {
  if (!isObj(over)) return over === undefined ? structuredClone(base) : over;
  const out = isObj(base) ? structuredClone(base) : {};
  for (const [k, v] of Object.entries(over)) out[k] = isObj(v) && isObj(out[k]) ? merge(out[k], v) : v;
  return out;
}

const ENV_RE = /\$\{([A-Z0-9_]+)(?::-(.*?))?\}/gi;
function substitute(v) {
  if (typeof v === 'string') {
    const whole = /^\$\{([A-Z0-9_]+)(?::-(.*?))?\}$/i.exec(v);
    const rep = (_, name, def) => (process.env[name] !== undefined ? process.env[name] : def !== undefined ? def : '');
    const s = v.replace(ENV_RE, rep);
    if (whole && /^-?\d+(\.\d+)?$/.test(s)) return Number(s); // ${PORT:-3000} → number
    if (whole && (s === 'true' || s === 'false')) return s === 'true';
    return s;
  }
  if (Array.isArray(v)) return v.map(substitute);
  if (isObj(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, substitute(x)]));
  return v;
}

const abs = (p) => (p && !path.isAbsolute(p) ? path.resolve(root, p) : p);

function argConfigPath(argv = process.argv) {
  const i = argv.indexOf('--config');
  if (i >= 0 && argv[i + 1]) return argv[i + 1];
  const eq = argv.find((a) => a.startsWith('--config='));
  return eq ? eq.slice(9) : process.env.FLOWFORGE_CONFIG || '';
}

function readLocal(source) {
  if (typeof source === 'string' && source) {
    const file = path.resolve(source);
    return { raw: YAML.parse(fs.readFileSync(file, 'utf8')) || {}, file };
  }
  if (isObj(source)) return { raw: source, file: null };
  return { raw: {}, file: null };
}

/**
 * Build the effective config. Precedence (later wins):
 *   built-in defaults ← shared.common ← shared.<role> ← local YAML
 * Env references ${VAR} / ${VAR:-default} are resolved on the merged result.
 */
function buildConfig(role, raw, file, sharedDoc) {
  const base = role === 'worker' ? WORKER_DEFAULTS : MASTER_DEFAULTS;
  const layered = merge(merge(merge(base, sharedDoc?.common || {}), sharedDoc?.[role] || {}), raw);
  const cfg = substitute(layered);
  cfg.root = root;
  cfg.file = file;
  cfg.node.role = role;
  cfg.shared = raw.shared ? substitute(raw.shared) : null;
  validate(role, cfg);
  for (const tls of [cfg.grpc?.tls, cfg.masters?.tls]) if (tls) for (const k of ['certFile', 'keyFile', 'caFile']) tls[k] = abs(tls[k]);
  if (role === 'master') {
    cfg.store.file.dir = abs(cfg.store.file.dir);
    cfg.dataDb.fileDir = abs(cfg.dataDb.fileDir);
    cfg.grpc.advertiseAddress = cfg.grpc.advertiseAddress || `${cfg.grpc.host === '0.0.0.0' ? '127.0.0.1' : cfg.grpc.host}:${cfg.grpc.port}`;
    cfg.http.advertiseUrl = cfg.http.advertiseUrl || `http://${cfg.http.host === '0.0.0.0' ? '127.0.0.1' : cfg.http.host}:${cfg.http.port}`;
    cfg.cluster.etcd = cfg.cluster.etcd || cfg.store.etcd;
    cfg.cluster.mongo = cfg.cluster.mongo || cfg.store.mongo;
    resolveDataDb(cfg);
    // legacy flat names used by gateway / logger
    cfg.rabbitmq.enabled = !!cfg.rabbitmq.url;
    cfg.mqtt.enabled = !!cfg.mqtt.url;
  } else {
    cfg.http.advertiseUrl = cfg.http.advertiseUrl || (cfg.http.port ? `http://${os.hostname()}:${cfg.http.port}` : '');
  }
  return cfg;
}

/** Decide where db.write data goes; in cluster mode it must be shared (never local files). */
function resolveDataDb(cfg) {
  const d = cfg.dataDb;
  let t = d.type || 'auto';
  if (t === 'auto') t = d.mongoUrl ? 'mongo' : ['mongo', 'etcd'].includes(cfg.store.type) ? cfg.store.type : 'file';
  d.type = t;
  if (t === 'mongo' && !d.mongoUrl) d.mongoUrl = cfg.store.mongo.url;
  if (t === 'etcd') { d.etcd = d.etcd || cfg.store.etcd; d.etcdPrefix = d.etcdPrefix || (d.etcd.prefix || '/flowforge/').replace(/\/?$/, '/') + 'data/'; }
  if (cfg.cluster.enabled && t === 'file') {
    throw new Error(`invalid master config${cfg.file ? ' (' + cfg.file + ')' : ''}:\n  - cluster mode needs shared data storage: dataDb.type must be mongo or etcd (or auto with a mongo/etcd store)`);
  }
}

/** Synchronous load without a shared document (single node, tests, embedded worker). */
function loadConfig(role, source = argConfigPath()) {
  const { raw, file } = readLocal(source);
  if (raw.shared) throw new Error(`${file || 'config'} has a shared: block – use loadConfigAsync()`);
  return buildConfig(role, raw, file, null);
}

/**
 * Load a node config, pulling the cluster-shared document from etcd/MongoDB when the
 * local YAML has a `shared:` block. Returns { config, shared, sharedVersion, raw, file }.
 * `shared` is the open connection (used to watch for changes); null when not configured.
 */
async function loadConfigAsync(role, source = argConfigPath()) {
  const { raw, file } = readLocal(source);
  if (!raw.shared) return { config: buildConfig(role, raw, file, null), shared: null, sharedVersion: 0, raw, file };
  const { openShared } = require('./cluster/sharedConfig');
  const spec = substitute(raw.shared);
  let shared;
  try { shared = await openShared(spec); }
  catch (e) { throw new Error(`cannot reach shared config (${spec.source}): ${e.message}`); }
  const deadline = Date.now() + (Number(spec.waitMs) || 30000);
  let rec = null;
  for (;;) { // the shared document may not be pushed yet when a fresh cluster boots
    try { rec = await shared.get(); } catch (e) { if (Date.now() > deadline) throw new Error(`cannot read shared config: ${e.message}`); }
    if (rec || spec.required === false || Date.now() > deadline) break;
    console.warn(`[config] waiting for the shared cluster config in ${spec.source} (push it with: node bin/flowforge.js config push config/cluster.yaml)`);
    await new Promise((r) => setTimeout(r, 2000));
  }
  if (!rec && spec.required !== false) throw new Error(`no shared cluster config found in ${spec.source} – push one with: node bin/flowforge.js config push <cluster.yaml> --config ${file ? path.relative(process.cwd(), file) : '<node.yaml>'}`);
  const config = buildConfig(role, raw, file, rec?.doc || null);
  config.sharedVersion = rec?.version || 0;
  return { config, shared, sharedVersion: rec?.version || 0, sharedDoc: rec?.doc || null, raw, file };
}

function validate(role, cfg) {
  const errs = [];
  if (!/^[a-zA-Z0-9_.-]{1,64}$/.test(String(cfg.node.id))) errs.push('node.id must be 1-64 chars of letters, digits, _ . -');
  if (!cfg.security?.grpc?.username || !cfg.security?.grpc?.password) errs.push('security.grpc.username/password are required');
  if (role === 'master') {
    if (cfg.cluster.enabled) {
      if (!['etcd', 'mongo'].includes(cfg.cluster.coordinator)) errs.push('cluster.enabled requires cluster.coordinator: etcd | mongo');
      if (!['etcd', 'mongo'].includes(cfg.store.type)) errs.push('cluster.enabled requires a shared store (store.type: etcd | mongo)');
    }
    if (!['fail', 'restart'].includes(cfg.cluster.onWorkerLost)) errs.push('cluster.onWorkerLost must be fail | restart');
  } else {
    if (!['static', 'etcd', 'mongo'].includes(cfg.discovery.type)) errs.push('discovery.type must be static | etcd | mongo');
    if (cfg.discovery.type === 'static' && !(cfg.discovery.static?.masters || []).length) errs.push('discovery.static.masters needs at least one address');
    if (!(cfg.runtime.maxConcurrent > 0)) errs.push('runtime.maxConcurrent must be > 0');
  }
  if (errs.length) throw new Error(`invalid ${role} config${cfg.file ? ' (' + cfg.file + ')' : ''}:\n  - ${errs.join('\n  - ')}`);
  if (cfg.security.grpc.password === 'change-me' && !cfg._quiet) console.warn(`[config] ${cfg.node.id}: security.grpc.password is the default "change-me" – set it before production`);
}

module.exports = { loadConfig, loadConfigAsync, buildConfig, readLocal, argConfigPath, MASTER_DEFAULTS, WORKER_DEFAULTS, substitute, merge };
