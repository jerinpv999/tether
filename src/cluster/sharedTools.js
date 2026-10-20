'use strict';
const { buildConfig } = require('../config');

const ALLOWED_TOP = ['common', 'master', 'worker'];
// keys that identify a single node – they must stay in the node's own YAML
const NODE_ONLY = ['node', 'shared'];

/** Replace ${VAR} references with their default or a placeholder so structure can be validated anywhere. */
function placeholderEnv(v) {
  if (typeof v === 'string') return v.replace(/\$\{([A-Z0-9_]+)(?::-(.*?))?\}/gi, (_, n, d) => process.env[n] ?? (d !== undefined && d !== '' ? d : `placeholder-${n.toLowerCase()}`));
  if (Array.isArray(v)) return v.map(placeholderEnv);
  if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, placeholderEnv(x)]));
  return v;
}

/** Validates a shared document by building a master and a worker config from it. */
function validateSharedDoc(doc) {
  const errs = [];
  if (!doc || typeof doc !== 'object' || Array.isArray(doc)) return ['shared config must be a mapping with common / master / worker sections'];
  for (const k of Object.keys(doc)) if (!ALLOWED_TOP.includes(k)) errs.push(`unknown top-level section "${k}" (allowed: ${ALLOWED_TOP.join(', ')})`);
  for (const sec of ALLOWED_TOP) for (const k of NODE_ONLY) if (doc[sec]?.[k]) errs.push(`${sec}.${k} is node-specific – keep it in each node's own YAML`);
  const masked = [];
  (function walk(v, p) {
    if (v === '***') masked.push(p);
    else if (v && typeof v === 'object') for (const [k, x] of Object.entries(v)) walk(x, p ? `${p}.${k}` : k);
  })(doc, '');
  for (const p of masked) errs.push(`${p} is "***" (a redacted value) – pull with --reveal, or set the real value / an \${ENV} reference`);
  if (errs.length) return errs;
  const d = placeholderEnv(doc);
  for (const [role, local] of [['master', { node: { id: 'validate-master' }, _quiet: true }], ['worker', { node: { id: 'validate-worker' }, _quiet: true }]]) {
    try { buildConfig(role, local, null, d); } catch (e) { errs.push(...String(e.message).split('\n').slice(1).map((l) => `${role}: ${l.replace(/^\s*-\s*/, '')}`)); }
  }
  return errs;
}

module.exports = { validateSharedDoc, placeholderEnv };
