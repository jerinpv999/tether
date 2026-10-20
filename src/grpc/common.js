'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO = path.join(__dirname, '..', '..', 'proto', 'orchestrator.proto');
const MSG_LIMIT = 32 * 1024 * 1024;
const CHANNEL_OPTS = {
  'grpc.max_receive_message_length': MSG_LIMIT,
  'grpc.max_send_message_length': MSG_LIMIT,
  'grpc.keepalive_time_ms': 10000,      // detect dead peers quickly (failover)
  'grpc.keepalive_timeout_ms': 5000,
  'grpc.keepalive_permit_without_calls': 1,
};

let pkg;
function loadProto() {
  if (!pkg) {
    const def = protoLoader.loadSync(PROTO, { keepCase: true, longs: Number, defaults: true, oneofs: true });
    pkg = grpc.loadPackageDefinition(def).orchestrator;
  }
  return pkg;
}

// ───────────── HTTP Basic authentication in gRPC metadata ─────────────
function basicHeader(username, password) {
  return 'Basic ' + Buffer.from(`${username}:${password}`, 'utf8').toString('base64');
}

function authMetadata(username, password, extra = {}) {
  const md = new grpc.Metadata();
  md.set('authorization', basicHeader(username, password));
  for (const [k, v] of Object.entries(extra)) if (v != null) md.set(k, String(v));
  return md;
}

function parseBasic(value) {
  const v = Array.isArray(value) ? value[0] : value;
  if (!v || typeof v !== 'string' || !v.startsWith('Basic ')) return null;
  const dec = Buffer.from(v.slice(6).trim(), 'base64').toString('utf8');
  const i = dec.indexOf(':');
  if (i < 0) return null;
  return { username: dec.slice(0, i), password: dec.slice(i + 1) };
}

/** Constant-time string comparison (hash first so lengths don't leak). */
function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a)).digest();
  const hb = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}

/** Returns the authenticated username, or null. `verify(user, pass)` → boolean. */
function authenticate(call, verify) {
  const creds = parseBasic(call.metadata.get('authorization'));
  if (!creds) return null;
  return verify(creds.username, creds.password) ? creds.username : null;
}

/** Verifier for the shared cluster credentials (security.grpc). */
function clusterVerifier(sec) {
  const users = [{ username: sec.username, password: sec.password }, ...(sec.additionalUsers || [])];
  return (u, p) => users.some((x) => safeEqual(x.username, u) & safeEqual(x.password, p));
}

const unauthenticated = (msg = 'invalid or missing Basic credentials') => ({ code: grpc.status.UNAUTHENTICATED, details: msg });

// ───────────── transport security ─────────────
const readOpt = (p) => (p ? fs.readFileSync(p) : null);

function serverCredentials(tls) {
  if (!tls?.enabled) return grpc.ServerCredentials.createInsecure();
  return grpc.ServerCredentials.createSsl(readOpt(tls.caFile), [{ cert_chain: readOpt(tls.certFile), private_key: readOpt(tls.keyFile) }], !!tls.requireClientCert);
}

function channelCredentials(tls) {
  if (!tls?.enabled) return grpc.credentials.createInsecure();
  return grpc.credentials.createSsl(readOpt(tls.caFile), readOpt(tls.keyFile), readOpt(tls.certFile));
}

function clientOptions(tls) {
  const o = { ...CHANNEL_OPTS };
  if (tls?.enabled && tls.serverNameOverride) o['grpc.ssl_target_name_override'] = tls.serverNameOverride;
  return o;
}

async function bindServer(server, host, port, tls) {
  return new Promise((res, rej) => server.bindAsync(`${host}:${port}`, serverCredentials(tls), (e, p) => (e ? rej(e) : res(p))));
}

module.exports = {
  grpc, loadProto, CHANNEL_OPTS, authMetadata, parseBasic, authenticate, clusterVerifier, safeEqual,
  unauthenticated, serverCredentials, channelCredentials, clientOptions, bindServer, basicHeader,
};
