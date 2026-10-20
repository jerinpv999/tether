'use strict';
const { nodeTypes, triggerTypes, portsFor } = require('./nodeTypes');

const ID_RE = /^[a-zA-Z0-9_.-]{1,100}$/;

/** Returns a list of human-readable problems; empty list = valid. */
function validateWorkflow(wf) {
  const errs = [];
  if (!wf || typeof wf !== 'object') return ['workflow must be an object'];
  if (!wf.id || !ID_RE.test(wf.id)) errs.push('id is required (letters, digits, _ . -)');
  const nodes = Array.isArray(wf.nodes) ? wf.nodes : (errs.push('nodes must be an array'), []);
  const edges = Array.isArray(wf.edges) ? wf.edges : [];
  const byId = new Map();
  for (const n of nodes) {
    if (!n.id) { errs.push('every node needs an id'); continue; }
    if (byId.has(n.id)) errs.push(`duplicate node id ${n.id}`);
    byId.set(n.id, n);
    if (!nodeTypes[n.type]) errs.push(`node ${n.id}: unknown type "${n.type}"`);
  }
  const starts = nodes.filter((n) => n.type === 'start');
  if (starts.length !== 1) errs.push(`exactly one Start node required (found ${starts.length})`);
  for (const e of edges) {
    const from = byId.get(e.from), to = byId.get(e.to);
    if (!from) { errs.push(`edge from unknown node ${e.from}`); continue; }
    if (!to) { errs.push(`edge to unknown node ${e.to}`); continue; }
    const port = e.port || 'out';
    if (!portsFor(from).includes(port)) errs.push(`edge ${e.from}→${e.to}: node "${from.name || from.id}" has no port "${port}"`);
    if (to.type === 'start') errs.push(`edge ${e.from}→${e.to}: Start cannot have incoming edges`);
  }
  for (const n of nodes) {
    const c = n.config || {};
    if (n.type === 'subworkflow' && !c.workflowId) errs.push(`node ${n.name || n.id}: choose a workflow`);
    if (n.type === 'subworkflow' && c.workflowId === wf.id) errs.push(`node ${n.name || n.id}: a workflow cannot start itself`);
    if (n.type === 'switch' && (!Array.isArray(c.cases) || !c.cases.length)) errs.push(`node ${n.name || n.id}: at least one case`);
    if (n.type === 'signal' && !c.signalName) errs.push(`node ${n.name || n.id}: signal name required`);
  }
  for (const [i, t] of (wf.triggers || []).entries()) {
    if (!triggerTypes[t.type]) errs.push(`trigger #${i + 1}: unknown type "${t.type}"`);
    if (t.type === 'http' && !t.path) errs.push(`trigger #${i + 1}: path required`);
    if (t.type === 'mqtt' && !t.topic) errs.push(`trigger #${i + 1}: topic required`);
    if (t.type === 'rabbitmq' && !t.queue && !t.exchange) errs.push(`trigger #${i + 1}: queue or exchange required`);
    if (t.type === 'schedule' && !(Number(t.intervalMs) >= 1000)) errs.push(`trigger #${i + 1}: intervalMs >= 1000`);
  }
  return errs;
}

module.exports = { validateWorkflow, ID_RE };
