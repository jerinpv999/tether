'use strict';
const vm = require('vm');

/*
 * Expression & template evaluation.
 * NOTE: node:vm is NOT a security sandbox. It isolates globals and enforces a CPU
 * timeout, and each workflow already runs in its own OS process, but workflow
 * authors must be trusted. Swap in isolated-vm if untrusted users author code.
 */

const scriptCache = new Map();
function compile(src) {
  let s = scriptCache.get(src);
  if (!s) {
    s = new vm.Script(src, { filename: 'expression.js' });
    if (scriptCache.size > 2000) scriptCache.clear();
    scriptCache.set(src, s);
  }
  return s;
}

function evalExpr(expr, scope, timeout = 1000) {
  const ctx = vm.createContext({ ...scope });
  return compile(`(${expr}\n)`).runInContext(ctx, { timeout });
}

/** Runs a Function node body. `return` works; async/await is allowed. */
async function runCode(code, scope, timeout = 1000) {
  const ctx = vm.createContext({ ...scope });
  const fn = compile(`(async () => {\n${code}\n})()`).runInContext(ctx, { timeout });
  return await fn;
}

const FULL = /^\s*\{\{([\s\S]+?)\}\}\s*$/;
const PART = /\{\{([\s\S]+?)\}\}/g;

/**
 * Resolves {{ expr }} placeholders recursively in strings, arrays and objects.
 *  "{{ input.items }}"          → raw value (array, object, number …)
 *  "Order {{ input.id }} paid"  → interpolated string
 */
function template(value, scope) {
  if (typeof value === 'string') {
    if (!value.includes('{{')) return value;
    const full = FULL.exec(value);
    // exactly one placeholder covering the whole string → keep the value's type
    if (full && value.match(/\{\{/g).length === 1) return evalExpr(full[1], scope);
    return value.replace(PART, (_, e) => {
      const v = evalExpr(e, scope);
      return v === undefined || v === null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
    });
  }
  if (Array.isArray(value)) return value.map((v) => template(v, scope));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = template(v, scope);
    return out;
  }
  return value;
}

/** "500" | 500 → ms, "30s", "5m", "2h", "1d", "1h30m" */
function parseDuration(v) {
  if (typeof v === 'number') return v;
  const s = String(v ?? '').trim();
  if (/^\d+(\.\d+)?$/.test(s)) return Number(s);
  const units = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 };
  let total = 0, matched = false;
  for (const m of s.matchAll(/(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)/g)) { total += Number(m[1]) * units[m[2]]; matched = true; }
  if (!matched) throw new Error(`invalid duration "${v}"`);
  return total;
}

/** Plain JSON copy (also strips vm-realm prototypes). */
const plain = (v) => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));

module.exports = { evalExpr, runCode, template, parseDuration, plain };
