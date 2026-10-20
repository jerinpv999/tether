#!/usr/bin/env node
'use strict';
/*
 * FlowForge admin CLI – manages the cluster-shared configuration in etcd / MongoDB.
 *
 *   node bin/flowforge.js config push <cluster.yaml>  (--config <node.yaml> | --etcd host:2379[,..] [--prefix /flowforge/] | --mongo <url> [--db flowforge])
 *   node bin/flowforge.js config pull                 (same connection flags) [--out file.yaml] [--reveal]
 *   node bin/flowforge.js config effective --config <node.yaml> [--role master|worker] [--reveal]
 *   node bin/flowforge.js config validate <cluster.yaml>
 *
 * `push` validates the document, writes it with optimistic concurrency (use --force to
 * overwrite a version changed since you pulled) and every running node picks it up.
 */
const fs = require('fs');
const path = require('path');
const YAML = require('yaml');
const { readLocal, substitute, loadConfigAsync } = require('../src/config');
const { openShared, redact } = require('../src/cluster/sharedConfig');
const { validateSharedDoc } = require('../src/cluster/sharedTools');

const argv = process.argv.slice(2);
const flag = (name) => { const i = argv.indexOf(`--${name}`); return i >= 0 ? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true) : undefined; };
const positional = argv.filter((a, i) => !a.startsWith('--') && !(i > 0 && argv[i - 1].startsWith('--') && !['--force', '--reveal'].includes(argv[i - 1])));
const [cmd, sub, file] = positional;

function connectionSpec() {
  if (flag('etcd')) return { source: 'etcd', etcd: { hosts: String(flag('etcd')).split(','), prefix: flag('prefix') || '/flowforge/' } };
  if (flag('mongo')) return { source: 'mongo', mongo: { url: flag('mongo'), db: flag('db') || 'flowforge' } };
  const nodeYaml = flag('config');
  if (nodeYaml) {
    const { raw } = readLocal(nodeYaml);
    if (!raw.shared) throw new Error(`${nodeYaml} has no shared: block`);
    return substitute(raw.shared);
  }
  throw new Error('say where the shared config lives: --config <node.yaml> | --etcd host:port | --mongo <url>');
}

function usage() {
  console.log(fs.readFileSync(__filename, 'utf8').split('\n').slice(3, 13).map((l) => l.replace(/^ \* ?/, '')).join('\n'));
  process.exit(1);
}

(async () => {
  if (cmd !== 'config' || !sub) usage();

  if (sub === 'validate') {
    if (!file) usage();
    const doc = YAML.parse(fs.readFileSync(file, 'utf8'));
    const errs = validateSharedDoc(doc);
    if (errs.length) { console.error(`✘ ${file}\n  - ${errs.join('\n  - ')}`); process.exit(2); }
    console.log(`✔ ${file} is valid`);
    return;
  }

  if (sub === 'effective') {
    const role = flag('role') || (/worker/i.test(path.basename(String(flag('config') || ''))) ? 'worker' : 'master');
    const { config, shared } = await loadConfigAsync(role, flag('config'));
    await shared?.close();
    const { root, ...rest } = config;
    console.log(YAML.stringify(flag('reveal') ? rest : redact(rest)));
    return;
  }

  const shared = await openShared(connectionSpec());
  try {
    if (sub === 'push') {
      if (!file) usage();
      const doc = YAML.parse(fs.readFileSync(file, 'utf8'));
      const errs = validateSharedDoc(doc);
      if (errs.length) { console.error(`✘ not pushed – ${file} is invalid:\n  - ${errs.join('\n  - ')}`); process.exit(2); }
      const cur = await shared.get();
      const expect = flag('force') ? undefined : (cur?.version ?? 0);
      if (cur && !flag('force') && flag('if-version') && Number(flag('if-version')) !== cur.version) {
        console.error(`✘ shared config is at v${cur.version}, expected v${flag('if-version')} – pull it first or use --force`); process.exit(3);
      }
      const v = await shared.put(doc, `cli:${process.env.USER || 'unknown'}@${require('os').hostname()}`, expect);
      console.log(`✔ pushed ${file} to ${shared.source} as v${v}${cur ? ` (was v${cur.version})` : ''} – running nodes apply it now`);
    } else if (sub === 'pull') {
      const cur = await shared.get();
      if (!cur) { console.error('no shared config stored yet'); process.exit(4); }
      const header = `# FlowForge shared cluster config v${cur.version} – updated ${new Date(cur.updatedAt).toISOString()} by ${cur.updatedBy}\n`;
      const text = header + YAML.stringify(flag('reveal') ? cur.doc : redact(cur.doc));
      if (flag('out')) { fs.writeFileSync(flag('out'), text); console.log(`✔ wrote v${cur.version} to ${flag('out')}`); } else process.stdout.write(text);
    } else usage();
  } finally { await shared.close(); }
})().catch((e) => { console.error(`✘ ${e.message}`); process.exit(1); });
