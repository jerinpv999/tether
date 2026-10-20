#!/usr/bin/env node
'use strict';
// FlowForge worker node – only hosts workflow child processes.
//   node bin/worker.js --config config/worker-1.yaml
const { loadConfigAsync } = require('../src/config');
const { Worker } = require('../src/worker/worker');

(async () => {
  const loaded = await loadConfigAsync('worker');
  const worker = new Worker(loaded.config, loaded);
  await worker.start();

  let stopping = false;
  // SIGTERM = graceful drain (rolling deploys); a second SIGINT exits immediately.
  process.on('SIGTERM', () => { if (stopping) return; stopping = true; worker.stop({ graceful: true }).then(() => process.exit(0)); });
  process.on('SIGINT', () => {
    if (stopping) process.exit(130);
    stopping = true;
    worker.stop({ graceful: true }).then(() => process.exit(0));
  });
})().catch((e) => { console.error(`fatal: ${e.message}`); process.exit(1); });
