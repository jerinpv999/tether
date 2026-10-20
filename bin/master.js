#!/usr/bin/env node
'use strict';
// FlowForge master node.   node bin/master.js --config config/master-1.yaml
const { startMaster } = require('../src/master');

startMaster().then((m) => {
  const shutdown = (sig) => { console.log(`${sig} received`); m.stop().then(() => process.exit(0), () => process.exit(1)); };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}).catch((e) => { console.error(`fatal: ${e.message}`); process.exit(1); });
