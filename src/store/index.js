'use strict';
const { MemoryStore } = require('./memory');
const { FileStore } = require('./file');

async function createStore(config) {
  const t = config.store.type;
  let store;
  if (t === 'memory') store = new MemoryStore();
  else if (t === 'file' || t === 'json') store = new FileStore(config.store.file);
  else if (t === 'mongo' || t === 'mongodb') store = new (require('./mongo').MongoStore)(config.store.mongo, config.logs.retentionHours);
  else if (t === 'etcd') store = new (require('./etcd').EtcdStore)(config.store.etcd, config.logs.retentionHours);
  else throw new Error(`Unknown store type "${t}" (use memory | file | mongo | etcd)`);
  await store.init();
  return store;
}

module.exports = { createStore };
