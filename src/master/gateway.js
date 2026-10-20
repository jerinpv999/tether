'use strict';
const crypto = require('crypto');
const { createDataStore } = require('./dataStore');

/** AMQP topic match: `*` = one word, `#` = zero or more words. */
function amqpMatch(pattern, key) {
  if (!pattern || pattern === '#') return true;
  const p = pattern.split('.'), k = String(key || '').split('.');
  const rec = (i, j) => {
    if (i === p.length) return j === k.length;
    if (p[i] === '#') { for (let x = j; x <= k.length; x++) if (rec(i + 1, x)) return true; return false; }
    if (j === k.length) return false;
    return (p[i] === '*' || p[i] === k[j]) && rec(i + 1, j + 1);
  };
  return rec(0, 0);
}

/** MQTT topic filter match: `+` = one level, `#` = remaining levels. */
function mqttMatch(filter, topic) {
  const f = String(filter).replace(/^\$share\/[^/]+\//, '').split('/'), t = String(topic).split('/');
  for (let i = 0; i < f.length; i++) {
    if (f[i] === '#') return true;
    if (i >= t.length) return false;
    if (f[i] !== '+' && f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}

const parse = (buf) => { const s = buf.toString(); try { return JSON.parse(s); } catch { return s; } };
const encode = (v) => Buffer.from(typeof v === 'string' ? v : JSON.stringify(v ?? null));

/**
 * Gateway owns every external connection the master shares with all workflow
 * children: RabbitMQ, MQTT and the data database. Children never connect to
 * brokers themselves – they ask the master over gRPC – so one connection pool
 * serves thousands of workflow processes.
 *
 * When RABBITMQ_URL / MQTT_URL are not configured an in-process emulated bus is
 * used, so workflows and triggers still work end-to-end in development.
 */
class Gateway {
  constructor(config, logger) {
    this.config = config;
    this.log = logger;
    this.subs = new Map(); // id -> { source, unsubscribe }
    this.localRabbit = new Set(); // { queue, exchange, pattern, handler }
    this.localMqtt = new Set();   // { filter, handler }
    this.mqttHandlers = new Map(); // filter -> Set(handler)
  }

  async init() {
    const { rabbitmq, mqtt, dataDb } = this.config;
    if (rabbitmq.enabled) {
      const amqp = require('amqplib');
      this.amqpConn = await amqp.connect(rabbitmq.url);
      this.amqpConn.on('error', (e) => this.log.error(`RabbitMQ connection error: ${e.message}`));
      this.amqpConn.on('close', () => this.log.warn('RabbitMQ connection closed'));
      this.pubCh = await this.amqpConn.createConfirmChannel();
      this.log.info(`RabbitMQ connected (${redact(rabbitmq.url)})`);
    } else this.log.warn('RABBITMQ_URL not set – RabbitMQ activities/triggers use the in-process emulated bus');

    if (mqtt.enabled) {
      const mqttLib = require('mqtt');
      this.mqtt = await mqttLib.connectAsync(mqtt.url, { clientId: `flowforge-${crypto.randomBytes(4).toString('hex')}`, ...mqtt.options });
      this.mqtt.on('message', (topic, buf) => {
        const payload = parse(buf);
        for (const [filter, set] of this.mqttHandlers) if (mqttMatch(filter, topic)) for (const h of set) h(payload, { topic });
      });
      this.mqtt.on('error', (e) => this.log.error(`MQTT error: ${e.message}`));
      this.log.info(`MQTT connected (${redact(mqtt.url)})`);
    } else this.log.warn('MQTT_URL not set – MQTT activities/triggers use the in-process emulated bus');

    this.data = await createDataStore(dataDb);
    this.log.info(`Data database (db.write): ${describeData(dataDb)}`);
  }

  async close() {
    for (const id of [...this.subs.keys()]) await this.unsubscribe(id);
    await this.pubCh?.close().catch(() => {});
    await this.amqpConn?.close().catch(() => {});
    await this.mqtt?.endAsync().catch(() => {});
    await this.data?.close().catch(() => {});
  }

  // ───────────── publish ─────────────
  async publishRabbit({ exchange = '', routingKey = '', queue = '', message, persistent = true, headers }) {
    if (!this.pubCh) {
      let delivered = 0;
      for (const s of this.localRabbit) {
        const hit = queue ? s.queue === queue : (s.exchange === exchange && amqpMatch(s.pattern, routingKey));
        if (hit) { delivered++; setImmediate(() => s.handler(structuredClone(message), { exchange, routingKey, queue })); }
      }
      return { emulated: true, delivered };
    }
    const opts = { persistent, contentType: 'application/json', headers };
    if (queue) { await this.pubCh.assertQueue(queue, { durable: true }); this.pubCh.sendToQueue(queue, encode(message), opts); }
    else this.pubCh.publish(exchange, routingKey, encode(message), opts);
    await this.pubCh.waitForConfirms();
    return { published: true };
  }

  async publishMqtt({ topic, message, qos = 1, retain = false }) {
    if (!topic) throw new Error('topic is required');
    if (!this.mqtt) {
      let delivered = 0;
      for (const s of this.localMqtt) if (mqttMatch(s.filter, topic)) { delivered++; setImmediate(() => s.handler(structuredClone(message), { topic })); }
      return { emulated: true, delivered };
    }
    await this.mqtt.publishAsync(topic, encode(message), { qos: Number(qos), retain: !!retain });
    return { published: true };
  }

  // ───────────── subscribe ─────────────
  /**
   * handler(payload, meta) may return a promise; for durable RabbitMQ queues the
   * message is acked after the promise resolves (nacked + requeued on throw).
   */
  async subscribe(spec, handler, presetId) {
    const id = presetId || crypto.randomUUID();
    if (this.subs.has(id)) await this.unsubscribe(id);
    if (spec.source === 'rabbitmq') {
      if (!this.amqpConn) {
        const s = { queue: spec.queue || '', exchange: spec.exchange || '', pattern: spec.routingKey || '#', handler };
        this.localRabbit.add(s);
        this.subs.set(id, { unsubscribe: async () => this.localRabbit.delete(s) });
      } else {
        const ch = await this.amqpConn.createChannel();
        await ch.prefetch(spec.prefetch || this.config.rabbitmq.prefetch || 10);
        let q = spec.queue;
        if (q) await ch.assertQueue(q, { durable: spec.durable !== false });
        else q = (await ch.assertQueue('', { exclusive: true, autoDelete: true })).queue;
        if (spec.exchange) {
          await ch.assertExchange(spec.exchange, spec.exchangeType || 'topic', { durable: true });
          await ch.bindQueue(q, spec.exchange, spec.routingKey || '#');
        }
        const { consumerTag } = await ch.consume(q, async (msg) => {
          if (!msg) return;
          try {
            await handler(parse(msg.content), { exchange: msg.fields.exchange, routingKey: msg.fields.routingKey, queue: q, headers: msg.properties.headers });
            ch.ack(msg);
          } catch (e) {
            this.log.error(`RabbitMQ handler failed on ${q}: ${e.message}`);
            ch.nack(msg, false, !msg.fields.redelivered); // retry once, then drop / DLX
          }
        });
        this.subs.set(id, { unsubscribe: async () => { await ch.cancel(consumerTag).catch(() => {}); await ch.close().catch(() => {}); } });
      }
    } else if (spec.source === 'mqtt') {
      if (!spec.topic) throw new Error('topic is required');
      if (!this.mqtt) {
        const s = { filter: spec.topic, handler };
        this.localMqtt.add(s);
        this.subs.set(id, { unsubscribe: async () => this.localMqtt.delete(s) });
      } else {
        const filter = spec.topic;
        if (!this.mqttHandlers.has(filter)) {
          this.mqttHandlers.set(filter, new Set());
          await this.mqtt.subscribeAsync(filter, { qos: Number(spec.qos ?? 1) });
        }
        const wrapped = (p, m) => Promise.resolve(handler(p, m)).catch((e) => this.log.error(`MQTT handler failed on ${m.topic}: ${e.message}`));
        this.mqttHandlers.get(filter).add(wrapped);
        this.subs.set(id, {
          unsubscribe: async () => {
            const set = this.mqttHandlers.get(filter);
            set?.delete(wrapped);
            if (set && !set.size) { this.mqttHandlers.delete(filter); await this.mqtt.unsubscribeAsync(filter).catch(() => {}); }
          },
        });
      }
    } else throw new Error(`Unknown event source "${spec.source}"`);
    return id;
  }

  async unsubscribe(id) {
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    await s.unsubscribe();
  }

  // ───────────── data database (shared in cluster mode) ─────────────
  dbWrite(op) { return this.data.write(op); }
  dbList(collection, opts) { return this.data.list(collection, opts); }
  dbCollections() { return this.data.collections(); }
}

function describeData(d) {
  if (d.type === 'mongo') return `MongoDB ${redact(d.mongoUrl)} / ${d.mongoDb}`;
  if (d.type === 'etcd') return `etcd ${[].concat(d.etcd.hosts).join(',')} ${d.etcdPrefix}<collection>/<id>`;
  return `JSON files in ${d.fileDir} (single node only)`;
}

const redact = (u) => String(u).replace(/\/\/([^:@/]+):([^@/]+)@/, '//$1:***@');

module.exports = { Gateway, amqpMatch, mqttMatch };
