#!/usr/bin/env node
'use strict';
/*
 * Start the device-telemetry workflow from outside by publishing an MQTT message,
 * and wait for the workflow's acknowledgement on devices/<id>/ack.
 *
 *   MQTT_URL=mqtt://localhost:1883 node examples/clients/trigger-mqtt.js pump-7 82.5
 *
 * The broker must be the same one the masters use (master config: mqtt.url).
 * MQTT is fire-and-forget, so the result comes back as another MQTT message
 * (the workflow's "Ack to device" node); you can also poll the REST API with
 * the messageId, which is used as the instance id.
 */
const mqtt = require('mqtt');

const url = process.env.MQTT_URL || 'mqtt://localhost:1883';
const deviceId = process.argv[2] || 'pump-7';
const temperature = Number(process.argv[3] || 82.5);
const messageId = `msg-${Date.now()}`;

const client = mqtt.connect(url, { clientId: `device-${deviceId}-${process.pid}` });

client.on('connect', async () => {
  await client.subscribeAsync(`devices/${deviceId}/ack`, { qos: 1 });
  console.log(`→ devices/${deviceId}/telemetry`, { messageId, temperature });
  await client.publishAsync(`devices/${deviceId}/telemetry`, JSON.stringify({ messageId, temperature }), { qos: 1 });
});

client.on('message', (topic, buf) => {
  const msg = JSON.parse(buf.toString());
  if (msg.messageId !== messageId) return;
  console.log(`← ${topic}`, msg);
  console.log(`  details: GET /api/instances/${messageId}/activities`);
  client.end();
});

setTimeout(() => { console.error('no ack within 15 s – is FlowForge connected to this broker?'); process.exit(1); }, 15000).unref();
client.on('error', (e) => { console.error(e.message); process.exit(1); });
