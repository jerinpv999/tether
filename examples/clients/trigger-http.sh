#!/usr/bin/env bash
# Start the device-telemetry workflow from outside over HTTP.
# FLOWFORGE = any master (or the load balancer). Add -u user:pass if security.api is on.
FLOWFORGE=${FLOWFORGE:-http://localhost:3000}

# 1) Webhook trigger (defined in the workflow: POST /devices/:deviceId/telemetry, sync)
#    → waits for the Response node and returns its body. messageId becomes the instance id.
curl -s -X POST "$FLOWFORGE/hooks/devices/pump-7/telemetry" \
  -H 'content-type: application/json' \
  -d '{"messageId":"msg-1001","temperature":82.5}'
echo

# 2) Generic start API (works for any workflow, with or without triggers)
#    wait: "finish" → returns the final instance; omit wait → 202 immediately.
curl -s -X POST "$FLOWFORGE/api/workflows/device-telemetry/start" \
  -H 'content-type: application/json' \
  -d '{"instanceId":"msg-1002","input":{"deviceId":"pump-8","temperature":40},"wait":"finish"}'
echo

# 3) Check what happened
curl -s "$FLOWFORGE/api/instances/msg-1001/status"; echo
curl -s "$FLOWFORGE/api/instances/msg-1001/activities"; echo
