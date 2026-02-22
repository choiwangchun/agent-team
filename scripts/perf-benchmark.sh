#!/usr/bin/env bash

set -euo pipefail

PORT="${PORT:-3100}"
DURATION="${DURATION:-20}"
CONNECTIONS="${CONNECTIONS:-100}"
TARGET_URL="${TARGET_URL:-http://127.0.0.1:${PORT}/}"
BENCH_JSON="${BENCH_JSON:-/tmp/kairo-agent-autocannon.json}"
METRICS_JSON="${METRICS_JSON:-/tmp/kairo-agent-ops-metrics.json}"
SERVER_LOG="${SERVER_LOG:-/tmp/kairo-agent-server.log}"
SERVER_PID=""

cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

PORT="${PORT}" node server.js >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"
sleep 1

RSS_BEFORE_KB="$(ps -o rss= -p "${SERVER_PID}" | tr -d ' ')"
npx autocannon@7.14.0 -j -d "${DURATION}" -c "${CONNECTIONS}" "${TARGET_URL}" >"${BENCH_JSON}"
curl -sf "http://127.0.0.1:${PORT}/ops/metrics" >"${METRICS_JSON}"
RSS_AFTER_KB="$(ps -o rss= -p "${SERVER_PID}" | tr -d ' ')"

BENCH_JSON="${BENCH_JSON}" METRICS_JSON="${METRICS_JSON}" RSS_BEFORE_KB="${RSS_BEFORE_KB}" RSS_AFTER_KB="${RSS_AFTER_KB}" node <<'NODE'
const fs = require("fs");

const bench = JSON.parse(fs.readFileSync(process.env.BENCH_JSON, "utf8"));
const metrics = JSON.parse(fs.readFileSync(process.env.METRICS_JSON, "utf8"));

const summary = {
  target: bench.url,
  duration_s: bench.duration,
  connections: bench.connections,
  requests: {
    total: bench.requests.total,
    avg_per_sec: bench.requests.average,
  },
  latency_ms: {
    avg: bench.latency.average,
    p90: bench.latency.p90,
    p99: bench.latency.p99,
    max: bench.latency.max,
  },
  failures: {
    errors: bench.errors,
    timeouts: bench.timeouts,
    non2xx: bench.non2xx,
  },
  memory_kb: {
    rss_before: Number(process.env.RSS_BEFORE_KB),
    rss_after: Number(process.env.RSS_AFTER_KB),
    rss_delta: Number(process.env.RSS_AFTER_KB) - Number(process.env.RSS_BEFORE_KB),
  },
  event_loop: metrics.event_loop,
  app_latency_snapshot: metrics.requests.latency,
};

console.log(JSON.stringify(summary, null, 2));
NODE

echo "Raw benchmark JSON: ${BENCH_JSON}"
echo "Raw ops metrics JSON: ${METRICS_JSON}"
