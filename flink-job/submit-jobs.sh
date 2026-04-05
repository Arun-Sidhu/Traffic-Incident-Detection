#!/usr/bin/env bash
# submit-jobs.sh
set -euo pipefail

FLINK_MASTER="${FLINK_MASTER:-flink-jobmanager:8081}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:9092}"
IN_TOPIC="${IN_TOPIC:-gps_pings_raw}"
METRICS_TOPIC="${METRICS_TOPIC:-segment_metrics}"
OUT_TOPIC="${OUT_TOPIC:-traffic_incidents}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-file:///flink-checkpoints}"
SUBMIT_PINGCOUNT="${SUBMIT_PINGCOUNT:-false}"
SEGMENT_JOB_NAME="${SEGMENT_JOB_NAME:-Traffic Segment Metrics (10s, event-time)}"
DETECTOR_JOB_NAME="${DETECTOR_JOB_NAME:-Traffic Incident Detector (open/close)}"
SEGMENT_STARTUP_GRACE_SEC="${SEGMENT_STARTUP_GRACE_SEC:-10}"
JOB_START_TIMEOUT_SEC="${JOB_START_TIMEOUT_SEC:-120}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-2}"

OPEN_RATIO="${OPEN_RATIO:-0.80}"
CLOSE_RATIO="${CLOSE_RATIO:-0.90}"
MIN_COUNT="${MIN_COUNT:-3}"
CLOSE_HOLD_MS="${CLOSE_HOLD_MS:-10000}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

wait_for_http() {
  local url="$1"
  local label="$2"
  log "Waiting for ${label} at ${url} ..."
  until curl -fsS "${url}" >/dev/null; do
    sleep "${POLL_INTERVAL_SEC}"
  done
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  log "Waiting for ${label} on ${host}:${port} ..."
  until bash -lc "</dev/tcp/${host}/${port}" >/dev/null 2>&1; do
    sleep "${POLL_INTERVAL_SEC}"
  done
}

wait_for_taskmanager() {
  local deadline=$(( $(date +%s) + JOB_START_TIMEOUT_SEC ))
  log "Waiting for a Flink taskmanager to register ..."
  while true; do
    local payload
    payload="$(curl -fsS "http://${FLINK_MASTER}/taskmanagers")"
    if [[ "${payload}" != *'"taskmanagers":[]'* ]]; then
      log "Flink taskmanager is registered."
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      echo "Timed out waiting for a registered Flink taskmanager." >&2
      return 1
    fi
    sleep "${POLL_INTERVAL_SEC}"
  done
}

submit_job() {
  local class_name="$1"
  shift
  log "Submitting ${class_name}"

  local output
  output="$(/opt/flink/bin/flink run -d -m "${FLINK_MASTER}" -c "${class_name}" /job.jar "$@" 2>&1)"
  printf '%s\n' "${output}" >&2

  local job_id
  job_id="$(printf '%s\n' "${output}" | sed -n 's/.*JobID \([a-f0-9]\{32\}\).*/\1/p' | tail -n1)"
  if [[ -z "${job_id}" ]]; then
    echo "Failed to parse JobID from flink run output for ${class_name}." >&2
    return 1
  fi
  printf '%s\n' "${job_id}"
}

running_job_id_by_name() {
  local target_name="$1"
  local output
  output="$(/opt/flink/bin/flink list -r -m "${FLINK_MASTER}" 2>/dev/null || true)"
  printf '%s\n' "${output}" \
    | grep -F " : ${target_name}" \
    | sed 's/ :.*//' \
    | tail -n1
}

job_state_by_id() {
  local job_id="$1"
  curl -fsS "http://${FLINK_MASTER}/jobs/${job_id}" | sed -n 's/.*"state":"\([A-Z_]*\)".*/\1/p' | head -n1
}

wait_for_job_running() {
  local job_id="$1"
  local label="$2"
  local deadline=$(( $(date +%s) + JOB_START_TIMEOUT_SEC ))

  log "Waiting for ${label} (${job_id}) to reach RUNNING ..."
  while true; do
    local state=""
    state="$(job_state_by_id "${job_id}" || true)"
    case "${state}" in
      RUNNING)
        log "${label} is RUNNING."
        return 0
        ;;
      FAILED|FAILING|CANCELED|CANCELLING|SUSPENDED|RECONCILING)
        echo "${label} entered unexpected state: ${state}" >&2
        return 1
        ;;
      RESTARTING|CREATED|INITIALIZING|FINISHED|"")
        ;;
    esac

    if (( $(date +%s) >= deadline )); then
      echo "Timed out waiting for ${label} to reach RUNNING. Last state: ${state:-unknown}" >&2
      return 1
    fi
    sleep "${POLL_INTERVAL_SEC}"
  done
}

ensure_job_running() {
  local label="$1"
  local class_name="$2"
  shift 2

  local existing_job_id
  existing_job_id="$(running_job_id_by_name "${label}")"
  if [[ -n "${existing_job_id}" ]]; then
    log "${label} is already running as ${existing_job_id}; skipping duplicate submit."
    wait_for_job_running "${existing_job_id}" "${label}"
    printf '%s\n' "${existing_job_id}"
    return 0
  fi

  local new_job_id
  new_job_id="$(submit_job "${class_name}" "$@")"
  wait_for_job_running "${new_job_id}" "${label}"
  printf '%s\n' "${new_job_id}"
}

wait_for_flink() {
  wait_for_http "http://${FLINK_MASTER}/overview" "Flink REST API"
  wait_for_taskmanager
}

KAFKA_HOST="${BOOTSTRAP_SERVERS%%:*}"
KAFKA_PORT="${BOOTSTRAP_SERVERS##*:}"
wait_for_tcp "${KAFKA_HOST}" "${KAFKA_PORT}" "Kafka"
wait_for_flink

segment_job_id="$(ensure_job_running "${SEGMENT_JOB_NAME}" com.arun.traffic.SegmentMetricsJob \
  --bootstrapServers "${BOOTSTRAP_SERVERS}" \
  --inTopic "${IN_TOPIC}" \
  --outTopic "${METRICS_TOPIC}" \
  --checkpointDir "${CHECKPOINT_ROOT}/segment-metrics")"

log "Giving ${SEGMENT_JOB_NAME} ${SEGMENT_STARTUP_GRACE_SEC}s to stabilize before starting detector ..."
sleep "${SEGMENT_STARTUP_GRACE_SEC}"

detector_job_id="$(ensure_job_running "${DETECTOR_JOB_NAME}" com.arun.traffic.IncidentDetectionJob \
  --bootstrapServers "${BOOTSTRAP_SERVERS}" \
  --inTopic "${METRICS_TOPIC}" \
  --outTopic "${OUT_TOPIC}" \
  --checkpointDir "${CHECKPOINT_ROOT}/incident-detector" \
  --openRatio "${OPEN_RATIO}" \
  --closeRatio "${CLOSE_RATIO}" \
  --minCount "${MIN_COUNT}" \
  --closeHoldMs "${CLOSE_HOLD_MS}")"

if [[ "${SUBMIT_PINGCOUNT}" == "true" ]]; then
  pingcount_job_id="$(ensure_job_running "Ping Count Job" com.arun.traffic.PingCountJob \
    --bootstrapServers "${BOOTSTRAP_SERVERS}" \
    --inTopic "${IN_TOPIC}" \
    --outTopic gps_ping_counts)"
fi

log "All requested Flink jobs submitted and running."