#!/usr/bin/env bash
set -euo pipefail

# Dashboard full-chain regression runner.
#
# Scope:
# - exercise dashboard reload/restart/update for master + slave;
# - verify version convergence and no lingering draining generations;
# - verify master offline/restore then slave reconnect + heartbeat advance.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCF_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd "${SCF_DIR}/.." && pwd)"
BIN_DIR="${SCF_DIR}/bin"

APP="mtvideo"
ENV_NAME="dev"
MASTER_HOST="127.0.0.1"
MASTER_PORT=9580
SLAVE_HOST="127.0.0.1"
SLAVE_PORT=9680
DASHBOARD_PASSWORD="lanhai1987"
UPGRADE_TO="1.8.15"
WAIT_TIMEOUT=600
REPORT_DIR="${PROJECT_ROOT}/apps/${APP}/update/regression_reports"

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
REPORT_FILE="${REPORT_DIR}/dashboard_full_chain_regression_${RUN_TS}.log"
RUNTIME_LOG_DIR="${REPORT_DIR}/runtime_logs"
MASTER_RESTORE_LOG="${RUNTIME_LOG_DIR}/dashboard_full_chain_master_restore_${RUN_TS}.log"

STATUS="PASS"
FAIL_REASON=""

log() {
  local line
  line="$(date '+%m-%d %H:%M:%S') $*"
  printf '%s\n' "$line" | tee -a "$REPORT_FILE"
}

fail() {
  STATUS="FAIL"
  FAIL_REASON="$1"
  log "FAIL: $FAIL_REASON"
  exit 1
}

http_post_json() {
  local url="$1"
  local payload="$2"
  local auth_token="${3:-}"
  local timeout="${4:-12}"
  local tmp code body
  tmp="$(mktemp)"
  if [[ -n "$auth_token" ]]; then
    code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' \
      -H 'Content-Type: application/json' \
      -H "Authorization: Bearer ${auth_token}" \
      -X POST "$url" -d "$payload" 2>/dev/null || true)"
  else
    code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' \
      -H 'Content-Type: application/json' \
      -X POST "$url" -d "$payload" 2>/dev/null || true)"
  fi
  body="$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
  printf '%s\n%s' "$code" "$body"
}

http_get_json() {
  local url="$1"
  local auth_token="${2:-}"
  local timeout="${3:-8}"
  local tmp code body
  tmp="$(mktemp)"
  if [[ -n "$auth_token" ]]; then
    code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' \
      -H "Authorization: Bearer ${auth_token}" \
      "$url" 2>/dev/null || true)"
  else
    code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' \
      "$url" 2>/dev/null || true)"
  fi
  body="$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
  printf '%s\n%s' "$code" "$body"
}

json_read() {
  local json="$1"
  local path="$2"
  printf '%s' "$json" | php -r '
    $data = json_decode(stream_get_contents(STDIN), true);
    $path = explode(".", $argv[1]);
    $cur = $data;
    foreach ($path as $part) {
      if (!is_array($cur) || !array_key_exists($part, $cur)) {
        $cur = null;
        break;
      }
      $cur = $cur[$part];
    }
    if (is_bool($cur)) {
      echo $cur ? "true" : "false";
      exit(0);
    }
    if (is_scalar($cur)) {
      echo (string)$cur;
      exit(0);
    }
    echo "";
  ' "$path"
}

dashboard_login() {
  local response code body token err_code
  response="$(http_post_json "http://${MASTER_HOST}:${MASTER_PORT}/~/login" "{\"password\":\"${DASHBOARD_PASSWORD}\"}" "" 8)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  err_code="$(json_read "$body" "errCode")"
  token="$(json_read "$body" "data.token")"
  if [[ "$code" != "200" || "$err_code" != "0" || -z "$token" ]]; then
    fail "dashboard login failed: http=${code}, errCode=${err_code}, body=${body}"
  fi
  printf '%s' "$token"
}

health_snapshot() {
  local host="$1"
  local port="$2"
  local response code body message version
  response="$(http_get_json "http://${host}:${port}/_gateway/healthz" "" 3)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  message="$(json_read "$body" "message")"
  version="$(json_read "$body" "active_version")"
  printf '%s\t%s\t%s\t%s' "$code" "$message" "$version" "$body"
}

upstream_draining_count() {
  local host="$1"
  local port="$2"
  local response code body
  response="$(http_get_json "http://${host}:${port}/_gateway/upstreams" "" 4)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  if [[ "$code" != "200" ]]; then
    echo "9999"
    return
  fi
  printf '%s' "$body" | php -r '
    $data = json_decode(stream_get_contents(STDIN), true);
    $count = 0;
    $generations = (array)($data["generations"] ?? []);
    foreach ($generations as $generation) {
      $status = (string)($generation["status"] ?? "");
      if (in_array($status, ["draining", "prepared"], true)) {
        $count++;
      }
      foreach ((array)($generation["instances"] ?? []) as $instance) {
        $instanceStatus = (string)($instance["status"] ?? "");
        if (in_array($instanceStatus, ["draining", "prepared"], true)) {
          $count++;
        }
      }
    }
    echo (string)$count;
  '
}

wait_cluster_converged() {
  local expected_version="$1"
  local timeout="$2"
  local begin now elapsed
  begin="$(date +%s)"
  while true; do
    local m s m_code m_msg m_ver s_code s_msg s_ver m_drain s_drain
    m="$(health_snapshot "$MASTER_HOST" "$MASTER_PORT")"
    s="$(health_snapshot "$SLAVE_HOST" "$SLAVE_PORT")"
    m_code="$(echo "$m" | awk -F '\t' '{print $1}')"
    m_msg="$(echo "$m" | awk -F '\t' '{print $2}')"
    m_ver="$(echo "$m" | awk -F '\t' '{print $3}')"
    s_code="$(echo "$s" | awk -F '\t' '{print $1}')"
    s_msg="$(echo "$s" | awk -F '\t' '{print $2}')"
    s_ver="$(echo "$s" | awk -F '\t' '{print $3}')"
    m_drain="$(upstream_draining_count "$MASTER_HOST" "$MASTER_PORT")"
    s_drain="$(upstream_draining_count "$SLAVE_HOST" "$SLAVE_PORT")"
    if [[ "$m_code" == "200" && "$m_msg" == "ok" \
       && "$s_code" == "200" && "$s_msg" == "ok" \
       && "$(version_matches_expected "$m_ver" "$expected_version")" == "1" \
       && "$(version_matches_expected "$s_ver" "$expected_version")" == "1" \
       && "$m_drain" == "0" && "$s_drain" == "0" ]]; then
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      log "converge timeout snapshot: master=${m_code}/${m_msg}/${m_ver}/drain=${m_drain}, slave=${s_code}/${s_msg}/${s_ver}/drain=${s_drain}"
      return 1
    fi
    sleep 1
  done
}

version_matches_expected() {
  local actual="$1"
  local expected="$2"
  if [[ "$actual" == "$expected" \
     || "$actual" == "${expected}-reload-"* \
     || "$actual" == "${expected}-rolling-"* \
     || "$actual" == "${expected}-update-"* ]]; then
    echo "1"
    return
  fi
  echo "0"
}

normalize_release_version() {
  local version="$1"
  # Reload/rolling/update generations append a runtime suffix to display version.
  # Upgrade round-trip should always target the canonical release version.
  version="$(echo "$version" | sed -E 's/-(reload|rolling|update)-[0-9]{14}$//')"
  printf '%s' "$version"
}

nodes_snapshot() {
  local token="$1"
  local response code body
  response="$(http_get_json "http://${MASTER_HOST}:${MASTER_PORT}/~/nodes" "$token" 8)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  if [[ "$code" != "200" ]]; then
    echo ""
    return
  fi
  echo "$body"
}

node_value_by_role() {
  local nodes_json="$1"
  local role="$2"
  local field="$3"
  printf '%s' "$nodes_json" | php -r '
    $data = json_decode(stream_get_contents(STDIN), true);
    $role = $argv[1];
    $field = $argv[2];
    foreach ((array)($data["data"] ?? []) as $row) {
      if ((string)($row["role"] ?? "") !== $role) {
        continue;
      }
      $v = $row[$field] ?? null;
      if (is_bool($v)) {
        echo $v ? "true" : "false";
      } elseif (is_scalar($v)) {
        echo (string)$v;
      }
      exit(0);
    }
    echo "";
  ' "$role" "$field"
}

resolve_slave_node_host() {
  local token="$1"
  local timeout="${2:-120}"
  local begin now elapsed nodes host
  begin="$(date +%s)"
  while true; do
    nodes="$(nodes_snapshot "$token")"
    host="$(node_value_by_role "$nodes" "slave" "host")"
    if [[ -n "$host" ]]; then
      printf '%s' "$host"
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      fail "unable to resolve slave host from /~/nodes"
    fi
    sleep 1
  done
}

dispatch_dashboard_command() {
  local token="$1"
  local command="$2"
  local host="$3"
  local payload response code body err_code message
  payload="$(printf '{"command":"%s","host":"%s","params":{}}' "$command" "$host")"
  response="$(http_post_json "http://${MASTER_HOST}:${MASTER_PORT}/~/command" "$payload" "$token" 12)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  err_code="$(json_read "$body" "errCode")"
  message="$(json_read "$body" "message")"
  log "${command} ${host} => ${body}"
  if [[ "$code" != "200" || "$err_code" != "0" ]]; then
    fail "dashboard command failed: command=${command}, host=${host}, http=${code}, errCode=${err_code}, message=${message}"
  fi
}

dispatch_dashboard_update() {
  local token="$1"
  local type="$2"
  local version="$3"
  local payload response code body err_code accepted
  payload="$(printf '{"type":"%s","version":"%s"}' "$type" "$version")"
  response="$(http_post_json "http://${MASTER_HOST}:${MASTER_PORT}/~/update" "$payload" "$token" 20)"
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  err_code="$(json_read "$body" "errCode")"
  accepted="$(json_read "$body" "data.accepted")"
  log "update ${type}->${version} => ${body}"
  if [[ "$code" != "200" || "$err_code" != "0" || "$accepted" != "true" ]]; then
    fail "dashboard update failed: type=${type}, version=${version}, http=${code}, errCode=${err_code}, accepted=${accepted}"
  fi
}

wait_master_down() {
  local timeout="$1"
  local begin now elapsed code message
  begin="$(date +%s)"
  while true; do
    local snapshot
    snapshot="$(health_snapshot "$MASTER_HOST" "$MASTER_PORT")"
    code="$(echo "$snapshot" | awk -F '\t' '{print $1}')"
    message="$(echo "$snapshot" | awk -F '\t' '{print $2}')"
    if [[ "$code" != "200" || "$message" != "ok" ]]; then
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      return 1
    fi
    sleep 1
  done
}

wait_slave_reconnected_and_heartbeat_advanced() {
  local token="$1"
  local baseline_master_hb="$2"
  local baseline_slave_hb="$3"
  local timeout="$4"
  local begin now elapsed
  begin="$(date +%s)"
  while true; do
    local nodes online master_hb slave_hb
    nodes="$(nodes_snapshot "$token")"
    online="$(node_value_by_role "$nodes" "slave" "online")"
    master_hb="$(node_value_by_role "$nodes" "master" "heart_beat")"
    slave_hb="$(node_value_by_role "$nodes" "slave" "heart_beat")"
    if [[ "$online" == "true" \
       && "$master_hb" =~ ^[0-9]+$ && "$slave_hb" =~ ^[0-9]+$ \
       && "$master_hb" -gt "$baseline_master_hb" \
       && "$slave_hb" -gt "$baseline_slave_hb" ]]; then
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      log "heartbeat wait snapshot: online=${online}, master_hb=${master_hb}, slave_hb=${slave_hb}, baseline_m=${baseline_master_hb}, baseline_s=${baseline_slave_hb}"
      return 1
    fi
    sleep 1
  done
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]
  --password <dashboard-password>   Dashboard login password (default: ${DASHBOARD_PASSWORD})
  --upgrade-to <version>            App version to upgrade then rollback (default: ${UPGRADE_TO})
  --wait-timeout <seconds>          Converge timeout (default: ${WAIT_TIMEOUT})
  --master-port <port>              Master gateway port (default: ${MASTER_PORT})
  --slave-port <port>               Slave gateway port (default: ${SLAVE_PORT})
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --password)
        DASHBOARD_PASSWORD="$2"
        shift 2
        ;;
      --upgrade-to)
        UPGRADE_TO="$2"
        shift 2
        ;;
      --wait-timeout)
        WAIT_TIMEOUT="$2"
        shift 2
        ;;
      --master-port)
        MASTER_PORT="$2"
        shift 2
        ;;
      --slave-port)
        SLAVE_PORT="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

main() {
  parse_args "$@"
  mkdir -p "$REPORT_DIR" "$RUNTIME_LOG_DIR"
  : > "$REPORT_FILE"

  log "report=${REPORT_FILE}"
  log "step=precheck"
  local master_snapshot slave_snapshot baseline_version
  master_snapshot="$(health_snapshot "$MASTER_HOST" "$MASTER_PORT")"
  slave_snapshot="$(health_snapshot "$SLAVE_HOST" "$SLAVE_PORT")"
  baseline_version="$(normalize_release_version "$(echo "$master_snapshot" | awk -F '\t' '{print $3}')")"
  if [[ "$(echo "$master_snapshot" | awk -F '\t' '{print $1}')" != "200" || "$(echo "$master_snapshot" | awk -F '\t' '{print $2}')" != "ok" ]]; then
    fail "master health precheck failed: $(echo "$master_snapshot" | awk -F '\t' '{print $4}')"
  fi
  if [[ "$(echo "$slave_snapshot" | awk -F '\t' '{print $1}')" != "200" || "$(echo "$slave_snapshot" | awk -F '\t' '{print $2}')" != "ok" ]]; then
    fail "slave health precheck failed: $(echo "$slave_snapshot" | awk -F '\t' '{print $4}')"
  fi
  if [[ -z "$baseline_version" ]]; then
    fail "unable to determine baseline active version"
  fi
  if [[ "$baseline_version" == "$UPGRADE_TO" ]]; then
    # Avoid no-op upgrade round and keep rollback deterministic.
    UPGRADE_TO="1.8.14"
    if [[ "$baseline_version" == "$UPGRADE_TO" ]]; then
      UPGRADE_TO="1.8.15"
    fi
  fi
  log "baseline_version=${baseline_version}, upgrade_to=${UPGRADE_TO}"

  local token slave_node_host
  token="$(dashboard_login)"
  slave_node_host="$(resolve_slave_node_host "$token" "$WAIT_TIMEOUT")"
  log "slave_host=${slave_node_host}"

  local round_begin round_cost

  round_begin="$(date +%s)"
  dispatch_dashboard_command "$token" "reload" "localhost"
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "reload localhost did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "reload localhost cost=${round_cost}s"

  round_begin="$(date +%s)"
  dispatch_dashboard_command "$token" "reload" "$slave_node_host"
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "reload slave did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "reload ${slave_node_host} cost=${round_cost}s"

  round_begin="$(date +%s)"
  dispatch_dashboard_command "$token" "restart" "$slave_node_host"
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "restart slave did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "restart ${slave_node_host} cost=${round_cost}s"

  round_begin="$(date +%s)"
  dispatch_dashboard_command "$token" "restart" "localhost"
  sleep 2
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "restart localhost did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "restart localhost cost=${round_cost}s"

  token="$(dashboard_login)"
  slave_node_host="$(resolve_slave_node_host "$token" "$WAIT_TIMEOUT")"

  round_begin="$(date +%s)"
  dispatch_dashboard_update "$token" "app" "$UPGRADE_TO"
  if ! wait_cluster_converged "$UPGRADE_TO" "$WAIT_TIMEOUT"; then
    fail "update to ${UPGRADE_TO} did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "update app->${UPGRADE_TO} cost=${round_cost}s"

  token="$(dashboard_login)"
  slave_node_host="$(resolve_slave_node_host "$token" "$WAIT_TIMEOUT")"

  round_begin="$(date +%s)"
  dispatch_dashboard_update "$token" "app" "$baseline_version"
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "rollback to ${baseline_version} did not converge"
  fi
  round_cost=$(( $(date +%s) - round_begin ))
  log "update app->${baseline_version} cost=${round_cost}s"

  token="$(dashboard_login)"
  local nodes_before master_hb_before slave_hb_before
  nodes_before="$(nodes_snapshot "$token")"
  master_hb_before="$(node_value_by_role "$nodes_before" "master" "heart_beat")"
  slave_hb_before="$(node_value_by_role "$nodes_before" "slave" "heart_beat")"
  if [[ ! "$master_hb_before" =~ ^[0-9]+$ || ! "$slave_hb_before" =~ ^[0-9]+$ ]]; then
    fail "invalid heartbeat snapshot before offline/restore"
  fi

  log "master offline/restore begin"
  (cd "$BIN_DIR" && ./gateway stop -app="$APP" -env="$ENV_NAME" -role=master -port="$MASTER_PORT" >/dev/null 2>&1 || true)
  if ! wait_master_down 40; then
    fail "master did not go offline in expected time"
  fi
  local slave_snapshot_offline
  slave_snapshot_offline="$(health_snapshot "$SLAVE_HOST" "$SLAVE_PORT")"
  log "master offline confirmed, slave health=$(echo "$slave_snapshot_offline" | awk -F '\t' '{print $1"/"$2"/"$3}')"

  (cd "$BIN_DIR" && nohup ./gateway start -app="$APP" -env="$ENV_NAME" -role=master -port="$MASTER_PORT" -phar >"$MASTER_RESTORE_LOG" 2>&1 &)
  if ! wait_cluster_converged "$baseline_version" "$WAIT_TIMEOUT"; then
    fail "master restore did not converge, log=${MASTER_RESTORE_LOG}"
  fi
  token="$(dashboard_login)"
  if ! wait_slave_reconnected_and_heartbeat_advanced "$token" "$master_hb_before" "$slave_hb_before" 120; then
    fail "slave reconnect/heartbeat did not recover after master restore"
  fi
  log "master restore done"

  # Heartbeat liveness check after all operations.
  local nodes_mid hb_master_mid hb_slave_mid
  nodes_mid="$(nodes_snapshot "$token")"
  hb_master_mid="$(node_value_by_role "$nodes_mid" "master" "heart_beat")"
  hb_slave_mid="$(node_value_by_role "$nodes_mid" "slave" "heart_beat")"
  if [[ ! "$hb_master_mid" =~ ^[0-9]+$ || ! "$hb_slave_mid" =~ ^[0-9]+$ ]]; then
    fail "heartbeat liveness snapshot parse failed"
  fi
  if ! wait_slave_reconnected_and_heartbeat_advanced "$token" "$hb_master_mid" "$hb_slave_mid" 45; then
    fail "heartbeat not advancing after steady-state probe"
  fi
  token="$(dashboard_login)"
  local nodes_end hb_master_end hb_slave_end
  nodes_end="$(nodes_snapshot "$token")"
  hb_master_end="$(node_value_by_role "$nodes_end" "master" "heart_beat")"
  hb_slave_end="$(node_value_by_role "$nodes_end" "slave" "heart_beat")"
  log "heartbeat advancing: master ${hb_master_mid}->${hb_master_end}, slave ${hb_slave_mid}->${hb_slave_end}"

  local master_drain slave_drain
  master_drain="$(upstream_draining_count "$MASTER_HOST" "$MASTER_PORT")"
  slave_drain="$(upstream_draining_count "$SLAVE_HOST" "$SLAVE_PORT")"
  if [[ "$master_drain" != "0" || "$slave_drain" != "0" ]]; then
    fail "draining residue detected: master=${master_drain}, slave=${slave_drain}"
  fi
  log "draining residue: master=${master_drain}, slave=${slave_drain}"

  log "PASS"
}

main "$@"
