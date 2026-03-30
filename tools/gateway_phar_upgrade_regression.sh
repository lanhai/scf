#!/usr/bin/env bash
set -euo pipefail

# Gateway PHAR regression runner.
#
# Responsibility:
# - Start/stop master + slave gateway nodes in PHAR mode.
# - Trigger dashboard-equivalent appoint_update commands via internal gateway API.
# - Verify version convergence on both nodes for each requested iteration.
# - Produce machine-readable and human-readable reports for repeatable regression runs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCF_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd "${SCF_DIR}/.." && pwd)"
BIN_DIR="${SCF_DIR}/bin"

APP="mtvideo"
ENV_NAME="dev"
MASTER_HOST="127.0.0.1"
SLAVE_HOST="127.0.0.1"
MASTER_PORT=9580
SLAVE_PORT=9680
UPDATE_TYPE="app"
VERSIONS_CSV="1.8.14,1.8.15,1.8.14,1.8.15"
WAIT_TIMEOUT=300
BOOT_TIMEOUT=90
TEARDOWN=0
REPORT_DIR=""
REPORT_LABEL=""

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
STARTED_AT_HUMAN="$(date '+%F %T %z')"
START_EPOCH="$(date +%s)"
COMMAND_LINE="$0 $*"
STATUS="PASS"
FAIL_REASON=""

ROUNDS_FILE=""
REPORT_MD=""
REPORT_JSON=""
RUN_ID=""
MASTER_START_LOG=""
SLAVE_START_LOG=""

log() {
  printf '%s %s\n' "$(date '+%m-%d %H:%M:%S')" "$*"
}

trim() {
  local text="$1"
  # shellcheck disable=SC2001
  text="$(echo "$text" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  printf '%s' "$text"
}

sanitize_cell() {
  local text="$1"
  text="${text//$'\r'/ }"
  text="${text//$'\n'/ }"
  text="${text//$'\t'/ }"
  printf '%s' "$text"
}

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [options]

Options:
  --app <name>                 App directory name (default: ${APP})
  --env <env>                  Runtime env (default: ${ENV_NAME})
  --master-host <host>         Master gateway host (default: ${MASTER_HOST})
  --slave-host <host>          Slave gateway host (default: ${SLAVE_HOST})
  --master-port <port>         Master gateway port (default: ${MASTER_PORT})
  --slave-port <port>          Slave gateway port (default: ${SLAVE_PORT})
  --update-type <type>         appoint_update type: app/framework/public (default: ${UPDATE_TYPE})
  --versions <v1,v2,...>       Iteration sequence (default: ${VERSIONS_CSV})
  --wait-timeout <seconds>     Per-round converge timeout (default: ${WAIT_TIMEOUT})
  --boot-timeout <seconds>     Startup health timeout (default: ${BOOT_TIMEOUT})
  --report-dir <path>          Report output directory (default: apps/<app>/update/regression_reports)
  --label <name>               Optional label suffix in report filename
  --teardown                   Stop master/slave at end of run
  -h, --help                   Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --versions 1.8.14,1.8.15,1.8.14 --label nightly
  $(basename "$0") --update-type app --wait-timeout 420
EOF
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

json_accepted() {
  local json="$1"
  printf '%s' "$json" | php -r '
    $data = json_decode(stream_get_contents(STDIN), true);
    $v = null;
    if (is_array($data)) {
      if (array_key_exists("accepted", $data)) {
        $v = $data["accepted"];
      } elseif (isset($data["data"]) && is_array($data["data"]) && array_key_exists("accepted", $data["data"])) {
        $v = $data["data"]["accepted"];
      }
    }
    $ok = ($v === true || $v === 1 || $v === "1" || $v === "true");
    echo $ok ? "1" : "0";
  '
}

http_get() {
  local url="$1"
  local timeout="${2:-3}"
  local tmp
  tmp="$(mktemp)"
  local code
  code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' "$url" 2>/dev/null || true)"
  local body
  body="$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
  printf '%s\n%s' "$code" "$body"
}

http_post_json() {
  local url="$1"
  local payload="$2"
  local timeout="${3:-8}"
  local tmp
  tmp="$(mktemp)"
  local code
  code="$(curl -sS --max-time "$timeout" -o "$tmp" -w '%{http_code}' \
    -H 'Content-Type: application/json' \
    -X POST \
    "$url" \
    -d "$payload" 2>/dev/null || true)"
  local body
  body="$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
  printf '%s\n%s' "$code" "$body"
}

gateway_stop() {
  local role="$1"
  local port="$2"
  (cd "$BIN_DIR" && ./gateway stop -app="$APP" -env="$ENV_NAME" -role="$role" -port="$port" >/dev/null 2>&1 || true)
}

gateway_start_phar() {
  local role="$1"
  local port="$2"
  local logfile="$3"
  (
    cd "$BIN_DIR"
    nohup ./gateway start -app="$APP" -env="$ENV_NAME" -role="$role" -port="$port" -phar >"$logfile" 2>&1 &
    echo $!
  )
}

get_health_snapshot() {
  local host="$1"
  local port="$2"
  local response
  response="$(http_get "http://${host}:${port}/_gateway/healthz" 3)"
  local code body
  code="$(echo "$response" | sed -n '1p')"
  body="$(echo "$response" | sed -n '2,$p')"
  local message active
  message="$(json_read "$body" "message")"
  active="$(json_read "$body" "active_version")"
  printf '%s\t%s\t%s\t%s' "$code" "$message" "$active" "$body"
}

wait_gateway_ready() {
  local role="$1"
  local host="$2"
  local port="$3"
  local timeout="$4"
  local begin now elapsed
  begin="$(date +%s)"
  while true; do
    local snapshot code message active
    snapshot="$(get_health_snapshot "$host" "$port")"
    code="$(echo "$snapshot" | awk -F '\t' '{print $1}')"
    message="$(echo "$snapshot" | awk -F '\t' '{print $2}')"
    active="$(echo "$snapshot" | awk -F '\t' '{print $3}')"
    if [[ "$code" == "200" && "$message" == "ok" ]]; then
      log "[ready] role=${role} ${host}:${port} active_version=${active:-<empty>}"
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      log "[timeout] role=${role} healthz not ready within ${timeout}s"
      return 1
    fi
    sleep 1
  done
}

collect_versions() {
  local m s
  m="$(get_health_snapshot "$MASTER_HOST" "$MASTER_PORT")"
  s="$(get_health_snapshot "$SLAVE_HOST" "$SLAVE_PORT")"
  local m_code m_msg m_ver s_code s_msg s_ver
  m_code="$(echo "$m" | awk -F '\t' '{print $1}')"
  m_msg="$(echo "$m" | awk -F '\t' '{print $2}')"
  m_ver="$(echo "$m" | awk -F '\t' '{print $3}')"
  s_code="$(echo "$s" | awk -F '\t' '{print $1}')"
  s_msg="$(echo "$s" | awk -F '\t' '{print $2}')"
  s_ver="$(echo "$s" | awk -F '\t' '{print $3}')"
  printf '%s\t%s\t%s\t%s\t%s\t%s' "$m_code" "$m_msg" "$m_ver" "$s_code" "$s_msg" "$s_ver"
}

wait_converged_version() {
  local expected="$1"
  local timeout="$2"
  local begin now elapsed
  begin="$(date +%s)"
  while true; do
    local versions m_code m_msg m_ver s_code s_msg s_ver
    versions="$(collect_versions)"
    m_code="$(echo "$versions" | awk -F '\t' '{print $1}')"
    m_msg="$(echo "$versions" | awk -F '\t' '{print $2}')"
    m_ver="$(echo "$versions" | awk -F '\t' '{print $3}')"
    s_code="$(echo "$versions" | awk -F '\t' '{print $4}')"
    s_msg="$(echo "$versions" | awk -F '\t' '{print $5}')"
    s_ver="$(echo "$versions" | awk -F '\t' '{print $6}')"
    if [[ "$m_code" == "200" && "$s_code" == "200" && "$m_msg" == "ok" && "$s_msg" == "ok" && "$m_ver" == "$expected" && "$s_ver" == "$expected" ]]; then
      printf '%s\t%s' "$m_ver" "$s_ver"
      return 0
    fi
    now="$(date +%s)"
    elapsed=$((now - begin))
    if (( elapsed >= timeout )); then
      printf '%s\t%s' "$m_ver" "$s_ver"
      return 1
    fi
    sleep 1
  done
}

write_round() {
  local round="$1"
  local target="$2"
  local status="$3"
  local duration="$4"
  local http_code="$5"
  local master_ver="$6"
  local slave_ver="$7"
  local note="$8"
  note="$(sanitize_cell "$note")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" "$target" "$status" "$duration" "$http_code" "$master_ver" "$slave_ver" "$note" >>"$ROUNDS_FILE"
}

generate_json_report() {
  local finished_human="$1"
  local duration_sec="$2"
  php -r '
    $rows = [];
    $fp = fopen($argv[1], "r");
    if ($fp) {
      while (($line = fgets($fp)) !== false) {
        $line = rtrim($line, "\r\n");
        if ($line === "") {
          continue;
        }
        $parts = explode("\t", $line);
        $rows[] = [
          "round" => (int)($parts[0] ?? 0),
          "target_version" => (string)($parts[1] ?? ""),
          "status" => (string)($parts[2] ?? ""),
          "duration_seconds" => (int)($parts[3] ?? 0),
          "request_http_code" => (string)($parts[4] ?? ""),
          "master_active_version" => (string)($parts[5] ?? ""),
          "slave_active_version" => (string)($parts[6] ?? ""),
          "note" => (string)($parts[7] ?? ""),
        ];
      }
      fclose($fp);
    }
    $report = [
      "run_id" => $argv[2],
      "status" => $argv[3],
      "fail_reason" => $argv[4],
      "started_at" => $argv[5],
      "finished_at" => $argv[6],
      "duration_seconds" => (int)$argv[7],
      "app" => $argv[8],
      "env" => $argv[9],
      "master" => [
        "host" => $argv[10],
        "port" => (int)$argv[11],
      ],
      "slave" => [
        "host" => $argv[12],
        "port" => (int)$argv[13],
      ],
      "update_type" => $argv[14],
      "versions" => array_values(array_filter(array_map("trim", explode(",", $argv[15])), fn($v) => $v !== "")),
      "command" => $argv[16],
      "startup_logs" => [
        "master" => $argv[17],
        "slave" => $argv[18],
      ],
      "rounds" => $rows,
    ];
    echo json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  ' \
    "$ROUNDS_FILE" "$RUN_ID" "$STATUS" "$FAIL_REASON" "$STARTED_AT_HUMAN" "$finished_human" "$duration_sec" \
    "$APP" "$ENV_NAME" "$MASTER_HOST" "$MASTER_PORT" "$SLAVE_HOST" "$SLAVE_PORT" "$UPDATE_TYPE" "$VERSIONS_CSV" \
    "$COMMAND_LINE" "$MASTER_START_LOG" "$SLAVE_START_LOG" >"$REPORT_JSON"
}

generate_markdown_report() {
  local finished_human="$1"
  local duration_sec="$2"
  {
    echo "# Gateway PHAR Upgrade Regression Report"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- status: **${STATUS}**"
    echo "- fail_reason: \`${FAIL_REASON:-none}\`"
    echo "- started_at: \`${STARTED_AT_HUMAN}\`"
    echo "- finished_at: \`${finished_human}\`"
    echo "- duration_seconds: \`${duration_sec}\`"
    echo "- app/env: \`${APP}/${ENV_NAME}\`"
    echo "- master: \`${MASTER_HOST}:${MASTER_PORT}\`"
    echo "- slave: \`${SLAVE_HOST}:${SLAVE_PORT}\`"
    echo "- update_type: \`${UPDATE_TYPE}\`"
    echo "- versions: \`${VERSIONS_CSV}\`"
    echo "- command: \`${COMMAND_LINE}\`"
    echo "- startup_log_master: \`${MASTER_START_LOG}\`"
    echo "- startup_log_slave: \`${SLAVE_START_LOG}\`"
    echo
    echo "## Round Results"
    echo
    echo "| round | target | status | duration(s) | http | master | slave | note |"
    echo "| --- | --- | --- | ---: | --- | --- | --- | --- |"
    while IFS=$'\t' read -r round target round_status duration code master_v slave_v note; do
      [[ -n "${round}" ]] || continue
      printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
        "$round" "$target" "$round_status" "$duration" "$code" "$master_v" "$slave_v" "$note"
    done <"$ROUNDS_FILE"
    echo
  } >"$REPORT_MD"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --app)
        APP="$(trim "${2:-}")"
        shift 2
        ;;
      --env)
        ENV_NAME="$(trim "${2:-}")"
        shift 2
        ;;
      --master-host)
        MASTER_HOST="$(trim "${2:-}")"
        shift 2
        ;;
      --slave-host)
        SLAVE_HOST="$(trim "${2:-}")"
        shift 2
        ;;
      --master-port)
        MASTER_PORT="$(trim "${2:-}")"
        shift 2
        ;;
      --slave-port)
        SLAVE_PORT="$(trim "${2:-}")"
        shift 2
        ;;
      --update-type)
        UPDATE_TYPE="$(trim "${2:-}")"
        shift 2
        ;;
      --versions)
        VERSIONS_CSV="$(trim "${2:-}")"
        shift 2
        ;;
      --wait-timeout)
        WAIT_TIMEOUT="$(trim "${2:-}")"
        shift 2
        ;;
      --boot-timeout)
        BOOT_TIMEOUT="$(trim "${2:-}")"
        shift 2
        ;;
      --report-dir)
        REPORT_DIR="$(trim "${2:-}")"
        shift 2
        ;;
      --label)
        REPORT_LABEL="$(trim "${2:-}")"
        shift 2
        ;;
      --teardown)
        TEARDOWN=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

validate_inputs() {
  if [[ -z "$APP" || -z "$ENV_NAME" || -z "$UPDATE_TYPE" ]]; then
    echo "app/env/update_type must not be empty" >&2
    exit 2
  fi
  if [[ -z "$VERSIONS_CSV" ]]; then
    echo "versions must not be empty" >&2
    exit 2
  fi
  if ! [[ "$MASTER_PORT" =~ ^[0-9]+$ && "$SLAVE_PORT" =~ ^[0-9]+$ ]]; then
    echo "master_port/slave_port must be numbers" >&2
    exit 2
  fi
  if ! [[ "$WAIT_TIMEOUT" =~ ^[0-9]+$ && "$BOOT_TIMEOUT" =~ ^[0-9]+$ ]]; then
    echo "wait_timeout/boot_timeout must be numbers" >&2
    exit 2
  fi
  if [[ ! -x "${BIN_DIR}/gateway" ]]; then
    echo "gateway executable not found: ${BIN_DIR}/gateway" >&2
    exit 2
  fi
  command -v curl >/dev/null 2>&1 || {
    echo "curl not found" >&2
    exit 2
  }
  command -v php >/dev/null 2>&1 || {
    echo "php not found" >&2
    exit 2
  }
}

prepare_report_paths() {
  if [[ -z "$REPORT_DIR" ]]; then
    REPORT_DIR="${PROJECT_ROOT}/apps/${APP}/update/regression_reports"
  fi
  mkdir -p "$REPORT_DIR"
  RUN_ID="gateway_phar_regression_${APP}_${ENV_NAME}_${RUN_TS}"
  if [[ -n "$REPORT_LABEL" ]]; then
    RUN_ID="${RUN_ID}_$(sanitize_cell "$REPORT_LABEL" | tr ' ' '_')"
  fi
  mkdir -p "${REPORT_DIR}/runtime_logs"
  MASTER_START_LOG="${REPORT_DIR}/runtime_logs/${RUN_ID}_master_start.log"
  SLAVE_START_LOG="${REPORT_DIR}/runtime_logs/${RUN_ID}_slave_start.log"
  REPORT_MD="${REPORT_DIR}/${RUN_ID}.md"
  REPORT_JSON="${REPORT_DIR}/${RUN_ID}.json"
  ROUNDS_FILE="$(mktemp)"
}

teardown_if_needed() {
  if (( TEARDOWN == 1 )); then
    log "teardown enabled: stopping gateway master/slave"
    gateway_stop "slave" "$SLAVE_PORT"
    gateway_stop "master" "$MASTER_PORT"
  fi
}

fail_run() {
  local reason="$1"
  STATUS="FAIL"
  FAIL_REASON="$reason"
}

main() {
  parse_args "$@"
  validate_inputs
  prepare_report_paths
  trap 'rm -f "$ROUNDS_FILE"' EXIT

  log "run_id=${RUN_ID}"
  log "stopping stale gateways first"
  gateway_stop "slave" "$SLAVE_PORT"
  gateway_stop "master" "$MASTER_PORT"

  local master_start_pid slave_start_pid
  log "starting gateway master role=master port=${MASTER_PORT} phar=on"
  master_start_pid="$(gateway_start_phar "master" "$MASTER_PORT" "$MASTER_START_LOG")"
  log "master launcher pid=${master_start_pid}, log=${MASTER_START_LOG}"
  log "starting gateway slave role=slave port=${SLAVE_PORT} phar=on"
  slave_start_pid="$(gateway_start_phar "slave" "$SLAVE_PORT" "$SLAVE_START_LOG")"
  log "slave launcher pid=${slave_start_pid}, log=${SLAVE_START_LOG}"

  if ! wait_gateway_ready "master" "$MASTER_HOST" "$MASTER_PORT" "$BOOT_TIMEOUT"; then
    fail_run "master healthz timeout during startup"
  fi
  if [[ "$STATUS" == "PASS" ]] && ! wait_gateway_ready "slave" "$SLAVE_HOST" "$SLAVE_PORT" "$BOOT_TIMEOUT"; then
    fail_run "slave healthz timeout during startup"
  fi

  local rounds=0
  local ver
  IFS=',' read -r -a versions <<<"$VERSIONS_CSV"
  for ver in "${versions[@]}"; do
    ver="$(trim "$ver")"
    [[ -n "$ver" ]] || continue
    rounds=$((rounds + 1))
    if [[ "$STATUS" != "PASS" ]]; then
      write_round "$rounds" "$ver" "SKIPPED" "0" "-" "-" "-" "skipped due to previous failure"
      continue
    fi

    log "[round ${rounds}] dispatch appoint_update type=${UPDATE_TYPE} version=${ver}"
    local round_begin round_end round_cost
    round_begin="$(date +%s)"
    local payload response code body accepted message
    payload="$(printf '{"command":"appoint_update","params":{"type":"%s","version":"%s"}}' "$UPDATE_TYPE" "$ver")"
    response="$(http_post_json "http://${MASTER_HOST}:${MASTER_PORT}/_gateway/internal/command" "$payload" 8)"
    code="$(echo "$response" | sed -n '1p')"
    body="$(echo "$response" | sed -n '2,$p')"
    accepted="$(json_accepted "$body")"
    message="$(json_read "$body" "message")"
    if [[ -z "$message" ]]; then
      message="$(json_read "$body" "data.message")"
    fi

    if [[ "$code" != "200" || "$accepted" != "1" ]]; then
      round_end="$(date +%s)"
      round_cost=$((round_end - round_begin))
      write_round "$rounds" "$ver" "FAIL" "$round_cost" "$code" "-" "-" "dispatch failed: ${message:-$body}"
      fail_run "round ${rounds} dispatch failed (http=${code})"
      continue
    fi

    local converge_versions master_v slave_v
    if converge_versions="$(wait_converged_version "$ver" "$WAIT_TIMEOUT")"; then
      round_end="$(date +%s)"
      round_cost=$((round_end - round_begin))
      master_v="$(echo "$converge_versions" | awk -F '\t' '{print $1}')"
      slave_v="$(echo "$converge_versions" | awk -F '\t' '{print $2}')"
      write_round "$rounds" "$ver" "PASS" "$round_cost" "$code" "$master_v" "$slave_v" "${message:-accepted}"
      log "[round ${rounds}] converged: master=${master_v} slave=${slave_v} cost=${round_cost}s"
    else
      round_end="$(date +%s)"
      round_cost=$((round_end - round_begin))
      master_v="$(echo "$converge_versions" | awk -F '\t' '{print $1}')"
      slave_v="$(echo "$converge_versions" | awk -F '\t' '{print $2}')"
      write_round "$rounds" "$ver" "FAIL" "$round_cost" "$code" "$master_v" "$slave_v" "converge timeout ${WAIT_TIMEOUT}s"
      fail_run "round ${rounds} converge timeout (${WAIT_TIMEOUT}s)"
    fi
  done

  teardown_if_needed

  local end_epoch duration_sec finished_human
  end_epoch="$(date +%s)"
  duration_sec=$((end_epoch - START_EPOCH))
  finished_human="$(date '+%F %T %z')"
  generate_json_report "$finished_human" "$duration_sec"
  generate_markdown_report "$finished_human" "$duration_sec"

  log "report generated:"
  log "  markdown: ${REPORT_MD}"
  log "  json:     ${REPORT_JSON}"
  log "final status: ${STATUS}"

  if [[ "$STATUS" != "PASS" ]]; then
    log "fail reason: ${FAIL_REASON}"
    exit 1
  fi
}

main "$@"
