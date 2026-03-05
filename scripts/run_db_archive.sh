#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config/db_archive.yaml}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
RETENTION_DAYS="${RETENTION_DAYS:-90}"

mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/db_archive_cron_$(date '+%Y%m%d_%H%M%S').log"

{
  echo "[$(date '+%F %T')] start run_db_archive.sh"
  echo "ROOT_DIR=$ROOT_DIR"
  echo "CONFIG_PATH=$CONFIG_PATH"
  echo "PYTHON_BIN=$PYTHON_BIN"
  echo "RETENTION_DAYS=$RETENTION_DAYS"
} >>"$RUN_LOG"

set +e
"$PYTHON_BIN" "$ROOT_DIR/scripts/db_archive.py" --config "$CONFIG_PATH" --run >>"$RUN_LOG" 2>&1
run_rc=$?

{
  echo "[$(date '+%F %T')] archive_exit_code=$run_rc"
  echo "[$(date '+%F %T')] pruning logs older than $RETENTION_DAYS days"
} >>"$RUN_LOG"

find "$LOG_DIR" -type f -name "db_archive_cron_*.log" -mtime +"$RETENTION_DAYS" -print -delete >>"$RUN_LOG" 2>&1
prune_rc=$?

{
  echo "[$(date '+%F %T')] prune_exit_code=$prune_rc"
  echo "[$(date '+%F %T')] done"
} >>"$RUN_LOG"

exit "$run_rc"
