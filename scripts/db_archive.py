#!/usr/bin/env python3
"""Weekly rotating archive for selected MySQL tables."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass
from email.mime.text import MIMEText
from logging import Formatter, INFO, Logger, StreamHandler, getLogger
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - import path differs between script execution and tests
    from .coordination import CoordinationStore, FileLock
except ImportError:  # pragma: no cover
    from coordination import CoordinationStore, FileLock

try:
    import mysql.connector as mysql_connector
except ImportError:  # pragma: no cover
    mysql_connector = None

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


ARCHIVE_SUFFIX_RE = re.compile(r"_a\d{8,12}$")
IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
UNRESOLVED_ENV_TOKEN_RE = re.compile(r"^\$(\{[A-Za-z_][A-Za-z0-9_]*\}|[A-Za-z_][A-Za-z0-9_]*)$")


@dataclass(frozen=True)
class TableConfig:
    name: str
    max_generations: int


@dataclass(frozen=True)
class EmailConfig:
    enabled: bool
    sender: str
    recipients: List[str]
    sendmail_path: str


@dataclass(frozen=True)
class TimescaleCheckConfig:
    enabled: bool
    mode: str
    command: str


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class CoordinationConfig:
    lock_path: str
    state_dir: str
    drop_requires_replication_complete: bool
    wait_timeout_seconds: int
    wait_poll_seconds: int


@dataclass(frozen=True)
class AppConfig:
    database: str
    tables: List[TableConfig]
    max_generations: int
    schedule_timezone: str
    dry_run: bool
    log_path: str
    mysql: MySQLConfig
    email: EmailConfig
    timescale_check: TimescaleCheckConfig
    coordination: CoordinationConfig


@dataclass(frozen=True)
class TableResult:
    table: str
    archive_table: str
    dropped_tables: List[str]
    ok: bool
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rotate and prune MySQL archive tables.")
    parser.add_argument(
        "--config",
        default="config/db_archive.yaml",
        help="Path to YAML config file.",
    )
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        help="Limit execution to one table. Repeatable.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Plan only, no DDL.")
    mode_group.add_argument("--run", action="store_true", help="Execute DDL.")
    return parser.parse_args()


def ensure_ident(identifier: str) -> str:
    if not IDENT_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")
    return identifier


def q(identifier: str) -> str:
    return f"`{ensure_ident(identifier)}`"


def expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env(item) for item in value]
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def validate_not_unresolved_env(field_name: str, value: str) -> None:
    if UNRESOLVED_ENV_TOKEN_RE.fullmatch(value.strip()):
        raise ValueError(
            f"{field_name} still looks like an unresolved env token: {value}. "
            "Set the environment variable or replace with a real value in config."
        )


def load_config(path: str) -> AppConfig:
    if yaml is None:
        raise RuntimeError("PyYAML is required. Install with: pip install PyYAML")
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    cfg = expand_env(raw)

    database = str(cfg.get("database", "")).strip()
    if not database:
        raise ValueError("config.database is required")

    max_generations = int(cfg.get("max_generations", 4))
    if max_generations < 1:
        raise ValueError("config.max_generations must be >= 1")

    table_entries = cfg.get("tables", [])
    if not table_entries:
        raise ValueError("config.tables must contain at least one table")

    tables: List[TableConfig] = []
    for entry in table_entries:
        if isinstance(entry, str):
            table_name = entry.strip()
            table_max = max_generations
        else:
            table_name = str(entry.get("name", "")).strip()
            table_max = int(entry.get("max_generations", max_generations))
        if not table_name:
            raise ValueError("table entry missing name")
        tables.append(TableConfig(name=table_name, max_generations=table_max))

    mysql_cfg = cfg.get("mysql", {})
    mysql = MySQLConfig(
        host=str(mysql_cfg.get("host", "localhost")),
        port=int(mysql_cfg.get("port", 3306)),
        user=str(mysql_cfg.get("user", "")),
        password=str(mysql_cfg.get("password", "")),
        database=str(mysql_cfg.get("database", database)),
    )
    if not mysql.user or not mysql.password:
        raise ValueError("config.mysql.user and config.mysql.password are required")
    validate_not_unresolved_env("config.mysql.user", mysql.user)
    validate_not_unresolved_env("config.mysql.password", mysql.password)

    email_cfg = cfg.get("email", {})
    email = EmailConfig(
        enabled=bool(email_cfg.get("enabled", True)),
        sender=str(email_cfg.get("sender", "")),
        recipients=[str(item).strip() for item in email_cfg.get("recipients", []) if str(item).strip()],
        sendmail_path=str(email_cfg.get("sendmail_path", "/usr/sbin/sendmail")),
    )

    timescale_cfg = cfg.get("timescale_check", {})
    timescale_check = TimescaleCheckConfig(
        enabled=bool(timescale_cfg.get("enabled", False)),
        mode=str(timescale_cfg.get("mode", "warn")).lower(),
        command=str(timescale_cfg.get("command", "")).strip(),
    )
    if timescale_check.mode not in {"warn", "block"}:
        raise ValueError("timescale_check.mode must be 'warn' or 'block'")

    coordination = CoordinationConfig(
        lock_path=str(
            cfg.get(
                "coordination_lock_path",
                cfg.get("lock_path", "/home/epcenergy/coordination/db_repl_archive.lock"),
            )
        ),
        state_dir=str(cfg.get("shared_state_dir", "/home/epcenergy/coordination/streams")),
        drop_requires_replication_complete=bool(cfg.get("drop_requires_replication_complete", True)),
        wait_timeout_seconds=int(cfg.get("coordination_wait_timeout_seconds", 900)),
        wait_poll_seconds=int(cfg.get("coordination_wait_poll_seconds", 30)),
    )

    return AppConfig(
        database=database,
        tables=tables,
        max_generations=max_generations,
        schedule_timezone=str(cfg.get("schedule_timezone", "America/New_York")),
        dry_run=bool(cfg.get("dry_run", True)),
        log_path=str(cfg.get("log_path", "/home/epc_ai/auto_DB_Archive/logs/db_archive.log")),
        mysql=mysql,
        email=email,
        timescale_check=timescale_check,
        coordination=coordination,
    )


def setup_logger(log_path: str, run_id: str) -> Logger:
    logger = getLogger(f"db_archive_{run_id}")
    logger.setLevel(INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    stream = StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        backupCount=90,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def resolve_tables(config_tables: Sequence[TableConfig], selected: Sequence[str]) -> List[TableConfig]:
    if not selected:
        return list(config_tables)
    lookup = {table.name: table for table in config_tables}
    missing = [name for name in selected if name not in lookup]
    if missing:
        raise ValueError(f"Unknown table(s) from --table: {', '.join(missing)}")
    return [lookup[name] for name in selected]


def list_archive_tables(conn: Any, database: str, base_table: str) -> List[str]:
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name LIKE %s
        ORDER BY table_name
    """
    cursor = conn.cursor()
    cursor.execute(sql, (database, f"{base_table}_a%"))
    rows = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return filter_archive_tables(base_table, rows)


def filter_archive_tables(base_table: str, table_names: Iterable[str]) -> List[str]:
    prefix = f"{base_table}_a"
    return sorted(
        name
        for name in table_names
        if name.startswith(prefix) and ARCHIVE_SUFFIX_RE.search(name)
    )


def build_archive_name(base_table: str, existing_archives: Sequence[str], now: dt.datetime) -> str:
    base_suffix = now.strftime("%y%m%d%H%M")
    candidate = f"{base_table}_a{base_suffix}"
    if candidate not in existing_archives:
        return candidate
    return f"{base_table}_a{now.strftime('%y%m%d%H%M%S')}"


def compute_drop_candidates(archives: Sequence[str], max_generations: int) -> List[str]:
    if len(archives) <= max_generations:
        return []
    return list(sorted(archives)[: len(archives) - max_generations])


def query_max_id(conn: Any, database: str, table_name: str) -> int:
    sql = f"SELECT COALESCE(MAX(id), 0) FROM {q(database)}.{q(table_name)}"
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()
    return int(row[0] or 0)


def run_timescale_check(cfg: TimescaleCheckConfig, logger: Logger) -> Tuple[bool, str]:
    if not cfg.enabled:
        return True, "Timescale check disabled"
    if not cfg.command:
        msg = "Timescale check enabled but command is empty"
        if cfg.mode == "block":
            return False, msg
        logger.warning(msg)
        return True, msg

    proc = subprocess.run(
        cfg.command,
        shell=True,
        capture_output=True,
        text=True,
        check=False,
    )
    detail = f"rc={proc.returncode}; stdout={proc.stdout.strip()}; stderr={proc.stderr.strip()}"
    if proc.returncode == 0:
        return True, detail
    if cfg.mode == "block":
        return False, detail
    logger.warning("Timescale check warning: %s", detail)
    return True, detail


def rotate_single_table(
    conn: Any,
    database: str,
    table_cfg: TableConfig,
    now: dt.datetime,
    dry_run: bool,
    logger: Logger,
    store: CoordinationStore,
    coordination_cfg: CoordinationConfig,
) -> TableResult:
    table_name = table_cfg.name
    stream_state = store.load(table_name, auto_create=False)
    if stream_state is None:
        raise RuntimeError(f"Missing stream state for {table_name}. Run repl/init_replication_state.php first.")
    open_segment = stream_state.open_segment()
    if open_segment.physical_table != table_name:
        raise RuntimeError(
            f"Open segment for {table_name} points to {open_segment.physical_table}, expected {table_name}"
        )

    archive_tables = list_archive_tables(conn, database, table_name)
    new_archive_name = build_archive_name(table_name, archive_tables, now)
    planned_archive_max_id = query_max_id(conn, database, table_name)
    planned_state = stream_state.close_open_segment(new_archive_name, planned_archive_max_id, now.isoformat())
    all_archives = sorted(archive_tables + [new_archive_name])
    drop_candidates = compute_drop_candidates(all_archives, table_cfg.max_generations)
    drop_allowed, drop_skipped = store.drop_decision(
        planned_state,
        drop_candidates,
        coordination_cfg.drop_requires_replication_complete,
    )

    if dry_run:
        detail = (
            f"DRY-RUN archive={new_archive_name}; max_id={planned_archive_max_id}; "
            f"drop={drop_allowed if drop_allowed else []}; "
            f"skipped={drop_skipped if drop_skipped else []}"
        )
        logger.info("[%s] %s", table_name, detail)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=drop_allowed,
            ok=True,
            detail=detail,
        )

    rename_sql = (
        f"RENAME TABLE {q(database)}.{q(table_name)} TO "
        f"{q(database)}.{q(new_archive_name)}"
    )
    create_sql = (
        f"CREATE TABLE {q(database)}.{q(table_name)} LIKE "
        f"{q(database)}.{q(new_archive_name)}"
    )
    cursor = conn.cursor()
    dropped_tables: List[str] = []
    try:
        cursor.execute(rename_sql)
        cursor.execute(create_sql)

        archive_max_id = query_max_id(conn, database, new_archive_name)
        rotated_state = stream_state.close_open_segment(new_archive_name, archive_max_id, now.isoformat())
        store.save(rotated_state)

        drop_allowed, drop_skipped = store.drop_decision(
            rotated_state,
            drop_candidates,
            coordination_cfg.drop_requires_replication_complete,
        )
        for drop_table in drop_allowed:
            cursor.execute(f"DROP TABLE {q(database)}.{q(drop_table)}")
            dropped_tables.append(drop_table)

        if dropped_tables:
            store.save(rotated_state.prune_dropped(dropped_tables))

        detail = (
            f"archive={new_archive_name}; max_id={archive_max_id}; "
            f"drop={dropped_tables if dropped_tables else []}; "
            f"skipped={drop_skipped if drop_skipped else []}"
        )
        logger.info("[%s] %s", table_name, detail)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=dropped_tables,
            ok=True,
            detail=detail,
        )
    except Exception as exc:
        detail = f"error={exc}"
        logger.exception("[%s] Archive failed", table_name)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=dropped_tables,
            ok=False,
            detail=detail,
        )
    finally:
        cursor.close()


def send_email_notice(email_cfg: EmailConfig, subject: str, body: str, logger: Logger) -> None:
    if not email_cfg.enabled or not email_cfg.recipients or not email_cfg.sender:
        logger.info("Email disabled or missing recipients/sender; skip sending.")
        return
    if not email_cfg.sendmail_path or not Path(email_cfg.sendmail_path).exists():
        logger.warning("sendmail binary not found at %s; skip sending email.", email_cfg.sendmail_path)
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_cfg.sender
    msg["To"] = ", ".join(email_cfg.recipients)
    msg["Reply-To"] = email_cfg.sender

    proc = subprocess.run(
        [email_cfg.sendmail_path, "-t", "-i"],
        input=msg.as_string(),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sendmail failed rc={proc.returncode}; stderr={proc.stderr.strip()}")
    logger.info("Email sent to %s", ", ".join(email_cfg.recipients))


def build_report(
    run_id: str,
    started_at: dt.datetime,
    ended_at: dt.datetime,
    dry_run: bool,
    timescale_ok: bool,
    timescale_detail: str,
    results: Sequence[TableResult],
) -> Tuple[str, str]:
    failed = [item for item in results if not item.ok]
    status = "Success" if not failed else "Failed"
    subject = f"DB Archive {status} - {ended_at.strftime('%Y-%m-%d %H:%M:%S')}"
    lines = [
        f"run_id: {run_id}",
        f"started_at: {started_at.isoformat()}",
        f"ended_at: {ended_at.isoformat()}",
        f"duration_seconds: {(ended_at - started_at).total_seconds():.2f}",
        f"dry_run: {dry_run}",
        f"timescale_check_ok: {timescale_ok}",
        f"timescale_check_detail: {timescale_detail}",
        f"success_tables: {len(results) - len(failed)}",
        f"failed_tables: {len(failed)}",
        "",
        "Results:",
    ]
    for item in results:
        lines.append(f"- {item.table}: {'OK' if item.ok else 'FAIL'}; {item.detail}")
    return subject, "\n".join(lines)


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    run_id = f"{dt.datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    dry_run = config.dry_run
    if args.dry_run:
        dry_run = True
    elif args.run:
        dry_run = False

    logger = setup_logger(config.log_path, run_id)
    logger.info("Start DB archive run_id=%s", run_id)
    selected_tables = resolve_tables(config.tables, args.table)
    logger.info(
        "Mode=%s database=%s tables=%s",
        "DRY-RUN" if dry_run else "RUN",
        config.database,
        [item.name for item in selected_tables],
    )

    started_at = dt.datetime.now()
    results: List[TableResult] = []
    timescale_ok, timescale_detail = run_timescale_check(config.timescale_check, logger)
    if not timescale_ok:
        ended_at = dt.datetime.now()
        subject, body = build_report(
            run_id,
            started_at,
            ended_at,
            dry_run,
            timescale_ok,
            timescale_detail,
            results,
        )
        try:
            send_email_notice(config.email, subject, body, logger)
        except Exception:
            logger.exception("Email send failed after blocked timescale check.")
        logger.error("Timescale check blocked archive run: %s", timescale_detail)
        return 2

    try:
        if mysql_connector is None:
            raise RuntimeError(
                "mysql-connector-python is required. Install with: pip install mysql-connector-python"
            )
        store = CoordinationStore(config.coordination.state_dir)
        with FileLock(
            config.coordination.lock_path,
            timeout_seconds=config.coordination.wait_timeout_seconds,
            poll_seconds=config.coordination.wait_poll_seconds,
        ):
            conn = mysql_connector.connect(
                host=config.mysql.host,
                port=config.mysql.port,
                user=config.mysql.user,
                password=config.mysql.password,
                database=config.mysql.database,
                autocommit=True,
            )
            try:
                now = dt.datetime.now()
                for table_cfg in selected_tables:
                    results.append(
                        rotate_single_table(
                            conn=conn,
                            database=config.database,
                            table_cfg=table_cfg,
                            now=now,
                            dry_run=dry_run,
                            logger=logger,
                            store=store,
                            coordination_cfg=config.coordination,
                        )
                    )
            finally:
                conn.close()
    except Exception:
        logger.exception("Archive run failed before table operations completed.")
        ended_at = dt.datetime.now()
        subject, body = build_report(
            run_id,
            started_at,
            ended_at,
            dry_run,
            timescale_ok,
            timescale_detail,
            results,
        )
        try:
            send_email_notice(config.email, subject, body, logger)
        except Exception:
            logger.exception("Email send failed after run exception.")
        return 1

    ended_at = dt.datetime.now()
    subject, body = build_report(
        run_id,
        started_at,
        ended_at,
        dry_run,
        timescale_ok,
        timescale_detail,
        results,
    )
    try:
        send_email_notice(config.email, subject, body, logger)
    except Exception:
        logger.exception("Email send failed.")

    failed_count = len([item for item in results if not item.ok])
    logger.info(
        "Archive completed run_id=%s success=%d failed=%d",
        run_id,
        len(results) - failed_count,
        failed_count,
    )
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
