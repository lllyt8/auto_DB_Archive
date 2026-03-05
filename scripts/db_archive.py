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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import fcntl

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
class AppConfig:
    database: str
    tables: List[TableConfig]
    max_generations: int
    schedule_timezone: str
    dry_run: bool
    log_path: str
    lock_path: str
    mysql: MySQLConfig
    email: EmailConfig
    timescale_check: TimescaleCheckConfig


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
        return {k: expand_env(v) for k, v in value.items()}
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

    database = cfg.get("database", "").strip()
    if not database:
        raise ValueError("config.database is required")

    max_generations = int(cfg.get("max_generations", 4))
    if max_generations < 1:
        raise ValueError("config.max_generations must be >= 1")

    table_entries = cfg.get("tables", [])
    if not table_entries:
        raise ValueError("config.tables must contain at least one table")

    tables = []
    for entry in table_entries:
        if isinstance(entry, str):
            table_name = entry.strip()
            table_max = max_generations
        else:
            table_name = str(entry.get("name", "")).strip()
            table_max = int(entry.get("max_generations", max_generations))
        if not table_name:
            raise ValueError("table entry missing name")
        if table_max < 1:
            raise ValueError(f"table {table_name} has invalid max_generations")
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
        recipients=[str(x).strip() for x in email_cfg.get("recipients", []) if str(x).strip()],
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

    return AppConfig(
        database=database,
        tables=tables,
        max_generations=max_generations,
        schedule_timezone=str(cfg.get("schedule_timezone", "EST")),
        dry_run=bool(cfg.get("dry_run", True)),
        log_path=str(cfg.get("log_path", "/home/epc_ai/aidetect/logs/db_archive.log")),
        lock_path=str(cfg.get("lock_path", "/tmp/db_archive.lock")),
        mysql=mysql,
        email=email,
        timescale_check=timescale_check,
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


class FileLock:
    def __init__(self, path: str):
        self.path = path
        self.handle = None

    def __enter__(self) -> "FileLock":
        lock_file = Path(self.path)
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.handle = lock_file.open("w", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(f"Another archive job is already running: {self.path}") from exc
        self.handle.write(f"{os.getpid()}\n")
        self.handle.flush()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.handle:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def resolve_tables(config_tables: Sequence[TableConfig], selected: Sequence[str]) -> List[TableConfig]:
    if not selected:
        return list(config_tables)
    lookup = {table.name: table for table in config_tables}
    missing = [name for name in selected if name not in lookup]
    if missing:
        raise ValueError(f"Unknown table(s) from --table: {', '.join(missing)}")
    return [lookup[name] for name in selected]


def list_archive_tables(
    conn: Any,
    database: str,
    base_table: str,
) -> List[str]:
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name LIKE %s
        ORDER BY table_name
    """
    like_pattern = f"{base_table}_a%"
    cursor = conn.cursor()
    cursor.execute(sql, (database, like_pattern))
    names = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return filter_archive_tables(base_table, names)


def filter_archive_tables(base_table: str, table_names: Iterable[str]) -> List[str]:
    prefix = f"{base_table}_a"
    result = []
    for name in table_names:
        if name.startswith(prefix) and ARCHIVE_SUFFIX_RE.search(name):
            result.append(name)
    return sorted(result)


def build_archive_name(
    base_table: str,
    existing_archives: Sequence[str],
    now: dt.datetime,
) -> str:
    base_suffix = now.strftime("%y%m%d%H%M")
    candidate = f"{base_table}_a{base_suffix}"
    if candidate not in existing_archives:
        return candidate
    return f"{base_table}_a{now.strftime('%y%m%d%H%M%S')}"


def compute_drop_candidates(archives: Sequence[str], max_generations: int) -> List[str]:
    if len(archives) <= max_generations:
        return []
    return list(sorted(archives)[: len(archives) - max_generations])


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
    stdout = proc.stdout.strip()
    stderr = proc.stderr.strip()
    detail = f"rc={proc.returncode}; stdout={stdout}; stderr={stderr}"
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
) -> TableResult:
    table_name = table_cfg.name
    archive_tables = list_archive_tables(conn, database, table_name)
    new_archive_name = build_archive_name(table_name, archive_tables, now)
    all_archives = sorted(archive_tables + [new_archive_name])
    to_drop = compute_drop_candidates(all_archives, table_cfg.max_generations)

    rename_sql = (
        f"RENAME TABLE {q(database)}.{q(table_name)} TO "
        f"{q(database)}.{q(new_archive_name)}"
    )
    create_sql = (
        f"CREATE TABLE {q(database)}.{q(table_name)} LIKE "
        f"{q(database)}.{q(new_archive_name)}"
    )

    if dry_run:
        detail = (
            f"DRY-RUN archive={new_archive_name}; "
            f"drop={to_drop if to_drop else '[]'}"
        )
        logger.info("[%s] %s", table_name, detail)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=to_drop,
            ok=True,
            detail=detail,
        )

    cursor = conn.cursor()
    try:
        cursor.execute(rename_sql)
        cursor.execute(create_sql)
        for drop_table in to_drop:
            drop_sql = f"DROP TABLE {q(database)}.{q(drop_table)}"
            cursor.execute(drop_sql)
        detail = f"archive={new_archive_name}; drop={to_drop if to_drop else '[]'}"
        logger.info("[%s] %s", table_name, detail)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=to_drop,
            ok=True,
            detail=detail,
        )
    except Exception as exc:
        detail = f"error={exc}"
        logger.exception("[%s] Archive failed", table_name)
        return TableResult(
            table=table_name,
            archive_table=new_archive_name,
            dropped_tables=[],
            ok=False,
            detail=detail,
        )
    finally:
        cursor.close()


def send_email_notice(
    email_cfg: EmailConfig,
    subject: str,
    body: str,
    logger: Logger,
) -> None:
    if not email_cfg.enabled:
        logger.info("Email disabled in config; skip sending.")
        return
    if not email_cfg.recipients:
        logger.warning("No recipients configured; skip sending email.")
        return
    if not email_cfg.sender:
        logger.warning("Email sender missing; skip sending email.")
        return
    sendmail_path = email_cfg.sendmail_path.strip()
    if not sendmail_path:
        logger.warning("sendmail_path is empty; skip sending email.")
        return
    if not Path(sendmail_path).exists():
        logger.warning("sendmail binary not found at %s; skip sending email.", sendmail_path)
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = email_cfg.sender
    msg["To"] = ", ".join(email_cfg.recipients)
    msg["Reply-To"] = email_cfg.sender

    proc = subprocess.run(
        [sendmail_path, "-t", "-i"],
        input=msg.as_string(),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"sendmail failed rc={proc.returncode}; stderr={proc.stderr.strip()}"
        )
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
    success = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]

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
        f"success_tables: {len(success)}",
        f"failed_tables: {len(failed)}",
        "",
        "Results:",
    ]
    for item in results:
        lines.append(f"- {item.table}: {'OK' if item.ok else 'FAIL'}; {item.detail}")
    return subject, "\n".join(lines)


def main() -> int:
    args = parse_args()
    run_id = f"{dt.datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    config = load_config(args.config)
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
        [t.name for t in selected_tables],
    )

    started_at = dt.datetime.now()
    results: List[TableResult] = []
    timescale_ok, timescale_detail = run_timescale_check(config.timescale_check, logger)
    if not timescale_ok:
        ended_at = dt.datetime.now()
        subject, body = build_report(
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            dry_run=dry_run,
            timescale_ok=timescale_ok,
            timescale_detail=timescale_detail,
            results=results,
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
        with FileLock(config.lock_path):
            conn = mysql_connector.connect(
                host=config.mysql.host,
                port=config.mysql.port,
                user=config.mysql.user,
                password=config.mysql.password,
                database=config.mysql.database,
                autocommit=True,
            )
            try:
                if config.mysql.database != config.database:
                    logger.warning(
                        "config.mysql.database (%s) != config.database (%s)",
                        config.mysql.database,
                        config.database,
                    )
                now = dt.datetime.now()
                for table_cfg in selected_tables:
                    result = rotate_single_table(
                        conn=conn,
                        database=config.database,
                        table_cfg=table_cfg,
                        now=now,
                        dry_run=dry_run,
                        logger=logger,
                    )
                    results.append(result)
            finally:
                conn.close()
    except Exception:
        logger.exception("Archive run failed before table operations completed.")
        ended_at = dt.datetime.now()
        subject, body = build_report(
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            dry_run=dry_run,
            timescale_ok=timescale_ok,
            timescale_detail=timescale_detail,
            results=results,
        )
        try:
            send_email_notice(config.email, subject, body, logger)
        except Exception:
            logger.exception("Email send failed after run exception.")
        return 1

    ended_at = dt.datetime.now()
    subject, body = build_report(
        run_id=run_id,
        started_at=started_at,
        ended_at=ended_at,
        dry_run=dry_run,
        timescale_ok=timescale_ok,
        timescale_detail=timescale_detail,
        results=results,
    )
    try:
        send_email_notice(config.email, subject, body, logger)
    except Exception:
        logger.exception("Email send failed.")

    failed_count = len([x for x in results if not x.ok])
    logger.info(
        "Archive completed run_id=%s success=%d failed=%d",
        run_id,
        len(results) - failed_count,
        failed_count,
    )
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
