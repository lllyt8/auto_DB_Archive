# Coordinated Archive + Replication Runbook

## 1) Deploy files
Copy the updated files to the server:

1. `scripts/db_archive.py`
2. `scripts/coordination.py`
3. `scripts/run_db_archive.sh`
4. `config/db_archive.yaml`
5. `repl/config.php`
6. `repl/coordination.php`
7. `repl/copy_method.php`
8. `repl/data_replication.php`
9. `repl/init_replication_state.php`
10. `repl/recover_missing_rows.php`
11. `repl/crontab.sh`

## 2) Shared coordination directory
Create the shared coordination directory used by both PHP replication and Python archive:

```bash
mkdir -p /home/epc_ai/coordination/streams
chown -R epc_ai:epc_ai /home/epc_ai/coordination
chmod -R 775 /home/epc_ai/coordination
```

Expected files at runtime:

- `/home/epc_ai/coordination/db_repl_archive.lock`
- `/home/epc_ai/coordination/streams/*.json`

## 3) Install Python requirements
```bash
python3 -m pip install --user mysql-connector-python pyyaml
```

## 4) Stop old schedulers
Disable the old long-running replication loop and the old archive cron before bootstrapping state.

## 5) Bootstrap stream state
Run once after deployment:

```bash
cd /home/epc_ai/auto_DB_Archive
php repl/init_replication_state.php
```

Use `--force` only when intentionally rebuilding state from live tables.

## 6) Dry-run the recovery pass
This reports recoverable gaps without writing to Timescale:

```bash
cd /home/epc_ai/auto_DB_Archive
php repl/recover_missing_rows.php --dry-run
```

Expected output per table:

```text
dry-run table=ess_string_HON source_rows=... target_matches=... missing_rows=... inserted_rows=0
```

## 7) Apply the recovery pass
After reviewing the dry-run output:

```bash
cd /home/epc_ai/auto_DB_Archive
php repl/recover_missing_rows.php --apply
```

This fills recoverable gaps and advances the segment state files.

## 8) Validate replication
Run a one-shot replication pass:

```bash
cd /home/epc_ai/auto_DB_Archive
php repl/data_replication.php
```

Expected behavior:

1. The script acquires `/home/epc_ai/coordination/db_repl_archive.lock`.
2. It processes closed archive segments before the open current segment.
3. It logs a per-table summary without mentioning `last_ts`.

## 9) Validate archive in dry-run
```bash
cd /home/epc_ai/auto_DB_Archive
python3 scripts/db_archive.py --config config/db_archive.yaml --dry-run
```

Expected behavior:

1. It acquires the same shared lock as the replication job.
2. It verifies the stream state for each archived table.
3. It shows `drop=[]` or only drops archive tables whose segment state is already `replication_complete=true`.

## 10) Production schedules
Use one-shot schedulers only.

Replication cron:

```cron
*/10 * * * * cd /home/epc_ai/auto_DB_Archive && /bin/bash repl/crontab.sh
```

Archive cron:

```cron
15 2 * * 0 cd /home/epc_ai/auto_DB_Archive && /bin/bash scripts/run_db_archive.sh
```

The safety guarantee comes from the shared lock and segment state, not from clock skew between the two jobs.

## 11) Operational checks
After the first coordinated archive cycle:

1. Confirm each archived table has one closed segment for the new archive and one open segment for the new current table.
2. Confirm closed segments move to `replication_complete=true` after replication catches up.
3. Confirm archive logs show `skipped=[...]` rather than dropping any archive segment that is not fully replicated.
4. Confirm replication logs show `segments_completed=[...]` when it finishes backfilling archived segments.

## 12) Rollback
If the new flow needs to be disabled:

1. Stop the new replication cron.
2. Stop the new archive cron.
3. Keep `/home/epc_ai/coordination/streams` intact for forensic review.
4. Restore the previous PHP replication and archive scripts only after confirming which archive segments have already been rotated and which have been replicated.
