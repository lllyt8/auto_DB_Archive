# HostGator DB Archive Runbook

## 1) Prepare files on server
1. Copy `scripts/db_archive.py` to `/home/epc_ai/aidetect/scripts/db_archive.py`.
2. Copy `config/db_archive.yaml` to `/home/epc_ai/aidetect/config/db_archive.yaml`.
3. Edit `/home/epc_ai/aidetect/config/db_archive.yaml` with production credentials.
4. Verify local sendmail exists:
```bash
which sendmail
ls -l /usr/sbin/sendmail
```

## 2) Install Python dependencies
```bash
python3 -m pip install --user mysql-connector-python pyyaml
```

## 3) Grant minimal DB permissions
1. Login with MySQL admin account.
2. Run `sql/grants_db_archive.sql` after replacing placeholders.
3. Verify:
```sql
SHOW GRANTS FOR 'ARCHIVE_USER'@'localhost';
```

## 4) Validate in dry-run
```bash
cd /home/epc_ai/aidetect
python3 scripts/db_archive.py --config config/db_archive.yaml --dry-run
```

Expected result:
1. Exit code `0`
2. Log shows planned archive names and planned drops.
3. No table rename/create/drop occurs.

## 5) Validate local mail channel (sendmail)
```bash
python3 - <<'PY'
import subprocess
msg = "From: noreply@epcenergy.app\nTo: it@epcenergy.io\nSubject: DB Archive sendmail test\n\nsendmail test\n"
res = subprocess.run(["/usr/sbin/sendmail", "-t", "-i"], input=msg, text=True, capture_output=True)
print("rc=", res.returncode)
print("stderr=", res.stderr.strip())
PY
```

## 6) Run single-table live test
```bash
cd /home/epc_ai/aidetect
python3 scripts/db_archive.py --config config/db_archive.yaml --run --table ess_string_DLN
```

Validation SQL:
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema='epcenergy_ess_01'
  AND (table_name='ess_string_DLN' OR table_name LIKE 'ess_string_DLN_a%')
ORDER BY table_name;
```

## 7) Weekly cron (Sunday 02:10 EST)
Add to `crontab -e`:
```cron
10 2 * * 0 cd /home/epc_ai/aidetect && /usr/bin/python3 scripts/db_archive.py --config config/db_archive.yaml --run >> /home/epc_ai/aidetect/logs/db_archive_cron.log 2>&1
```

## 8) Operational checks
1. Confirm one email is received per run with `DB Archive Success` or `DB Archive Failed`.
2. Confirm `/home/epc_ai/aidetect/logs/db_archive.log` rotates daily and retains 90 files.
3. Confirm archive tables are capped at 4 generations per source table.
4. Confirm main tables continue receiving writes after each rotation.

## 9) Rollback and incident handling
1. Disable cron entry to stop further rotations.
2. If a specific table failed after rename but before create, recreate from latest archive:
```sql
CREATE TABLE epcenergy_ess_01.ess_string_HON LIKE epcenergy_ess_01.ess_string_HON_aYYMMDDHHMM;
```
3. Re-run script only for failed table after verification:
```bash
python3 scripts/db_archive.py --config config/db_archive.yaml --run --table ess_string_HON
```
