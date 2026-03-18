<?php
require_once 'connection.php';
require_once 'coordination.php';
require_once 'copy_method.php';
require_once 'log_message.php';

function parse_init_args($argv)
{
    $force = false;
    $selected = array();
    $allowed = array_flip(all_replication_tables());

    for ($index = 1; $index < count($argv); $index += 1) {
        if ($argv[$index] === '--force') {
            $force = true;
            continue;
        }
        if ($argv[$index] === '--table') {
            if (!isset($argv[$index + 1])) {
                throw new InvalidArgumentException("--table requires a value");
            }
            $table = ensure_coordination_identifier($argv[$index + 1]);
            if (!isset($allowed[$table])) {
                throw new InvalidArgumentException("Unknown logical table: {$table}");
            }
            $selected[] = $table;
            $index += 1;
            continue;
        }
        throw new InvalidArgumentException("Unknown argument: {$argv[$index]}");
    }

    return array(
        'force' => $force,
        'tables' => empty($selected) ? all_replication_tables() : array_values(array_unique($selected)),
    );
}

function mysql_connection()
{
    $source = new Database(
        $GLOBALS['DB_TYPE'],
        $GLOBALS['DB_SERVER'],
        '',
        $GLOBALS['DB_USER'],
        $GLOBALS['DB_PW'],
        $GLOBALS['DB_NAME']
    );
    $connection = $source->connect();
    if (!$connection) {
        throw new RuntimeException("Connection to source MySQL failed");
    }
    return array($source, $connection);
}

function target_connection()
{
    $target = new Database(
        $GLOBALS['DBTS_TYPE'],
        $GLOBALS['DBTS_SERVER'],
        '31277',
        $GLOBALS['DBTS_USER'],
        $GLOBALS['DBTS_PW'],
        $GLOBALS['DBTS_NAME']
    );
    $connection = $target->connect_pg();
    if (!$connection) {
        throw new RuntimeException("Connection to target Timescale failed");
    }
    return array($target, $connection);
}

function list_archive_tables_for_logical($dbSrc, $logicalTable)
{
    $sql = '
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema_name
          AND table_name LIKE :table_name
        ORDER BY table_name ASC
    ';
    $stmt = $dbSrc->prepare($sql);
    $stmt->execute(
        array(
            'schema_name' => $GLOBALS['DB_NAME'],
            'table_name' => $logicalTable . '_a%',
        )
    );

    $matches = array();
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $tableName = $row['table_name'];
        if (preg_match('/^' . preg_quote($logicalTable, '/') . '_a\d{8,12}$/', $tableName)) {
            $matches[] = $tableName;
        }
    }
    sort($matches);
    return $matches;
}

function source_table_has_column($dbSrc, $physicalTable, $columnName)
{
    static $cache = array();
    $cacheKey = $physicalTable . '|' . $columnName;
    if (isset($cache[$cacheKey])) {
        return $cache[$cacheKey];
    }

    $sql = '
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
          AND column_name = :column_name
        LIMIT 1
    ';
    $stmt = $dbSrc->prepare($sql);
    $stmt->execute(
        array(
            'schema_name' => $GLOBALS['DB_NAME'],
            'table_name' => $physicalTable,
            'column_name' => $columnName,
        )
    );

    $cache[$cacheKey] = (bool)$stmt->fetchColumn();
    return $cache[$cacheKey];
}

function fetch_target_max_ts($dbTarget, $logicalTable)
{
    static $cache = array();
    if (array_key_exists($logicalTable, $cache)) {
        return $cache[$logicalTable];
    }

    $targetColumns = fetch_target_columns($dbTarget, $logicalTable);
    if (!in_array('ts', $targetColumns, true)) {
        $cache[$logicalTable] = null;
        return null;
    }

    $targetTable = resolve_target_table_name($dbTarget, $logicalTable);
    $sql = 'SELECT MAX(' . quote_pg_identifier('ts') . ') AS max_ts FROM ' . quote_pg_identifier($targetTable);
    $stmt = $dbTarget->prepare($sql);
    $stmt->execute();
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    $cache[$logicalTable] = $row['max_ts'] ?? null;
    return $cache[$logicalTable];
}

function fetch_target_max_id($dbTarget, $logicalTable)
{
    static $cache = array();
    if (array_key_exists($logicalTable, $cache)) {
        return $cache[$logicalTable];
    }

    $targetColumns = fetch_target_columns($dbTarget, $logicalTable);
    if (!in_array('id', $targetColumns, true)) {
        $cache[$logicalTable] = 0;
        return 0;
    }

    $targetTable = resolve_target_table_name($dbTarget, $logicalTable);
    $sql = 'SELECT COALESCE(MAX(' . quote_pg_identifier('id') . '), 0) AS max_id FROM ' . quote_pg_identifier($targetTable);
    $stmt = $dbTarget->prepare($sql);
    $stmt->execute();
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    $cache[$logicalTable] = (int)($row['max_id'] ?? 0);
    return $cache[$logicalTable];
}

function bootstrap_copied_until_id_from_ts($dbSrc, $physicalTable, $sourceMaxId, $targetMaxTs)
{
    if ($targetMaxTs === null || $sourceMaxId <= 0) {
        return 0;
    }

    $sql = 'SELECT COALESCE(MAX(id), 0) AS max_id FROM ' . quote_mysql_identifier($physicalTable)
        . ' WHERE ts < :target_max_ts';
    $stmt = $dbSrc->prepare($sql);
    $stmt->execute(
        array(
            'target_max_ts' => $targetMaxTs,
        )
    );
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return min($sourceMaxId, (int)($row['max_id'] ?? 0));
}

function bootstrap_copied_until_id_from_id($sourceMaxId, $targetMaxId)
{
    if ($sourceMaxId <= 0 || $targetMaxId <= 0) {
        return 0;
    }
    return min($sourceMaxId, max(0, $targetMaxId - 1));
}

function determine_bootstrap_copied_until_id($dbSrc, $dbTarget, $logicalTable, $physicalTable)
{
    $sourceMaxId = fetch_source_max_id($dbSrc, $physicalTable);
    if ($sourceMaxId <= 0) {
        return 0;
    }

    if (source_table_has_column($dbSrc, $physicalTable, 'ts')) {
        $targetMaxTs = fetch_target_max_ts($dbTarget, $logicalTable);
        if ($targetMaxTs !== null) {
            return bootstrap_copied_until_id_from_ts($dbSrc, $physicalTable, $sourceMaxId, $targetMaxTs);
        }
    }

    return bootstrap_copied_until_id_from_id($sourceMaxId, fetch_target_max_id($dbTarget, $logicalTable));
}

function build_bootstrap_state($dbSrc, $dbTarget, $logicalTable)
{
    $segments = array();
    $epoch = 1;
    if (in_array($logicalTable, archive_managed_tables(), true)) {
        foreach (list_archive_tables_for_logical($dbSrc, $logicalTable) as $archiveTable) {
            $segments[] = array(
                'epoch' => $epoch,
                'physical_table' => $archiveTable,
                'closed' => true,
                'max_id' => fetch_source_max_id($dbSrc, $archiveTable),
                'copied_until_id' => determine_bootstrap_copied_until_id($dbSrc, $dbTarget, $logicalTable, $archiveTable),
                'replication_complete' => false,
                'archived_at' => null,
            );
            $epoch += 1;
        }
    }

    $openSegment = default_open_segment($logicalTable, $epoch);
    $openSegment['copied_until_id'] = determine_bootstrap_copied_until_id($dbSrc, $dbTarget, $logicalTable, $logicalTable);
    $segments[] = $openSegment;

    return normalize_stream_state(
        $logicalTable,
        array(
            'logical_table' => $logicalTable,
            'next_epoch' => $epoch + 1,
            'segments' => $segments,
        )
    );
}

$lockHandle = null;
$db = null;
$targetDb = null;

try {
    $args = parse_init_args($argv);
    $lockHandle = acquire_coordination_lock(true, 60, 1);
    list($db, $dbConn) = mysql_connection();
    list($targetDb, $targetConn) = target_connection();

    foreach ($args['tables'] as $logicalTable) {
        $statePath = stream_state_path($logicalTable);
        if (file_exists($statePath) && !$args['force']) {
            echo "skip {$logicalTable}: state already exists" . PHP_EOL;
            continue;
        }

        $state = build_bootstrap_state($dbConn, $targetConn, $logicalTable);
        write_stream_state($logicalTable, $state);
        echo "initialized {$logicalTable} segments=" . count($state['segments']) . " copied_until_id="
            . (int)$state['segments'][count($state['segments']) - 1]['copied_until_id'] . PHP_EOL;
    }
} catch (Throwable $exc) {
    log_error("Init replication state failed: " . $exc->getMessage());
    fwrite(STDERR, $exc->getMessage() . PHP_EOL);
    exit(1);
} finally {
    if ($db instanceof Database) {
        $db->close_connection();
    }
    if ($targetDb instanceof Database) {
        $targetDb->close_connection();
    }
    release_coordination_lock($lockHandle);
}
