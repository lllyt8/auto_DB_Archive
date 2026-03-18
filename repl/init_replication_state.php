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

function build_bootstrap_state($dbSrc, $logicalTable)
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
                'copied_until_id' => 0,
                'replication_complete' => false,
                'archived_at' => null,
            );
            $epoch += 1;
        }
    }

    $segments[] = default_open_segment($logicalTable, $epoch);
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

try {
    $args = parse_init_args($argv);
    $lockHandle = acquire_coordination_lock(true, 60, 1);
    list($db, $dbConn) = mysql_connection();

    foreach ($args['tables'] as $logicalTable) {
        $statePath = stream_state_path($logicalTable);
        if (file_exists($statePath) && !$args['force']) {
            echo "skip {$logicalTable}: state already exists" . PHP_EOL;
            continue;
        }

        $state = build_bootstrap_state($dbConn, $logicalTable);
        write_stream_state($logicalTable, $state);
        echo "initialized {$logicalTable} segments=" . count($state['segments']) . PHP_EOL;
    }
} catch (Throwable $exc) {
    log_error("Init replication state failed: " . $exc->getMessage());
    fwrite(STDERR, $exc->getMessage() . PHP_EOL);
    exit(1);
} finally {
    if ($db instanceof Database) {
        $db->close_connection();
    }
    release_coordination_lock($lockHandle);
}
