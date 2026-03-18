<?php
require_once 'connection.php';
require_once 'coordination.php';
require_once 'copy_method.php';
require_once 'log_message.php';

function parse_recovery_args($argv)
{
    $apply = false;
    $dryRun = true;
    $selected = array();
    $allowed = array_flip(all_replication_tables());

    for ($index = 1; $index < count($argv); $index += 1) {
        if ($argv[$index] === '--apply') {
            $apply = true;
            $dryRun = false;
            continue;
        }
        if ($argv[$index] === '--dry-run') {
            $dryRun = true;
            $apply = false;
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
        'apply' => $apply,
        'dry_run' => $dryRun,
        'tables' => empty($selected) ? all_replication_tables() : array_values(array_unique($selected)),
    );
}

function recovery_source_connection()
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

function recovery_target_connection()
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

$lockHandle = null;
$sourceDb = null;
$targetDb = null;

try {
    $args = parse_recovery_args($argv);
    $lockHandle = acquire_coordination_lock(true, 60, 1);
    list($sourceDb, $sourceConn) = recovery_source_connection();
    list($targetDb, $targetConn) = recovery_target_connection();

    foreach ($args['tables'] as $logicalTable) {
        $state = load_stream_state($logicalTable, false);
        if ($state === null) {
            throw new RuntimeException(
                "Missing stream state for {$logicalTable}. Run php repl/init_replication_state.php first."
            );
        }
        $result = process_stream_replication(
            $sourceConn,
            $targetConn,
            $logicalTable,
            $state,
            $GLOBALS['REPL_RECOVERY_BATCH_SIZE'],
            $args['apply'],
            function ($updatedState) use ($logicalTable, $args) {
                if ($args['apply']) {
                    write_stream_state($logicalTable, $updatedState);
                }
            }
        );

        if ($args['apply']) {
            write_stream_state($logicalTable, $result['state']);
        }

        echo sprintf(
            "%s table=%s source_rows=%d target_matches=%d missing_rows=%d inserted_rows=%d segments_completed=%s\n",
            $args['apply'] ? 'apply' : 'dry-run',
            $logicalTable,
            $result['summary']['source_rows'],
            $result['summary']['target_matches'],
            $result['summary']['missing_rows'],
            $result['summary']['inserted_rows'],
            json_encode($result['summary']['segments_completed'], JSON_UNESCAPED_SLASHES)
        );
    }
} catch (Throwable $exc) {
    log_error("Recover missing rows failed: " . $exc->getMessage());
    fwrite(STDERR, $exc->getMessage() . PHP_EOL);
    exit(1);
} finally {
    if ($sourceDb instanceof Database) {
        $sourceDb->close_connection();
    }
    if ($targetDb instanceof Database) {
        $targetDb->close_connection();
    }
    release_coordination_lock($lockHandle);
}
