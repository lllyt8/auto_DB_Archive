<?php
require_once 'connection.php';
require_once 'coordination.php';
require_once 'copy_method.php';
require_once 'log_message.php';

function parse_selected_tables($argv)
{
    $allowed = array_flip(all_replication_tables());
    $selected = array();

    for ($index = 1; $index < count($argv); $index += 1) {
        if ($argv[$index] !== '--table') {
            continue;
        }
        if (!isset($argv[$index + 1])) {
            throw new InvalidArgumentException("--table requires a value");
        }
        $table = ensure_coordination_identifier($argv[$index + 1]);
        if (!isset($allowed[$table])) {
            throw new InvalidArgumentException("Unknown logical table: {$table}");
        }
        $selected[] = $table;
        $index += 1;
    }

    if (empty($selected)) {
        return all_replication_tables();
    }

    return array_values(array_unique($selected));
}

function open_source_connection()
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

function open_target_connection()
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

$startTime = microtime(true);
$exitCode = 0;
$lockHandle = null;
$sourceDb = null;
$sourceConn = null;
$targetDb = null;
$targetConn = null;

try {
    $selectedTables = parse_selected_tables($argv);
} catch (Throwable $exc) {
    log_error("Replication argument error: " . $exc->getMessage());
    fwrite(STDERR, $exc->getMessage() . PHP_EOL);
    exit(1);
}

try {
    $lockHandle = acquire_coordination_lock(false);
} catch (Throwable $exc) {
    $message = "Replication busy_skip: " . $exc->getMessage();
    log_message($message);
    echo $message . PHP_EOL;
    exit(0);
}

try {
    list($sourceDb, $sourceConn) = open_source_connection();
    list($targetDb, $targetConn) = open_target_connection();

    $summaries = array();
    foreach ($selectedTables as $logicalTable) {
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
            $GLOBALS['REPL_BATCH_SIZE'],
            true,
            function ($updatedState) use ($logicalTable) {
                write_stream_state($logicalTable, $updatedState);
            }
        );
        write_stream_state($logicalTable, $result['state']);
        $summaries[] = $result['summary'];
    }

    foreach ($summaries as $summary) {
        log_message(
            sprintf(
                'Replication summary table=%s source_rows=%d target_matches=%d missing_rows=%d inserted_rows=%d segments_completed=%s',
                $summary['logical_table'],
                $summary['source_rows'],
                $summary['target_matches'],
                $summary['missing_rows'],
                $summary['inserted_rows'],
                json_encode($summary['segments_completed'], JSON_UNESCAPED_SLASHES)
            )
        );
    }
} catch (Throwable $exc) {
    $exitCode = 1;
    $message = "Replication failed: " . $exc->getMessage();
    log_error($message);
    echo $message . PHP_EOL;
} finally {
    if ($sourceDb instanceof Database) {
        $sourceDb->close_connection();
    }
    if ($targetDb instanceof Database) {
        $targetDb->close_connection();
    }
    release_coordination_lock($lockHandle);
}

$durationMessage = "Data Replication Completed: " . round((microtime(true) - $startTime), 2) . "sec";
log_message($durationMessage);
exit($exitCode);
