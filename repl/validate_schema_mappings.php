<?php
require_once 'connection.php';
require_once 'coordination.php';
require_once 'copy_method.php';

function validator_source_connection()
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

function validator_target_connection()
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

function parse_validator_args($argv)
{
    $allowed = array_flip(all_replication_tables());
    $selected = array();

    for ($index = 1; $index < count($argv); $index += 1) {
        if ($argv[$index] !== '--table') {
            throw new InvalidArgumentException("Unknown argument: {$argv[$index]}");
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

    return empty($selected) ? all_replication_tables() : array_values(array_unique($selected));
}

function source_columns_for_table($dbSrc, $table)
{
    $stmt = $dbSrc->query('SHOW COLUMNS FROM ' . quote_mysql_identifier($table));
    $columns = array();
    foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
        $columns[] = $row['Field'];
    }
    return $columns;
}

$sourceDb = null;
$targetDb = null;

try {
    $tables = parse_validator_args($argv);
    list($sourceDb, $sourceConn) = validator_source_connection();
    list($targetDb, $targetConn) = validator_target_connection();

    foreach ($tables as $table) {
        try {
            $sourceColumns = source_columns_for_table($sourceConn, $table);
            $map = build_target_column_map($targetConn, $table, $sourceColumns);
            $skipped = array_values(array_diff($sourceColumns, array_keys($map)));
            sort($skipped);
            sort($sourceColumns);
            $targetColumns = fetch_target_columns($targetConn, $table);
            sort($targetColumns);

            echo json_encode(
                array(
                    'table' => $table,
                    'status' => 'ok',
                    'source_columns' => $sourceColumns,
                    'target_columns' => $targetColumns,
                    'mapped_columns' => $map,
                    'skipped_source_columns' => $skipped,
                ),
                JSON_UNESCAPED_SLASHES
            ) . PHP_EOL;
        } catch (Throwable $exc) {
            echo json_encode(
                array(
                    'table' => $table,
                    'status' => 'error',
                    'error' => $exc->getMessage(),
                ),
                JSON_UNESCAPED_SLASHES
            ) . PHP_EOL;
        }
    }
} finally {
    if ($sourceDb instanceof Database) {
        $sourceDb->close_connection();
    }
    if ($targetDb instanceof Database) {
        $targetDb->close_connection();
    }
}
