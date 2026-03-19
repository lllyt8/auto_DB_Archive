<?php
require_once 'log_message.php';
require_once 'coordination.php';

function quote_mysql_identifier($identifier)
{
    return '`' . ensure_coordination_identifier($identifier) . '`';
}

function quote_pg_identifier($identifier)
{
    return '"' . str_replace('"', '""', (string)$identifier) . '"';
}

function fetch_source_max_id($dbSrc, $physicalTable)
{
    $sql = 'SELECT COALESCE(MAX(id), 0) AS max_id FROM ' . quote_mysql_identifier($physicalTable);
    $stmt = $dbSrc->prepare($sql);
    $stmt->execute();
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return (int)($row['max_id'] ?? 0);
}

function fetch_source_batch($dbSrc, $physicalTable, $copiedUntilId, $highWaterId, $batchSize)
{
    if ($highWaterId <= $copiedUntilId) {
        return array();
    }

    $sql = 'SELECT * FROM ' . quote_mysql_identifier($physicalTable)
        . ' WHERE id > :copied_until_id AND id <= :high_water_id ORDER BY id ASC LIMIT '
        . (int)$batchSize;
    $stmt = $dbSrc->prepare($sql);
    $stmt->execute(
        array(
            'copied_until_id' => (int)$copiedUntilId,
            'high_water_id' => (int)$highWaterId,
        )
    );
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

function normalize_value_for_hash($value)
{
    if ($value === null) {
        return null;
    }
    if (is_bool($value)) {
        return $value ? '1' : '0';
    }
    return (string)$value;
}

function normalize_row_for_hash(array $row, array $columns)
{
    $normalized = array();
    foreach ($columns as $column) {
        $normalized[$column] = normalize_value_for_hash($row[$column] ?? null);
    }
    return $normalized;
}

function hash_row(array $row, array $columns)
{
    return hash(
        'sha256',
        json_encode(
            normalize_row_for_hash($row, $columns),
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
        )
    );
}

function canonicalize_column_name($column)
{
    return strtolower(preg_replace('/[^a-z0-9]/i', '', (string)$column));
}

function resolve_target_table_name($dbTarget, $logicalTable)
{
    static $cache = array();
    if (isset($cache[$logicalTable])) {
        return $cache[$logicalTable];
    }

    $sql = 'SELECT table_name FROM information_schema.tables'
        . ' WHERE table_schema = :schema_name AND lower(table_name) = lower(:table_name)'
        . ' ORDER BY table_name ASC';
    $stmt = $dbTarget->prepare($sql);
    $stmt->execute(
        array(
            'schema_name' => 'public',
            'table_name' => $logicalTable,
        )
    );

    $matches = array();
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $matches[] = $row['table_name'];
    }

    if (empty($matches)) {
        throw new RuntimeException("Target table not found: {$logicalTable}");
    }
    if (count($matches) > 1) {
        throw new RuntimeException("Target table lookup is ambiguous for {$logicalTable}");
    }

    $cache[$logicalTable] = $matches[0];
    return $matches[0];
}

function fetch_target_columns($dbTarget, $logicalTable)
{
    static $cache = array();
    if (isset($cache[$logicalTable])) {
        return $cache[$logicalTable];
    }

    $targetTable = resolve_target_table_name($dbTarget, $logicalTable);
    $sql = 'SELECT column_name FROM information_schema.columns'
        . ' WHERE table_schema = :schema_name AND table_name = :table_name'
        . ' ORDER BY ordinal_position ASC';
    $stmt = $dbTarget->prepare($sql);
    $stmt->execute(
        array(
            'schema_name' => 'public',
            'table_name' => $targetTable,
        )
    );

    $columns = array();
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $columns[] = $row['column_name'];
    }

    if (empty($columns)) {
        throw new RuntimeException("Target table has no columns: {$targetTable}");
    }

    $cache[$logicalTable] = $columns;
    return $columns;
}

function build_target_column_map($dbTarget, $logicalTable, array $sourceColumns)
{
    static $cache = array();
    static $skippedWarnings = array();
    $cacheKey = $logicalTable . '|' . implode(',', $sourceColumns);
    if (isset($cache[$cacheKey])) {
        return $cache[$cacheKey];
    }

    $targetColumns = fetch_target_columns($dbTarget, $logicalTable);
    $exact = array_fill_keys($targetColumns, true);
    $canonical = array();
    foreach ($targetColumns as $targetColumn) {
        $canonicalKey = canonicalize_column_name($targetColumn);
        if (!isset($canonical[$canonicalKey])) {
            $canonical[$canonicalKey] = array();
        }
        $canonical[$canonicalKey][] = $targetColumn;
    }

    $mapping = array();
    $skippedColumns = array();
    foreach ($sourceColumns as $sourceColumn) {
        if (isset($exact[$sourceColumn])) {
            $mapping[$sourceColumn] = $sourceColumn;
            continue;
        }

        $canonicalKey = canonicalize_column_name($sourceColumn);
        $matches = $canonical[$canonicalKey] ?? array();
        if (count($matches) === 1) {
            $mapping[$sourceColumn] = $matches[0];
            continue;
        }
        if (count($matches) > 1) {
            throw new RuntimeException(
                "Ambiguous target column mapping for {$sourceColumn} on {$logicalTable}"
            );
        }

        $skippedColumns[] = $sourceColumn;
    }

    if (empty($mapping)) {
        throw new RuntimeException("No shared columns found for target table {$logicalTable}");
    }

    if (!empty($skippedColumns)) {
        sort($skippedColumns);
        $warningKey = $logicalTable . '|' . implode(',', $skippedColumns);
        if (!isset($skippedWarnings[$warningKey])) {
            log_message(
                'Skipping unmapped source columns table=' . $logicalTable . ' columns=' . implode(',', $skippedColumns)
            );
            $skippedWarnings[$warningKey] = true;
        }
    }

    $cache[$cacheKey] = $mapping;
    return $mapping;
}

function remap_source_rows(array $sourceRows, array $columnMap)
{
    $mappedRows = array();
    foreach ($sourceRows as $row) {
        $mapped = array();
        foreach ($columnMap as $sourceColumn => $targetColumn) {
            $mapped[$targetColumn] = $row[$sourceColumn] ?? null;
        }
        $mappedRows[] = $mapped;
    }
    return $mappedRows;
}

function chunked_target_candidates($dbTarget, $targetTable, array $columns, array $sourceRows)
{
    if (empty($sourceRows)) {
        return array();
    }

    $hasTs = in_array('ts', $columns, true);
    $ids = array_values(
        array_unique(
            array_map(
                function ($row) {
                    return (int)$row['id'];
                },
                $sourceRows
            )
        )
    );

    $tableSql = quote_pg_identifier($targetTable);
    $columnSql = implode(', ', array_map('quote_pg_identifier', $columns));
    $rows = array();
    $minTs = null;
    $maxTs = null;
    if ($hasTs) {
        $tsValues = array_map(
            function ($row) {
                return (string)$row['ts'];
            },
            $sourceRows
        );
        $minTs = min($tsValues);
        $maxTs = max($tsValues);
    }

    foreach (array_chunk($ids, 500) as $chunkIndex => $chunk) {
        $params = array();
        $placeholders = array();
        foreach ($chunk as $idIndex => $idValue) {
            $name = ':id_' . $chunkIndex . '_' . $idIndex;
            $params[$name] = (int)$idValue;
            $placeholders[] = $name;
        }

        $sql = 'SELECT ' . $columnSql . ' FROM ' . $tableSql
            . ' WHERE ' . quote_pg_identifier('id') . ' IN (' . implode(', ', $placeholders) . ')';
        if ($hasTs) {
            $sql .= ' AND ' . quote_pg_identifier('ts') . ' BETWEEN :min_ts_' . $chunkIndex
                . ' AND :max_ts_' . $chunkIndex;
            $params[':min_ts_' . $chunkIndex] = $minTs;
            $params[':max_ts_' . $chunkIndex] = $maxTs;
        }

        $stmt = $dbTarget->prepare($sql);
        foreach ($params as $name => $value) {
            $stmt->bindValue($name, $value);
        }
        $stmt->execute();
        $rows = array_merge($rows, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    return $rows;
}

function filter_missing_rows(array $sourceRows, array $targetRows, array $columns)
{
    $targetHashes = array();
    foreach ($targetRows as $targetRow) {
        $targetHashes[hash_row($targetRow, $columns)] = true;
    }

    $missingRows = array();
    $targetMatchCount = 0;
    foreach ($sourceRows as $sourceRow) {
        $hash = hash_row($sourceRow, $columns);
        if (isset($targetHashes[$hash])) {
            $targetMatchCount += 1;
            continue;
        }
        $missingRows[] = $sourceRow;
    }

    return array($missingRows, $targetMatchCount);
}

function insert_rows($dbTarget, $targetTable, array $columns, array $rows)
{
    if (empty($rows)) {
        return 0;
    }

    $tableSql = quote_pg_identifier($targetTable);
    $columnSql = implode(', ', array_map('quote_pg_identifier', $columns));
    $valueSql = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
    $sql = 'INSERT INTO ' . $tableSql . ' (' . $columnSql . ') VALUES ' . $valueSql;
    $stmt = $dbTarget->prepare($sql);

    $inserted = 0;
    $dbTarget->beginTransaction();
    try {
        foreach ($rows as $row) {
            $values = array();
            foreach ($columns as $column) {
                $values[] = $row[$column] ?? null;
            }
            $stmt->execute($values);
            $inserted += 1;
        }
        $dbTarget->commit();
    } catch (Throwable $exc) {
        if ($dbTarget->inTransaction()) {
            $dbTarget->rollBack();
        }
        throw $exc;
    }

    return $inserted;
}

function copy_segment_batch(
    $dbSrc,
    $dbTarget,
    $logicalTable,
    $physicalTable,
    $copiedUntilId,
    $highWaterId,
    $batchSize,
    $apply
) {
    $sourceRows = fetch_source_batch($dbSrc, $physicalTable, $copiedUntilId, $highWaterId, $batchSize);
    if (empty($sourceRows)) {
        return array(
            'source_rows' => 0,
            'target_matches' => 0,
            'missing_rows' => 0,
            'inserted_rows' => 0,
            'next_copied_until_id' => (int)$copiedUntilId,
        );
    }

    $targetTable = resolve_target_table_name($dbTarget, $logicalTable);
    $sourceColumns = array_keys($sourceRows[0]);
    $columnMap = build_target_column_map($dbTarget, $logicalTable, $sourceColumns);
    $columns = array_values($columnMap);
    $mappedSourceRows = remap_source_rows($sourceRows, $columnMap);
    $targetRows = chunked_target_candidates($dbTarget, $targetTable, $columns, $mappedSourceRows);
    list($missingRows, $targetMatches) = filter_missing_rows($mappedSourceRows, $targetRows, $columns);

    $insertedRows = 0;
    if ($apply && !empty($missingRows)) {
        $insertedRows = insert_rows($dbTarget, $targetTable, $columns, $missingRows);
    }

    $nextCopiedUntilId = max(
        array_map(
            function ($row) {
                return (int)$row['id'];
            },
            $sourceRows
        )
    );

    return array(
        'source_rows' => count($sourceRows),
        'target_matches' => $targetMatches,
        'missing_rows' => count($missingRows),
        'inserted_rows' => $insertedRows,
        'next_copied_until_id' => $nextCopiedUntilId,
    );
}

function process_stream_replication(
    $dbSrc,
    $dbTarget,
    $logicalTable,
    array $state,
    $batchSize,
    $apply,
    $stateUpdater = null
) {
    $summary = array(
        'logical_table' => $logicalTable,
        'source_rows' => 0,
        'target_matches' => 0,
        'missing_rows' => 0,
        'inserted_rows' => 0,
        'segments_completed' => array(),
    );

    $state = normalize_stream_state($logicalTable, $state);
    $segmentOrder = array_merge(pending_closed_segment_indexes($state), array(open_segment_index($state)));

    foreach ($segmentOrder as $segmentIndex) {
        $segment = $state['segments'][$segmentIndex];
        $highWaterId = $segment['closed']
            ? (int)($segment['max_id'] ?? 0)
            : fetch_source_max_id($dbSrc, $segment['physical_table']);

        if ($segment['closed'] && $highWaterId <= 0) {
            $state = update_segment_progress($state, $segmentIndex, 0);
            if ($apply && is_callable($stateUpdater)) {
                $stateUpdater($state);
            }
            $summary['segments_completed'][] = $segment['physical_table'];
            continue;
        }

        while ((int)$state['segments'][$segmentIndex]['copied_until_id'] < $highWaterId) {
            $segment = $state['segments'][$segmentIndex];
            $batchResult = copy_segment_batch(
                $dbSrc,
                $dbTarget,
                $logicalTable,
                $segment['physical_table'],
                (int)$segment['copied_until_id'],
                $highWaterId,
                (int)$batchSize,
                $apply
            );

            if ($batchResult['source_rows'] === 0) {
                throw new RuntimeException(
                    "No rows returned while replication expected progress for {$segment['physical_table']}"
                );
            }

            $state = update_segment_progress($state, $segmentIndex, $batchResult['next_copied_until_id']);
            if ($apply && is_callable($stateUpdater)) {
                $stateUpdater($state);
            }

            $summary['source_rows'] += $batchResult['source_rows'];
            $summary['target_matches'] += $batchResult['target_matches'];
            $summary['missing_rows'] += $batchResult['missing_rows'];
            $summary['inserted_rows'] += $apply ? $batchResult['inserted_rows'] : 0;
        }

        if ($state['segments'][$segmentIndex]['closed'] && $state['segments'][$segmentIndex]['replication_complete']) {
            $summary['segments_completed'][] = $state['segments'][$segmentIndex]['physical_table'];
        }
    }

    return array(
        'state' => $state,
        'summary' => $summary,
    );
}
