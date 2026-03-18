<?php
require_once 'config.php';
require_once 'log_message.php';

function all_replication_tables()
{
    return array_values(
        array_unique(
            array_merge(
                $GLOBALS['REPL_SUMMARY_TABLES'],
                $GLOBALS['REPL_SUMMARY_VOLTAGE_TABLES'],
                $GLOBALS['REPL_ID_TABLES']
            )
        )
    );
}

function archive_managed_tables()
{
    return array_values($GLOBALS['REPL_ARCHIVE_TABLES']);
}

function ensure_coordination_identifier($value)
{
    if (!preg_match('/^[A-Za-z0-9_]+$/', $value)) {
        throw new InvalidArgumentException("Unsafe identifier: {$value}");
    }
    return $value;
}

function ensure_coordination_directories()
{
    foreach (array($GLOBALS['REPL_COORDINATION_DIR'], $GLOBALS['REPL_STREAM_STATE_DIR']) as $path) {
        if (!is_dir($path) && !mkdir($path, 0775, true) && !is_dir($path)) {
            throw new RuntimeException("Failed to create coordination directory: {$path}");
        }
    }
}

function acquire_coordination_lock($blocking = false, $timeoutSeconds = 0, $pollSeconds = 1)
{
    ensure_coordination_directories();
    $handle = fopen($GLOBALS['REPL_LOCK_FILE'], 'c+');
    if ($handle === false) {
        throw new RuntimeException("Unable to open coordination lock: {$GLOBALS['REPL_LOCK_FILE']}");
    }

    $start = time();
    do {
        if (flock($handle, LOCK_EX | LOCK_NB)) {
            ftruncate($handle, 0);
            fwrite($handle, (string)getmypid() . PHP_EOL);
            fflush($handle);
            return $handle;
        }

        if (!$blocking) {
            fclose($handle);
            throw new RuntimeException("Unable to acquire coordination lock: busy");
        }

        if ($timeoutSeconds > 0 && (time() - $start) >= $timeoutSeconds) {
            fclose($handle);
            throw new RuntimeException("Timed out waiting for coordination lock");
        }
        sleep(max(1, (int)$pollSeconds));
    } while (true);
}

function release_coordination_lock($handle)
{
    if (is_resource($handle)) {
        flock($handle, LOCK_UN);
        fclose($handle);
    }
}

function default_open_segment($physicalTable, $epoch)
{
    return array(
        'epoch' => (int)$epoch,
        'physical_table' => ensure_coordination_identifier($physicalTable),
        'closed' => false,
        'max_id' => null,
        'copied_until_id' => 0,
        'replication_complete' => false,
        'archived_at' => null,
    );
}

function default_stream_state($logicalTable)
{
    $logicalTable = ensure_coordination_identifier($logicalTable);
    return array(
        'logical_table' => $logicalTable,
        'next_epoch' => 2,
        'segments' => array(
            default_open_segment($logicalTable, 1),
        ),
    );
}

function normalize_segment(array $segment)
{
    $normalized = array(
        'epoch' => (int)($segment['epoch'] ?? 0),
        'physical_table' => ensure_coordination_identifier((string)($segment['physical_table'] ?? '')),
        'closed' => (bool)($segment['closed'] ?? false),
        'max_id' => array_key_exists('max_id', $segment) && $segment['max_id'] !== null ? (int)$segment['max_id'] : null,
        'copied_until_id' => (int)($segment['copied_until_id'] ?? 0),
        'replication_complete' => (bool)($segment['replication_complete'] ?? false),
        'archived_at' => $segment['archived_at'] ?? null,
    );

    if (!$normalized['closed']) {
        $normalized['replication_complete'] = false;
        $normalized['max_id'] = null;
    } elseif ($normalized['max_id'] === null || $normalized['max_id'] <= 0) {
        $normalized['max_id'] = max(0, (int)$normalized['max_id']);
        $normalized['replication_complete'] = true;
        $normalized['copied_until_id'] = max(0, $normalized['copied_until_id']);
    } elseif ($normalized['copied_until_id'] >= $normalized['max_id']) {
        $normalized['replication_complete'] = true;
        $normalized['copied_until_id'] = $normalized['max_id'];
    }

    return $normalized;
}

function normalize_stream_state($logicalTable, array $state)
{
    $logicalTable = ensure_coordination_identifier($logicalTable);
    $segments = array();
    foreach (($state['segments'] ?? array()) as $segment) {
        $segments[] = normalize_segment($segment);
    }

    if (empty($segments)) {
        $segments[] = default_open_segment($logicalTable, 1);
    }

    usort(
        $segments,
        function ($left, $right) {
            return $left['epoch'] <=> $right['epoch'];
        }
    );

    $openCount = 0;
    $maxEpoch = 0;
    foreach ($segments as $segment) {
        if (!$segment['closed']) {
            $openCount += 1;
        }
        $maxEpoch = max($maxEpoch, $segment['epoch']);
    }

    if ($openCount !== 1) {
        throw new RuntimeException("State for {$logicalTable} must contain exactly one open segment");
    }

    $nextEpoch = (int)($state['next_epoch'] ?? ($maxEpoch + 1));
    $nextEpoch = max($nextEpoch, $maxEpoch + 1);

    return array(
        'logical_table' => $logicalTable,
        'next_epoch' => $nextEpoch,
        'segments' => array_values($segments),
    );
}

function stream_state_path($logicalTable)
{
    $logicalTable = ensure_coordination_identifier($logicalTable);
    return rtrim($GLOBALS['REPL_STREAM_STATE_DIR'], '/') . "/{$logicalTable}.json";
}

function write_stream_state($logicalTable, array $state)
{
    ensure_coordination_directories();
    $normalized = normalize_stream_state($logicalTable, $state);
    $path = stream_state_path($logicalTable);
    $tmpPath = $path . '.tmp.' . uniqid('', true);
    $payload = json_encode($normalized, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL;
    $handle = fopen($tmpPath, 'wb');
    if ($handle === false) {
        throw new RuntimeException("Failed to open temporary state file: {$tmpPath}");
    }
    if (fwrite($handle, $payload) === false) {
        fclose($handle);
        @unlink($tmpPath);
        throw new RuntimeException("Failed to write temporary state file: {$tmpPath}");
    }
    fflush($handle);
    if (function_exists('fsync')) {
        fsync($handle);
    }
    fclose($handle);
    if (!rename($tmpPath, $path)) {
        @unlink($tmpPath);
        throw new RuntimeException("Failed to atomically replace state file: {$path}");
    }
    return $normalized;
}

function load_stream_state($logicalTable, $autoCreate = true)
{
    ensure_coordination_directories();
    $path = stream_state_path($logicalTable);
    if (!file_exists($path)) {
        if (!$autoCreate) {
            return null;
        }
        return write_stream_state($logicalTable, default_stream_state($logicalTable));
    }

    $payload = file_get_contents($path);
    if ($payload === false) {
        throw new RuntimeException("Failed to read stream state: {$path}");
    }

    $decoded = json_decode($payload, true);
    if (!is_array($decoded)) {
        throw new RuntimeException("Invalid stream state JSON: {$path}");
    }
    return normalize_stream_state($logicalTable, $decoded);
}

function list_stream_state_files()
{
    ensure_coordination_directories();
    $files = glob(rtrim($GLOBALS['REPL_STREAM_STATE_DIR'], '/') . '/*.json') ?: array();
    sort($files);
    return $files;
}

function open_segment_index(array $state)
{
    $found = null;
    foreach ($state['segments'] as $index => $segment) {
        if (!$segment['closed']) {
            if ($found !== null) {
                throw new RuntimeException("Multiple open segments found for {$state['logical_table']}");
            }
            $found = $index;
        }
    }
    if ($found === null) {
        throw new RuntimeException("No open segment found for {$state['logical_table']}");
    }
    return $found;
}

function pending_closed_segment_indexes(array $state)
{
    $indexes = array();
    foreach ($state['segments'] as $index => $segment) {
        if ($segment['closed'] && !$segment['replication_complete']) {
            $indexes[] = $index;
        }
    }
    return $indexes;
}

function close_open_segment(array $state, $archiveName, $maxId, $archivedAt)
{
    $state = normalize_stream_state($state['logical_table'], $state);
    $openIndex = open_segment_index($state);
    $openSegment = $state['segments'][$openIndex];
    $nextEpoch = (int)$state['next_epoch'];

    $closedSegment = $openSegment;
    $closedSegment['physical_table'] = ensure_coordination_identifier($archiveName);
    $closedSegment['closed'] = true;
    $closedSegment['max_id'] = max(0, (int)$maxId);
    $closedSegment['archived_at'] = $archivedAt;
    $closedSegment = normalize_segment($closedSegment);

    $state['segments'][$openIndex] = $closedSegment;
    $state['segments'][] = default_open_segment($state['logical_table'], $nextEpoch);
    $state['next_epoch'] = $nextEpoch + 1;
    return normalize_stream_state($state['logical_table'], $state);
}

function update_segment_progress(array $state, $segmentIndex, $copiedUntilId)
{
    $segment = $state['segments'][$segmentIndex];
    $segment['copied_until_id'] = max((int)$segment['copied_until_id'], (int)$copiedUntilId);
    $state['segments'][$segmentIndex] = normalize_segment($segment);
    return normalize_stream_state($state['logical_table'], $state);
}

function remove_segments_by_physical_tables(array $state, array $physicalTables)
{
    $dropSet = array_flip($physicalTables);
    $state['segments'] = array_values(
        array_filter(
            $state['segments'],
            function ($segment) use ($dropSet) {
                return !isset($dropSet[$segment['physical_table']]);
            }
        )
    );
    return normalize_stream_state($state['logical_table'], $state);
}

function closed_segment_drop_decision(array $state, array $candidateTables, $requireComplete)
{
    $candidateSet = array_flip($candidateTables);
    $allow = array();
    $skip = array();

    foreach ($candidateTables as $table) {
        $table = ensure_coordination_identifier($table);
        $matched = null;
        foreach ($state['segments'] as $segment) {
            if ($segment['physical_table'] === $table) {
                $matched = $segment;
                break;
            }
        }

        if ($matched === null) {
            $skip[] = $table;
            continue;
        }

        if ($requireComplete && !$matched['replication_complete']) {
            $skip[] = $table;
            continue;
        }

        $allow[] = $table;
    }

    return array($allow, $skip);
}
