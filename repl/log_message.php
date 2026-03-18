<?php

$log_path = "/home/epcenergy/logs/";
$log_name = "_rep.log";

$status_filename = "server_state.log";

function ensure_log_dir()
{
    if (!is_dir($GLOBALS['log_path'])) {
        mkdir($GLOBALS['log_path'], 0775, true);
    }
}

function log_message($m)
{
    ensure_log_dir();
    $date = new DateTime();
    error_log($date->format('Y-m-d H:i:s') . " LOG=" . "\"{$m}\"" . "\n", 3, $GLOBALS['log_path'] . $date->format('d') . $GLOBALS['log_name']);
}

function log_error($m)
{
    ensure_log_dir();
    $date = new DateTime();
    error_log($date->format('Y-m-d H:i:s') . " ERROR=" . "\"{$m}\"" . "\n", 3, $GLOBALS['log_path'] . $date->format('d') . $GLOBALS['log_name']);
}

function update_server_state_log($db_server) {
    
    try {
        // get current server status as associative array
        $status_file = $GLOBALS['log_path'] . $GLOBALS['status_filename'];
        $status_file_json = file_get_contents($status_file);
        $server_status = json_decode($status_file_json, true);

        // get current time and last error time
        $ts_now = time();
        $ts_last_err = $server_status[$db_server]['err_ts'];

        // increase error count by 1
        $server_status[$db_server]['err_count'] += 1;
        $err_count = $server_status[$db_server]['err_count'];

        // reset error counter if there is no error past 30 min
        if ($ts_now > $ts_last_err + 1800 && $err_count < 10) {
            $server_status[$db_server]['err_count'] = 0;
        }

        // trigger error state
        if ($err_count > 9) {
            $server_status[$db_server]['state'] = "error";
        }

        // update status log
        $server_status[$db_server]['err_ts'] = strval($ts_now);
        $server_status[$db_server]['err_count'] = strval($err_count);
        $f = fopen($status_file, 'w') or die();
        fwrite($f, json_encode($server_status));
        fclose($f);
    } catch ( exception $e) {
        echo "update server state log failed: " . $e->getMessage();
    }
}

function get_server_state($db_server) {
    try {
        // get current server status as associative array
        $status_file = $GLOBALS['log_path'] . $GLOBALS['status_filename'];
        if (!file_exists($status_file)) {
            return 'online';
        }
        $status_file_json = file_get_contents($status_file);
        $server_status = json_decode($status_file_json, true);

         // 'online' or 'error'
        return $server_status[$db_server]['state'];
    } catch ( exception $e) {
        echo "get server state failed: " . $e->getMessage();
        return 'error';
    }
}
