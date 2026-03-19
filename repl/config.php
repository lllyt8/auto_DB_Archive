<?php
/** ------------- SOURCE DATABASE ------------------------- */
/** Production Server */
$DB_TYPE = 'mysql';
$DB_SERVER = "localhost";
$DB_USER = "epcenergy_app";
$DB_PW = "epc@20201010";
$DB_NAME = "epcenergy_ess_01";

/** Local Timescale Database (POSTGRES) */
$DBTS_TYPE = 'pgsql';
$DBTS_SERVER = "ol7985ua81.b49qsab0at.tsdb.cloud.timescale.com";
$DBTS_USER = "tsdbadmin";
$DBTS_PW = "rpltd7b7w9gnka4s";
$DBTS_NAME = "tsdb";

/** ------------------------------------------------------- */
$REPL_COORDINATION_DIR = "/home/epc_ai/coordination";
$REPL_LOCK_FILE = $REPL_COORDINATION_DIR . "/db_repl_archive.lock";
$REPL_STREAM_STATE_DIR = $REPL_COORDINATION_DIR . "/streams";
$REPL_BATCH_SIZE = 2000;
$REPL_RECOVERY_BATCH_SIZE = 1000;

$REPL_SUMMARY_TABLES = array(
    "ess_string_0000",
    "ess_string_DLN",
    "ess_string_NAAK",
    "ess_string_HON",
    "ess_string_HONSJ"
);

$REPL_SUMMARY_VOLTAGE_TABLES = array(
    "ess_string_voltage"
);

$REPL_ID_TABLES = array(
    "ess_string_config",
    "ess_system_config"
);

$REPL_ARCHIVE_TABLES = array(
    "ess_string_0000",
    "ess_string_DLN",
    "ess_string_HON",
    "ess_string_HONSJ"
);
