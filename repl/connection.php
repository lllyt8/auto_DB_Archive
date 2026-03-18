<?php

require_once 'config.php';
require_once 'log_message.php';

class Database
{
    private $DB_TYPE;
    private $DB_SERVER;
    private $DB_USER;
    private $DB_PW;
    private $DB_NAME;
    private $DB_PORT;
    private $CONN;

    function __construct($DB_TYPE, $DB_SERVER, $DB_PORT, $DB_USER, $DB_PW, $DB_NAME)
    {
        $this->DB_TYPE = $DB_TYPE;
        $this->DB_SERVER = $DB_SERVER;
        $this->DB_PORT = $DB_PORT;
        $this->DB_USER = $DB_USER;
        $this->DB_PW = $DB_PW;
        $this->DB_NAME = $DB_NAME;
        $this->CONN = null;
    }

    function connect()
    {
        try {
            $this->CONN = new PDO("{$this->DB_TYPE}:host={$this->DB_SERVER};dbname={$this->DB_NAME}", $this->DB_USER, $this->DB_PW,array(
        	PDO::ATTR_TIMEOUT => 200, // in seconds
    		));
            $this->CONN->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $log_message = "Connected successfully to database at: " . $this->DB_SERVER;
            //echo $log_message . "\n";
            //log_message($log_message);
            return $this->CONN;
        } catch (PDOException $e) {
            $error_message = "Connection Error: ". $this->DB_SERVER . ' ' . $e->getMessage();
            echo $error_message . "\n";
            log_error($error_message);
            //update_server_state_log($this->DB_SERVER);
            return '';
        }
    }

    function connect_pg()
    {
        try {
	    $this->CONN = new PDO("$this->DB_TYPE:host=$this->DB_SERVER;port=$this->DB_PORT;dbname=$this->DB_NAME;sslmode=require",$this->DB_USER,$this->DB_PW,array(
        	PDO::ATTR_TIMEOUT => 200, // in seconds
    		));
            //$this->CONN = new PDO("{$this->DB_TYPE}:host={$this->DB_SERVER};port={$this->DB_PORT};dbname={$this->DB_NAME};user={$this->DB_USER};password={$this->DB_PW}");
            $this->CONN->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $log_message = "Connected successfully to database at: " . $this->DB_SERVER;
            echo $log_message . "\n";
            //log_message($log_message);
            return $this->CONN;
        } catch (PDOException $e) {
            $error_message = "Connection Error: ". $this->DB_SERVER . ' ' . $e->getMessage();
            echo $error_message . "\n";
            log_error($error_message);
           // update_server_state_log($this->DB_SERVER);
            return '';
        }
    }

    function close_connection() {
        $this->CONN = null;
    }

}
