<?php
  $host    = "host=ol7985ua81.b49qsab0at.tsdb.cloud.timescale.com";
  $port    = "port=31277";
  $dbname   = "dbname=tsdb";
  $credentials = "user=tsdbadmin password=rpltd7b7w9gnka4s sslmode=require";
  try
  {
  	//$db = pg_connect("$host $port $dbname $credentials") or die('connection failed');
  	$db = pg_connect("host=ol7985ua81.b49qsab0at.tsdb.cloud.timescale.com  port=31277  dbname=tsdb  user=tsdbadmin password=rpltd7b7w9gnka4s sslmode=require") or die('connection failed');
  	if(!$db){
  	 echo "Error : Unable to open database\n".pg_last_error();
  	} else {
   	echo "Opened database successfully\n";
  	}
  }
  catch (Exception $e) 
  {
	echo "exxxxxxx:$e";
  }
?>
