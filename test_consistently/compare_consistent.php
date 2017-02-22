<?php
use migration\TDatabase;
use migration\TRedis;

require __DIR__ . DIRECTORY_SEPARATOR . "../application/Autoload.php";
require_once __DIR__ . DIRECTORY_SEPARATOR . "config.php";
require_once __DIR__ . DIRECTORY_SEPARATOR . ".." . DIRECTORY_SEPARATOR . "test" . DIRECTORY_SEPARATOR . "TDatabase.php";
require_once __DIR__ . DIRECTORY_SEPARATOR . ".." . DIRECTORY_SEPARATOR . "test" . DIRECTORY_SEPARATOR . "TRedis.php";

/**
 * SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS where TABLE_SCHEMA ='MyDatabaseName_1' and TABLE_NAME='dadan_log_0' order by COLUMN_NAME asc;
 *
 * get a table's column_name and column_type from stand db,store in redis.get another table's column_name and column_type from target database,
 * each database must have the same number tables
 *
 * first layer
 *
 *
 */


//stand TDatabase
$STDB = new TDatabase($standDsn, $standUserName, $standPassword);
//target TDatabase
$TRedis = new TRedis();


$standTablesNum = $STDB->getTablesNumber($standCompareDB);
if (empty($standTablesNum)) {
    echo "[FATAL ERROR] can't figure out how many tables in standard database {$standCompareDB} \r\n";
    exit;
}

foreach ($targetServers as $targetServer) {
    $TTDB = new TDatabase($targetServer['targetDsn'], $targetServer['targetUserName'], $targetServer['targetPassword']);
    //compare one database to another database
    foreach ($targetServer['targetDBs'] as $database) {
        //compare each database whether or not have the same table num as stand database
        $totalTablesNum = $TTDB->getTablesNumber($database);
        if ($totalTablesNum === false) {
            echo "[WARNING] can't figure out how many tables in {$database} \r\n";
            continue;
        }
        if ($standTablesNum != $totalTablesNum) {
            echo "[WARNING] stand database is {$standTablesNum} ,but target database  {$database}  is {$totalTablesNum} {$targetServer['targetDsn']} \r\n";
        }

        //get all tables
        $tables = $TTDB->getAllTablesFromDB($database);
        if (empty($tables)) {
            echo "[WARNING] database {$database} is not find tables \r\n";
            continue;
        }
        //compare a table's name and type whether or not is equal stand database
        foreach ($tables as $table) {

            if(empty($table['table'])){
                echo "[FAILED] invalid table name in database {$database} \r\n";
                continue;
            }else{
                $tableName = $table['table'];
            }
            $targets = $TTDB->getColumnNameAndType($database, $tableName);
            if (empty($targets)) {
                echo "[FAILED] can't fetch column name and column type from {$database}.{$tableName} \r\n";
                continue;
            }
            $result = compareColumnNameAndType($tableName, $targets);
            if ($result) {
                //echo "[success] target {$database}.{$tableName} is equal standard database {$standCompareDB}.{$tableName}\r\n";
                continue;
            } else {
                echo "[failed] target {$database}.{$tableName} is not equal standard database {$standCompareDB}.{$tableName} in {$targetServer['targetDsn']}\r\n";
            }
        }

    }


}

/**
 * @param $tableName
 * @param $targetTables
 * @return bool
 */
function compareColumnNameAndType($tableName, $targetTables) {
    global $myOwnIsStandard;
    $targetHash = getHashValue($targetTables);
    $standHash = getStandHashValueFromRedis($tableName);
    if($myOwnIsStandard && $standHash === true){
        return true;
    }
    if (!empty($targetHash) && !empty($standHash) && $standHash == $targetHash) {
        return true;
    } else {
        return false;
    }
}

function getStandHashValueFromRedis($tableName) {
    global $STDB, $TRedis, $standCompareDB,$myOwnIsStandard;

    $key = getKey($tableName);
    $value = $TRedis->getValue($key);
    if (empty($value)) {
        //get from database ,then store in redis
        $standTables = $STDB->getColumnNameAndType($standCompareDB, $tableName);
        if (empty($standTables)) {
            if($myOwnIsStandard){
                return true;
            }else{
                echo "[FATAL ERROR] can't get stand table {$standCompareDB}.{$tableName} \r\n";
                return false;
            }
        }
        $standHash = getHashValue($standTables);
        if ($standHash === false) {
            echo "[FATAL ERROR] can't get stand table {$standCompareDB}.{$tableName} hash value\r\n";
            return false;
        }
        $value = $standHash;
        $TRedis->setValue($key, $value);
    }
    return $value;
}

function getKey($TBN) {
    global $standCompareDB,$standHost;
    return "consistent_test_{$standHost}_{$standCompareDB}_" . $TBN;
}


function getHashValue(array $tableArray) {
    $tmps = array();
    foreach ($tableArray as $columnArray) {
        $tmps[$columnArray['name']] = $columnArray['type'];
    }
    ksort($tmps, SORT_STRING);
    $name = $type = '';
    foreach ($tmps as $key => $value) {
        $name .= $key;
        $type .= $value;
    }
    $hashString = $name . $type;
    if (empty($hashString)) {
        return false;
    }
    return md5($hashString);
}