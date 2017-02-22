<?php
/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/13/2017
 * Time: 2:21 PM
 */

const REDIS_SERVER_IP = "127.0.0.1";
const REDIS_SERVER_PORT = "6379";
const REDIS_EXPIRE_TIME = 604800;

$myOwnIsStandard = true;


$standDsn = "mysql:host=127.0.0.1:3306;dbname=information_schema;charset=utf8";
$standUserName = "root";
$standPassword = "root";
$standCompareDB = "MyDatabaseName";
$standHost = '224';



$targetServers = array(
    '0'=>array(
        'targetDsn' => "mysql:host=127.0.0.1:3306;dbname=information_schema;charset=utf8",
        'targetUserName' => "root",
        'targetPassword' => "root",
        'targetDBs' => array(
            'MyDatabaseName',
            'MyDatabaseName_0',
            'MyDatabaseName_1',
            'MyDatabaseName_2',
            'MyDatabaseName_3',
            'MyDatabaseName_4',
            'MyDatabaseName_5',
            'MyDatabaseName_6',
            'MyDatabaseName_7',
            'MyDatabaseName_8',
            'MyDatabaseName_9'
        )
    ),
    '1'=>array(
        'targetDsn' => "mysql:host=127.0.0.1:3306;dbname=information_schema;charset=utf8",
        'targetUserName' => "root",
        'targetPassword' => "root",
        'targetDBs' => array(
            'MyDatabaseName',
            'MyDatabaseName_0',
            'MyDatabaseName_1',
            'MyDatabaseName_2',
            'MyDatabaseName_3',
            'MyDatabaseName_4',
            'MyDatabaseName_5',
            'MyDatabaseName_6',
            'MyDatabaseName_7',
            'MyDatabaseName_8',
            'MyDatabaseName_9'
        )
    )
);







