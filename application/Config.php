<?php
/**
 * This file is configuration file for all this application,
 * unless you know every item clearly when you have to change it
 *
 * @author williamchu--<154893323@qq.com>
 * @since 2016-12-12 11:14:17
 *
 * 巭孬嫑乱动
 */

/************************************************************************************
 * debug switcher,production environment must be closed,else many log will be wrote
 * in work.log
 * **********************************************************************************/
const DEBUG = true;


/************************************************************************************
 * the name of redis message queue
 * redis server ip address
 * redis server port
 * default redis cache expire time(a week 3600*24*7 seconds), main for cache table
 * structure in redis
 * **********************************************************************************/
const REDIS_MESSAGE_QUEUE_NAME = "MIGRATION_QUEUE_0";
const REDIS_SERVER_IP = "127.0.0.1";
const REDIS_SERVER_PORT = "6379";
const REDIS_EXPIRE_TIME = 604800;


/************************************************************************************
 * the default mysql server port if you don't specify exactly
 * **********************************************************************************/

const DEFAULT_SOURCE_DATABASE_NAME = "MyDatabaseName";
const DEFAULT_DESTINATION_DATABASE_NAME = "MyDatabaseName";

const DEFAULT_MYSQL_PORT = 3306;
const DEFAULT_SOURCE_DATABASE_PORT = '3306';
const DEFAULT_DESTINATION_DATABASE_PORT = '3306';

const DEFAULT_USER_ID_COLUMN = 'userID';

const DEFAULT_TIME_TO_LIVE = "30";

/************************************************************************************
 * this application task schedule database connection information
 * **********************************************************************************/
const TASK_DSN = "mysql:host=127.0.0.1:3306;dbname=MyDatabaseName;charset=utf8";
const TASK_PASSWORD = 'yourPassword';
const TASK_USERNAME = 'yourUsername';
const TASK_TABLE_NAME_MAIN = "dataTransferTask";
const TASK_TABLE_NAME_SUB = "dataTransferSubTask";


/************************************************************************************
 * the database configure information for get table structure (column names in array)
 * **********************************************************************************/
const INFO_SCHEMA_DSN = "mysql:host=127.0.0.1:3306;dbname=information_schema;charset=utf8";
const INFO_SCHEMA_USERNAME = "root";
const INFO_SCHEMA_PASSWORD = "root";


/************************************************************************************
 * the migration step size (fast and slow mode can use different size)
 * **********************************************************************************/
const MIGRATION_EACH_TIME_FAST = 1000;
const MIGRATION_EACH_TIME_SLOW = 1000;


/************************************************************************************
 * the migration step size (fast and slow mode can use different size)
 * **********************************************************************************/
const PRODUCTION_CLUSTERS = array("127.0.0.1");


/************************************************************************************
 * consumer runner will fetch table structure from source database if true
 * else will fetch table structure from destination database,this maybe
 *
 * **********************************************************************************/
const FETCH_TABLE_STRUCTURE_FROM_SOURCE = true;


/************************************************************************************
 * if first execute sql failed.then will execute repeat depend on this value
 * **********************************************************************************/
const FAILED_REPEAT_EXECUTE_TIMES = 2;



/************************************************************************************
 * main task status
 * **********************************************************************************/
const TASK_STATUS_INITIALISED = 1;
const TASK_STATUS_SUCCESS = 2;//insert sql is executed,failed,then rollback,will be re-execute.
const TASK_STATUS_FAILED = 3;//insert sql is not executed.some problem make this failed,don't need rollback,do not re-execute



/************************************************************************************
 * sub task status
 * **********************************************************************************/
const SUB_TASK_STATUS_SUCCESS = 1;
const SUB_TASK_STATUS_FAILED = 2;
const SUB_TASK_STATUS_EXCEPTION = 3;
const SUB_TASK_STATUS_CLEANED = 4;


/************************************************************************************
 * delete or not origin records in the database
 * **********************************************************************************/
const DO_DELETE = 1;
const DO_NOT_DELETE = 0;





/**
 *{
 * code:"",
 * message:"",
 * data:{
 * "MTID":"5"
 * }
 *
 * }
 */


/************************************************************************************
 * three main task status
 * **********************************************************************************/
const ERROR_CODE_DEFAULT = 1;
const ERROR_CODE_PARAMS = 2;













