<?php
namespace migration;
use \Exception;
use \PDO;
/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/28/2016
 * Time: 9:52 AM
 */
class TDatabase extends Database{


    public function __construct($dsn, $username, $password) {
        parent::__construct($dsn, $username, $password);
    }


    /**
     * get all table name from a database
     * @param $dbName
     * @return array|bool
     * @throws Exception
     */
    public function getAllTablesFromDB($dbName){
        $sql = "select TABLE_NAME as `table` from information_schema.TABLES where TABLE_SCHEMA ='{$dbName}';";
        $query = $this->DBC->query($sql);
        if($query === false){
            return false;
            //throw new Exception("query error");
        }
        $results = $query->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    /**
     *
     * select count(TABLE_NAME) from information_schema.TABLES where TABLE_SCHEMA='MyDatabaseName_0';
     * @param $dbName
     * @return array|bool
     *
     */
    public function getTablesNumber($dbName){
        $sql = "select count(TABLE_NAME) as `num` from information_schema.TABLES where TABLE_SCHEMA ='{$dbName}';";
        $query = $this->DBC->query($sql);
        if($query === false){
            return false;
            //throw new Exception("query error");
        }
        $result = $query->fetch(PDO::FETCH_ASSOC);
        if(isset($result['num']) && $result['num'] !== 0){
            return $result['num'];
        }else{
            return false;
        }
    }

    /**
     *
     * SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS where TABLE_SCHEMA ='MyDatabaseName_1' and TABLE_NAME='dadan_log_0' order by COLUMN_NAME asc;
     *
     * @param $dbName
     * @param $tbName
     * @return array|bool
     */
    public function getColumnNameAndType($dbName,$tbName){
        $sql = "SELECT COLUMN_NAME as `name`,COLUMN_TYPE as `type` FROM information_schema.COLUMNS where TABLE_SCHEMA ='{$dbName}' and TABLE_NAME='{$tbName}' order by COLUMN_NAME asc;";
        $query = $this->DBC->query($sql);
        if($query === false){
            return false;
            //throw new Exception("query error");
        }
        $results = $query->fetchAll(PDO::FETCH_ASSOC);
        if(empty($results)){
            return false;
        }else{
            return $results;
        }
    }








}