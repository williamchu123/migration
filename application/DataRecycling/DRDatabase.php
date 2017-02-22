<?php
namespace migration\DataRecycling;

use migration\Database;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/17/2017
 * Time: 10:29 AM
 */

class DRDatabase extends Database {

    public function __construct($dsn, $username, $password, $option = array()) {
        parent::__construct($dsn, $username, $password, $option);
    }

    /**
     * get all expired (default 30 day) record that is successful migrated
     *
     * @param $mainTableName
     * @param $subTableName
     * @param $status
     * @param $delete
     * @param $deadLine
     * @return array|bool
     */
    public function getNeedCleanRecord($mainTableName, $subTableName, $status, $delete, $deadLine) {
        $sql = "select `a`.`taskContent`,`b`.* from '{$mainTableName}' as `a` INNER JOIN {$subTableName} as `b` on (`a`.`taskID` = `b`.`taskID`)where `a`.`created_at` < '{$deadLine}' and `a`.`status`='{$status}' and `b`.`delete` = {$delete}; ";
        return $this->getAllRecord($sql);
    }

    /**
     * update sub task status
     *
     * @param $table
     * @param $id
     * @param int $status
     * @return bool|\PDOStatement
     */
    public function updateSubTaskStatus($table, $id, $status = SUB_TASK_STATUS_CLEANED) {
        $now = date("Y-m-d H:i:s");
        $data = array(
            "status" => $status,
            "updated_at " => $now
        );
        $where = "id={$id}";
        return $this->update($table, $data, $where);
    }

    /**
     * delete all record in a table where uid specified
     *
     * @param $UID
     * @param $table
     * @param string $columnName
     * @return \PDOStatement
     */
    public function deleteUselessRecords($UID,$table,$columnName=DEFAULT_USER_ID_COLUMN){
        $sql = "delete IGNORE from `{$table}` where `$columnName` = '{$UID}'";
        return $this->DBC->query($sql);
    }




}