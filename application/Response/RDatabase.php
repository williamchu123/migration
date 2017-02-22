<?php
namespace migration\Response;

use migration\Database;
use migration\Consumer\BaseWorker;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/17/2017
 * Time: 3:04 PM
 */
class RDatabase extends Database {
    /**
     * RDatabase constructor.
     * @param $dsn
     * @param $username
     * @param $password
     * @param array $option
     */
    public function __construct($dsn, $username, $password, $option = array()) {
        parent::__construct($dsn, $username, $password, $option);
    }

    /**
     * get Sub Tasks
     * @param $UID
     * @param $ID
     * @return array|bool
     */
    public function getSubTasks($UID, $ID) {
        $sql = "select `subTaskID`,`status`,`errorMessage`,`times` from " . TASK_TABLE_NAME_SUB . " where UID='{$UID}' and taskID='{$ID}';";
        return $this->getAllRecord($sql);
    }

    /**
     * get task amount
     * @param string $UID
     * @param string $ID
     * @return mixed
     */
    public function getTaskNum($UID, $ID) {
        $sql = "select * from `" . TASK_TABLE_NAME_MAIN . "` where `UID`='{$UID}' and `taskID`='{$ID}';";
        $result = $this->getSingleRecord($sql);
        if (!empty($result['taskAmount'])) {
            return $result['taskAmount'];
        } else {
            return false;
        }
    }

    /**
     * update main task status
     * @param $UID
     * @param $taskID
     * @param $status
     * @return bool|\PDOStatement
     */
    public function updateMainTaskStatus($UID,$taskID,$status){
        $now = BaseWorker::getDate();
        $data = array(
            "status" => $status,
            "updated_at" => $now
        );
        $where = array(
            "`UID`='{$UID}'",
            "and `taskID`='{$taskID}'"
        );

        return $this->update(TASK_TABLE_NAME_MAIN,$data,$where);
    }


}