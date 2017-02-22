<?php
namespace migration\Response;

use migration\Response\RDatabase;
use \Exception;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/17/2017
 * Time: 2:59 PM
 */
class Response {


    /**
     * getTotalTasks
     * @param $UID
     * @param $ID
     * @return mixed
     */
    public function getTotalTasks($UID, $ID) {
        $RDB = new RDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD);
        return $RDB->getTaskNum($UID, $ID);
    }

    /**
     * @param $UID
     * @param $ID
     * @return bool
     */
    public function getStatus($UID, $ID) {
        $db = new RDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD);
        $results = $db->getSubTasks($UID, $ID);
        if (empty($results)) {
            return false;
        } else {
            return $results;
        }
    }

    /**
     * update main task status
     * @param $UID
     * @param $taskID
     * @param int $status
     * @return bool|\PDOStatement
     */
    public function updateStatus($UID,$taskID,$status=TASK_STATUS_SUCCESS){
        $db = new RDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD);
        return $db->updateMainTaskStatus($UID,$taskID,$status);
    }


}