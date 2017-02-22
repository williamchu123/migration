<?php
namespace migration;
use migration\Consumer\BaseWorker;
use \PDO;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/19/2016
 * Time: 3:49 PM
 */
class Task {
    private $DBC = null;
    public function __construct($dsn, $username, $password) {
        $this->DBC = new PDO($dsn, $username, $password);
    }

    /**
     * @param $taskContent
     * @param $taskAmount
     * @param $UID
     * @return bool
     */
    public function getNewTaskID($taskContent, $taskAmount,$UID) {
        $date =BaseWorker::getDate();
        $uid = uniqid();
        $sql = "insert `". TASK_TABLE_NAME_MAIN ."` (`taskID`,`UID`,`taskContent`,`taskAmount`,`status`,`created_at`,`updated_at`) value('{$uid}','{$UID}','{$taskContent}','{$taskAmount}','" . TASK_STATUS_INITIALISED . "','{$date}','{$date}');";
        $result = $this->DBC->query($sql);

        if ($result !== false) {
            return $uid;
        }
        return false;
    }


}
