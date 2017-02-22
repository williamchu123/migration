<?php

/**
 * todo it have very dangerous bug,anyone who know how use this API that can move others data to anywhere
 *
 *
 *
 *
 */

namespace migration;

use \Exception;
use migration\Response\Response;

$applicationPath = __DIR__ . DIRECTORY_SEPARATOR . "application" . DIRECTORY_SEPARATOR;
require_once $applicationPath . "Config.php";
require_once $applicationPath . "TaskConfig.php";
require_once $applicationPath . "Autoload.php";
require_once $applicationPath . "Common.php";


$ID = isset($_GET['taskID']) ? $_GET['taskID'] : "";
$UID = isset($_GET['UID']) ? $_GET['UID'] : "";
if (empty($ID)) {
    exit(stdResponse(ERROR_CODE_PARAMS, "critical parameter is lost"));
}
try {
    $response = new Response();
    $taskAmount = $response->getTotalTasks($UID, $ID);
    if ($taskAmount === false || $taskAmount == 0) {
        //invalid task
        exit(stdResponse(ERROR_CODE_DEFAULT, 'not existed task'));
    }
    $data = array();
    $tmp = $response->getStatus($UID, $ID);
    if (!empty($tmp)) {
        //if success sub-task is equal total task number,update status = 2 in 'dataTransferTask' table
        $collect = array();
        foreach ($tmp as $t){
            if($t['status'] == SUB_TASK_STATUS_SUCCESS){
                $collect[] = $t;
            }
        }
        $tmpNum = count($collect);
        if($taskAmount == $tmpNum){
            //update main task status
            $response->updateStatus($UID,$ID);

        }

        $data = $tmp;
    }

} catch (Exception $exception) {
    exit(stdResponse(ERROR_CODE_DEFAULT, "[EXCEPTION] {$exception->getMessage()}"));
}

$final = array(
    "total" => $taskAmount,
    "subTaskStatus" => $data
);

exit(stdResponse('', '', $final));







