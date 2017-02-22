<?php
namespace migration;
use migration\Producer\Producer;
use \Exception;


/**
 *
 * data format is like this
 *
 * {
 * "source":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix",
 * "destination":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix",
 * "subTaskIDS":"*",| ["yourTask_0","yourTask_1"]
 * "userID":"61",
 * "taskID":"1",
 * "deleteOrigin":false
 * }
 *
 *  if taskID is empty,this task must to be new create,else all or specified task will be check and re-execute
 *
 * @author williamchu--<154893323@qq.com>
 * @since 2016/12/14 15:13:15
 *
 */
$applicationPath = __DIR__ . DIRECTORY_SEPARATOR . "application" . DIRECTORY_SEPARATOR;

require $applicationPath . "Config.php";
require $applicationPath . "TaskConfig.php";
require $applicationPath . "Autoload.php";
require $applicationPath . "Common.php";

$data = isset($_POST['data']) ? $_POST['data'] : "";
if (empty($data)) {
    exit(stdResponse(ERROR_CODE_PARAMS, "critical parameter is lost"));
}

$dataObj = json_decode($data);

if ($dataObj != true) {
    exit(stdResponse(ERROR_CODE_PARAMS,"error parameter format",""));
}


try {
    $producer = new Producer();
    if (!$producer->checkParameterFormat($dataObj)) {
        exit(stdResponse(ERROR_CODE_PARAMS, "critical parameter part is lost", ''));
    }
    $sourceDBObj = $producer->solveDBParameter($dataObj->source);
    $destinationDBObj = $producer->solveDBParameter($dataObj->destination);
    if (!$sourceDBObj || !$destinationDBObj) {
        throw new Exception("lost critical parameters");
    }

    if (!empty($dataObj->taskID)) {
        $taskID = $dataObj->taskID;
    } else {
        //initialize a new task id
        $taskNumber = $producer->countTaskAmount($sourceDBObj, $destinationDBObj, $dataObj);
        //$taskNumber = count($taskArray);
        if ($taskNumber === false) {
            throw new Exception("task is not necessary");
        }
        $task = new Task(TASK_DSN, TASK_USERNAME, TASK_PASSWORD);
        $taskID = $task->getNewTaskID(json_encode($dataObj), $taskNumber, $dataObj->userID);
        if ($taskID === false) {
            throw new Exception("create task failed!");
        }
    }

    $messageArray = $producer->getTasks($taskID, $dataObj, $sourceDBObj, $destinationDBObj);
    if (empty($messageArray)) {
        throw new Exception("data has already in the same place");
    }
    $tmp = array();
    //dispatch task
    foreach ($messageArray as $message) {
        if (isset($message['subTaskID'])) {
            $tmp[] = $message['subTaskID'];
            unset($message['subTaskID']);
        }
        $producer->send($message);
    }
    // sub task will processed will be returned
    $data = array(
        'taskID' => $taskID,
        'subTaskIDs' => $tmp
    );
    exit(stdResponse("", "", $data));
} catch (Exception $exception) {
    $errorCode = empty($exception->getCode()) ? ERROR_CODE_DEFAULT : $exception->getCode();
    $errorMessage = empty($exception->getMessage()) ? "sorry,throw point not give answer" : $exception->getMessage();
    exit(stdResponse($errorCode, $errorMessage, ''));
}







