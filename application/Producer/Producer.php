<?php
namespace migration\Producer;

use migration\MessageQueue;
use migration\Task;
use \Exception;

/**
 * Class Producer
 *
 *
 * @package migration\Producer
 */
class Producer extends MessageQueue {

    public function __construct() {
        parent::__construct();
    }


    /**
     * @param $message
     * @return int|bool
     */
    public function send($message) {
        return $this->client->lPush(REDIS_MESSAGE_QUEUE_NAME, json_encode($message));
    }

    /**
     * @param $data
     * @return bool
     */
    public function checkParameterFormat($data) {
        if (isset($data->source, $data->destination, $data->subTaskIDS, $data->userID, $data->deleteOrigin)) {
            if (is_string($data->subTaskIDS)) {
                if (preg_match('/^\*$/', $data->subTaskIDS)) {
                    return true;
                } else {
                    return false;
                }
            } elseif (is_array($data->subTaskIDS)) {
                foreach ($data->subTaskIDS as $subTask) {
                    if (!empty($subTask)) {
                        continue;
                    } else {
                        return false;
                    }
                }
                return true;
            } else {
                //invalid format
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * return a message array,each message format is like this
     *
     * * {
     * "source":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix",
     * "destination":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix",
     * "subTaskIDS":"*",| ["yourTask_0","yourTask_1"],)
     * "userID":"61",
     * "deleteOrigin":false
     * }
     *
     * $message = array(
     * "sourceHostIP" => "127.0.0.1",
     * "sourceDatabaseName" => "MyDatabaseName",
     * "workData" => jsonecode("xxx"),
     * "sourceUser" => "root",
     * "sourcePort" => "3306",
     * "destinationHostIP" => "127.0.0.1",
     * "destinationDatabaseName" => "MyDatabaseName_1",
     * "destinationUser" => "root",
     * "destinationPort" => "3306",
     * "userID" => "61",
     * "userIDColumnName" => "visit_id",
     * "sourcePassword" => "root",
     * "destinationPassword" => "root",
     * "deleteSource" => true,
     * "taskID"=>10
     * );
     *
     * @param $taskID
     * @param $data
     * @param $sourceDBObj
     * @param $destinationDBObj
     * @return array
     */
    public function getTasks($taskID,$data,$sourceDBObj, $destinationDBObj) {
        /*
         *if source and destination are the same server and same database and table,then talk client invalid task,
         * if database and server is the same,but table is different,then only need to migrate spilt table ([0-9]).
         * if server is the same,database is different,then migrate all table,single task can use fast migrate.
         * others use slow migrate method if server is different,all table need be migrate,single group task can
         * use batch mode,others use slow migration
         */

        if (is_string($data->subTaskIDS)) {
            return $this->getAllTasks($taskID,$data,$sourceDBObj, $destinationDBObj);
        } else {
            return $this->getTasksByUserSpecifyWay($taskID,$data,$sourceDBObj, $destinationDBObj);
        }

    }


    /**
     * return message array
     *
     * @param string $userID
     * @param string $taskID
     * @param array $taskArray
     * @param array $source eg:array(
     * 'IP'=>"127.0.0.1",
     * 'DB'=>"MyDatabaseName",
     * 'UserName'=>"root",
     * 'Port'=>'3306',
     * 'password'=>'root',
     * 'TB'=>'0'
     * );
     * @param array $destination the same as $source
     * @param bool $delete
     * @param string $UserColumnName
     * @return array
     *
     */
    public function getMessages(string $userID, string $taskID, array $taskArray, array $source, array $destination, bool $delete, string $UserColumnName = DEFAULT_USER_ID_COLUMN) {
        $messageArray = array();
        foreach ($taskArray as $key => $task) {
            $task['subTaskID'] = $key;
            $taskMessage = $task;
            $message = array(
                "subTaskID" => $key,
                //database config data
                "sourceHostIP" => $source['IP'],
                "sourceDatabaseName" => $source['DB'],
                "sourceUser" => $source['UserName'],
                "sourcePort" => $source['Port'],
                "sourcePassword" => $source['Password'],
                "sourceTB" => $source['TB'],
                "destinationHostIP" => $destination['IP'],
                "destinationDatabaseName" => $destination['DB'],
                "destinationUser" => $destination['UserName'],
                "destinationPort" => $destination['Port'],
                "destinationPassword" => $destination['Password'],
                "destinationTB" => $destination['TB'],
                //work data
                "workData" => json_encode($taskMessage),
                "userID" => $userID,
                "userIDColumnName" => $UserColumnName,
                "deleteSource" => $delete,
                "taskID" => $taskID
            );
            $messageArray[] = $message;
        }
        return $messageArray;
    }

    /**
     *  return a sub task amount
     * @param $sourceObj
     * @param $destinationOjb
     * @param $data
     * @return bool|int
     * @throws Exception
     */
    public function countTaskAmount($sourceObj, $destinationOjb, $data) {
        $tasks = array();

        if (is_string($data->subTaskIDS) && strpos($data->subTaskIDS, '*') !== false) {
            global $normalTasks, $crossServerInProductionClustersTasks, $crossOnlyTableTasks, $crossServerOutProductionClusterTasks;
            if ($sourceObj->serverIP != $destinationOjb->serverIP && !in_array($destinationOjb->serverIP, PRODUCTION_CLUSTERS)) {
                //cross production clusters
                $tasks = array_merge($tasks, $crossServerInProductionClustersTasks, $crossServerOutProductionClusterTasks, $normalTasks);
            } elseif ($sourceObj->serverIP != $destinationOjb->serverIP && in_array($destinationOjb->serverIP, PRODUCTION_CLUSTERS)) {
                //not cross production clusters
                $tasks = array_merge($tasks, $normalTasks, $crossServerInProductionClustersTasks);
            } else {
                //not cross server,ip is the same
                if ($sourceObj->dataseName == $destinationOjb->dataseName && $sourceObj->tb == $destinationOjb->tb) {
                    //the same database and tb
                    throw new Exception("data has already in the same place");
                } elseif ($sourceObj->dataseName == $destinationOjb->dataseName && $sourceObj->tb != $destinationOjb->tb) {
                    //the same database ,different tb
                    $tasks = array_merge($tasks, $crossOnlyTableTasks);
                } else {
                    //not the same database,all normal task
                    $tasks = array_merge($tasks, $normalTasks);
                }
            }
        }

        if (empty($tasks)) {
            return false;
        } else {
            return count($tasks);
        }
    }


    /**
     * return a object contain info dbout connect database
     *
     * @param $data eg ("root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName")
     * @return bool|\stdClass
     *
     */
    public static function solveDBParameter($data) {
        if (!preg_match('/^\w+@[0-9\.]+:[0-9]+;password=.*;dbname=.+;tb=.+/', $data)) {
            return false;
        }
        $result = new \stdClass();

        $accountPosition = strpos($data, '@');
        $ipPosition = strpos($data, ":");
        $passwordPosition = strpos($data, ";password=");
        $databasePosition = strpos($data, ";dbname=");
        $tbPosition = strpos($data, ";tb=");

        $result->userName = substr($data, 0, $accountPosition);
        $result->serverIP = substr($data, ($accountPosition + 1), ($ipPosition - $accountPosition - 1));
        $result->serverPort = substr($data, ($ipPosition + 1), ($passwordPosition - $ipPosition - 1));
        $result->password = substr($data, ($passwordPosition + 10), ($databasePosition - $passwordPosition - 10));
        $result->dataseName = substr($data, ($databasePosition + 8), ($tbPosition - $databasePosition - 8));
        $result->tb = substr($data, ($tbPosition + 4));
        return $result;
    }


    /**
     * @param $sourceDBObj
     * @param $destinationDBObj
     * @param $data
     * @param string $taskID
     * @return array
     */
    private function getAllTasks($taskID='',$data,$sourceDBObj, $destinationDBObj) {
        $messageArray = array();
        //default task config in the config.php file
        global $normalTasks, $crossServerInProductionClustersTasks, $crossOnlyTableTasks, $crossServerOutProductionClusterTasks;
        //cross server need transfer
        $taskMessagesSource = self::arrangeDataForGetMessage((string)$sourceDBObj->serverIP, (string)DEFAULT_SOURCE_DATABASE_PORT, (string)DEFAULT_SOURCE_DATABASE_NAME, (string)$sourceDBObj->tb, (string)$sourceDBObj->userName, (string)$sourceDBObj->password);
        $taskMessagesDestination = self::arrangeDataForGetMessage((string)$destinationDBObj->serverIP, (string)DEFAULT_DESTINATION_DATABASE_PORT, (string)DEFAULT_DESTINATION_DATABASE_NAME, (string)$destinationDBObj->tb, (string)$destinationDBObj->userName, (string)$destinationDBObj->password);
        //normal task transfer
        $taskMessageNormalSource = self::arrangeDataForGetMessage((string)$sourceDBObj->serverIP, (string)$sourceDBObj->serverPort, (string)$sourceDBObj->dataseName, (string)$sourceDBObj->tb, (string)$sourceDBObj->userName, (string)$sourceDBObj->password);
        $taskMessageNormalDestination = self::arrangeDataForGetMessage((string)$destinationDBObj->serverIP, (string)$destinationDBObj->serverPort, (string)$destinationDBObj->dataseName, (string)$destinationDBObj->tb, (string)$destinationDBObj->userName, (string)$destinationDBObj->password);
        if ($sourceDBObj->serverIP != $destinationDBObj->serverIP && !in_array($destinationDBObj->serverIP, PRODUCTION_CLUSTERS)) {
            //cross server in production clusters
            $mergedTasks = array_merge($crossServerInProductionClustersTasks, $crossServerOutProductionClusterTasks);
            $taskMessages1 = $this->getMessages((string)$data->userID, (string)$taskID, $mergedTasks, $taskMessagesSource, $taskMessagesDestination, $data->deleteOrigin);

            //normal task
            $taskMessages2 = $this->getMessages((string)$data->userID, (string)$taskID, $normalTasks, $taskMessageNormalSource, $taskMessageNormalDestination, $data->deleteOrigin);
            $messageArray = array_merge($messageArray, $taskMessages1, $taskMessages2);
        } elseif ($sourceDBObj->serverIP != $destinationDBObj->serverIP && in_array($destinationDBObj->serverIP, PRODUCTION_CLUSTERS)) {
            //not cross production clusters
            $taskMessages1 = $this->getMessages((string)$data->userID, (string)$taskID, $crossServerInProductionClustersTasks, $taskMessagesSource, $taskMessagesDestination, $data->deleteOrigin);
            $taskMessages2 = $this->getMessages((string)$data->userID, (string)$taskID, $normalTasks, $taskMessageNormalSource, $taskMessageNormalDestination, $data->deleteOrigin);
            $messageArray = array_merge($messageArray, $taskMessages1, $taskMessages2);
        } else {
            //not cross server,ip is the same
            if ($sourceDBObj->dataseName == $destinationDBObj->dataseName && $sourceDBObj->tb == $destinationDBObj->tb) {
                //the same database and tb
                $messageArray = array();
            } elseif ($sourceDBObj->dataseName == $destinationDBObj->dataseName && $sourceDBObj->tb != $destinationDBObj->tb) {
                //the same database ,different tb
                $taskMessages1 = $this->getMessages((string)$data->userID, (string)$taskID, $crossOnlyTableTasks, $taskMessageNormalSource, $taskMessageNormalDestination, $data->deleteOrigin);
                $messageArray = array_merge($messageArray, $taskMessages1);
            } else {
                //not the same database,all normal task
                $taskMessages1 = $this->getMessages((string)$data->userID, (string)$taskID, $normalTasks, $taskMessageNormalSource, $taskMessageNormalDestination, $data->deleteOrigin);
                $messageArray = array_merge($messageArray, $taskMessages1);
            }
        }
        return $messageArray;
    }

    /**
     * arrange data for getMessage() to use
     *
     * array(
     * 'IP'=>"127.0.0.1",
     * 'DB'=>"MyDatabaseName",
     * 'UserName'=>"root",
     * 'Port'=>'3306',
     * 'password'=>'root',
     * 'TB'=>'0'
     * );
     * @param $IP
     * @param $port
     * @param $DB
     * @param $TB
     * @param $userName
     * @param $password
     * @return array
     */
    public static function arrangeDataForGetMessage($IP, $port, $DB, $TB, $userName, $password) {
        $tmp = array(
            'IP' => $IP,
            'Port' => $port,
            'DB' => $DB,
            'TB' => $TB,
            'UserName' => $userName,
            'Password' => $password
        );
        return $tmp;
    }


    /**
     * check whether or not this task existed and times is greater than 2,if greater,don't do this sub task
     *
     * array("yourTask_0","yourTask_1")
     * "taskID":"1",
     *
     * @param $taskID
     * @param $data
     * @param $sourceDBObj
     * @param $destinationDBObj
     * @return array
     */
    private function getTasksByUserSpecifyWay($taskID,$data,$sourceDBObj, $destinationDBObj) {
        $subTaskIDs = $data->subTaskIDS;
        if (empty($subTaskIDs)) {
            return array();
        }
        //because each task have the same id.so
        global $normalTasks, $crossServerInProductionClustersTasks, $crossServerOutProductionClusterTasks, $crossOnlyTableTasks;
        $UID = $data->userID;

        $PDatabase = new PDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD);
//        $allTasks = array_merge($normalTasks,$crossServerInProductionClustersTasks,$crossServerOutProductionClusterTasks,$crossOnlyTableTasks);

        $messages = $tmpNormal = $tmpCS = $tmpCOTT = array();
        foreach ($subTaskIDs as $subTask) {
            if ($this->isExcessExecuteTimesLimit($UID, $taskID, $subTask, $PDatabase)) {
                //excess execute times,ship this sub-task
                continue;
            }
            $key = $subTask;
            if (array_key_exists($key, $normalTasks)) {
                $tmpNormal[$key] = $normalTasks[$key];
            } elseif (array_key_exists($key, $crossServerInProductionClustersTasks)) {
                $tmpCS[$key] = $crossServerInProductionClustersTasks[$key];
            } elseif (array_key_exists($key, $crossServerOutProductionClusterTasks)) {
                $tmpCS[$key] = $crossServerOutProductionClusterTasks[$key];
            } elseif (array_key_exists($key, $crossOnlyTableTasks)) {
                $tmpCOTT[$key] = $crossOnlyTableTasks[$key];
            } else {
                //invalid sub-taskID,skip this task
                continue;
            }
        }

        $taskMessagesSource = self::arrangeDataForGetMessage((string)$sourceDBObj->serverIP, (string)DEFAULT_SOURCE_DATABASE_PORT, (string)DEFAULT_SOURCE_DATABASE_NAME, (string)$sourceDBObj->tb, (string)$sourceDBObj->userName, (string)$sourceDBObj->password);
        $taskMessagesDestination = self::arrangeDataForGetMessage((string)$destinationDBObj->serverIP, (string)DEFAULT_DESTINATION_DATABASE_PORT, (string)DEFAULT_DESTINATION_DATABASE_NAME, (string)$destinationDBObj->tb, (string)$destinationDBObj->userName, (string)$destinationDBObj->password);
        //normal task transfer
        $taskMessageNormalSource = self::arrangeDataForGetMessage((string)$sourceDBObj->serverIP, (string)$sourceDBObj->serverPort, (string)$sourceDBObj->dataseName, (string)$sourceDBObj->tb, (string)$sourceDBObj->userName, (string)$sourceDBObj->password);
        $taskMessageNormalDestination = self::arrangeDataForGetMessage((string)$destinationDBObj->serverIP, (string)$destinationDBObj->serverPort, (string)$destinationDBObj->dataseName, (string)$destinationDBObj->tb, (string)$destinationDBObj->userName, (string)$destinationDBObj->password);

        if (!empty($tmpNormal)) {
            $messages = array_merge($messages, $this->getMessages($UID,$taskID,$tmpNormal,$taskMessageNormalSource,$taskMessageNormalDestination,$data->deleteOrigin));
        }
        if (!empty($tmpCS)) {
            $messages = array_merge($messages, $this->getMessages($UID,$taskID,$tmpCS,$taskMessagesSource,$taskMessagesDestination,$data->deleteOrigin));
        }
        if (!empty($tmpCOTT)) {
            $messages = array_merge($messages, $this->getMessages($UID,$taskID,$tmpCOTT,$taskMessageNormalSource,$taskMessageNormalDestination,$data->deleteOrigin));
        }

        return $messages;
    }

    /**
     * return true,if this task is excess execute limit,else return false
     *
     * @param $UID
     * @param $taskID
     * @param $subTaskID
     * @param PDatabase $PDatabase
     * @param string $tableName
     * @param int $executeLimit
     * @return bool
     */
    private function isExcessExecuteTimesLimit($UID, $taskID, $subTaskID, PDatabase $PDatabase, $tableName = TASK_TABLE_NAME_SUB, $executeLimit = FAILED_REPEAT_EXECUTE_TIMES) {
        $sql = "select * from `{$tableName}` where `UID`='{$UID}' and `taskID`='{$taskID}' and `subTaskID`='{$subTaskID}';";
        $result = $PDatabase->getSingleRecord($sql);
        if ($result === false) {
            return false;
        }
        if ($result['times'] >= $executeLimit) {
            return true;
        }
    }


}