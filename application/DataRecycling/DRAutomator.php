<?php
namespace migration\DataRecycling;

use \Exception;
use \PDO;
use migration\Producer\Producer;
use migration\Consumer\CDatabase;
use migration\Log;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/16/2017
 * Time: 6:59 PM
 */
class DRAutomator {

    public $targetDBs = array();


    /**
     * get can delete record from db,parse content to get tables ,then delete all records with that user id
     */
    public function clean() {
        $mainTableName = TASK_TABLE_NAME_MAIN;
        $subTableName = TASK_TABLE_NAME_SUB;
        $deadLine = date('Y-m-d H:i:s', (time() - DEFAULT_TIME_TO_LIVE * 86400));
        $status = TASK_STATUS_SUCCESS;
        $delete = DO_DELETE;

        $taskDBC = new DRDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD, array(PDO::ATTR_PERSISTENT => 1));
        $results = $taskDBC->getNeedCleanRecord($mainTableName, $subTableName, $status, $delete, $deadLine);
        if ($results === false || empty($results)) {
            exit;
        }

        try {
            foreach ($results as $value) {
                if (empty($value['taskContent'])) {
                    continue;
                }
                $DBC = null;
                //check whether or not have the same database connection,if true,use previous DBC,else create new DBC
                $tmpConfig = json_decode($value['taskContent'], true);
                $tmpHashValue = $this->getDBConfigHashValue($tmpConfig['source']);
                if (array_key_exists($tmpHashValue, $this->targetDBs)) {
                    $DBC = $this->targetDBs[$tmpHashValue];
                } else {
                    $config = Producer::solveDBParameter($tmpConfig['source']);
                    $dsn = CDatabase::getDSN($config->serverIP, $config->serverPort, $config->databaseName);
                    $DBC = new DRDatabase($dsn, $config->userName, $config->password, array(PDO::ATTR_PERSISTENT => 1));
                    $this->targetDBs[$tmpHashValue] = $DBC;
                }
                if (!empty($value['tables'])) {
                    //do delete record work
                    $tables = json_decode($value['tables']);
                    foreach ($tables as $table) {
                        if ($DBC->deleteUselessRecords($value['UID'], $table)) {
                            throw new Exception("delete table error: {$table} in " . print_r($value, true));
                        }
                    }
                }
                $taskDBC->updateSubTaskStatus($subTableName, $value['id']);
            }
        } catch (Exception $exception) {
            Log::logMessage($exception->getMessage() . "\n");
        }

    }

    /**
     * get DB Config Hash Value
     * @param $dsn
     * @return string
     */
    private function getDBConfigHashValue($dsn) {
        if (preg_match('/.*;tb=[0-9]{1}$/', $dsn)) {
            //remove tb strings
            $position = strpos($dsn, ';tb=');
            $dsn = substr($dsn, 0, $position);
        }
        return md5($dsn);
    }


}


