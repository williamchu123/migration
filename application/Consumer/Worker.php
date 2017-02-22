<?php
namespace migration\Consumer;

use migration\Database;
use migration\Log;
use \Exception;
use \stdClass;
use \PDO;

/**
 * Class Worker
 *
 * @package migration\Consumer
 * @author williamchu--<154893323@qq.com>
 * @since 12/12/2016 1:15:46
 */
class Worker extends BaseWorker {
    public $SHIP = null;//source host ip
    public $SUN = null;//source user name
    public $SDN = null;//source database name
    public $STB = null;//source tb
    public $SPt = null;//source port
    public $SPd = null;//source password

    public $DHIP = null;//destination host ip
    public $DUN = null;//destination user name
    public $DDN = null;//destination database name
    public $DPt = null;//destination port
    public $DTB = null;//destination tb
    public $DPd = null;//destination password

    //the user is used for migration data from source to destination
    public $userID = null;
    public $userIDColumnName = null;
    public $deleteSource = false;
    public $taskID = null;
    public $subTaskID = null;
    public $workData = null;//array

    public $migratedTables = array();

    public $TABLE_SCHEMA_INFO_DBC = null;


    /**
     * @var array used for keep oid =>nid eg..
     */
    public $keeps = array();

    /**
     * migration entry
     *
     * @param array $data
     */
    public function run(array $data) {
        if (empty($data)) {
            return;
        }
        try {
            $this->solveParameters($data);
            //check all table is ready
            $this->prepareTableStructure();
            //give loser one more chance
            $result = false;
            for ($i = 0; $i < FAILED_REPEAT_EXECUTE_TIMES; $i++) {
                $result = $this->migration();
                //if execute success,then don't need execute again
                if ($result) {
                    $this->setSubTask((string)$this->userID, $this->taskID, $this->subTaskID, SUB_TASK_STATUS_SUCCESS);
                    break;
                } else {
                    //task execute failed,this task will be re-execute again,if re-execute is still failed.then record to database
                    continue;
                }
            }
            if (!$result) {
                $this->setSubTask((string)$this->userID, $this->taskID, $this->subTaskID, SUB_TASK_STATUS_FAILED, "this task failed,reason maybe find in here" . print_r($this, true));
            }
        } catch (Exception $exception) {
            $code = SUB_TASK_STATUS_EXCEPTION;
            $message = "sorry,throw point not give us reason";

            if (!empty($exception->getCode())) {
                $code = $exception->getCode();
                //if code is SUB_TASK_STATUS_EXCEPTION then task don't need re-execute
            }
            if (!empty($exception->getMessage())) {
                $message = $exception->getMessage();
            }

            //insert a Error message to dataTransferSubTask errorMessage
            $this->setSubTask((string)$this->userID, $this->taskID, $this->subTaskID, $code, $message);
        }
    }


    /**
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
     * "taskID"=>10,
     * );
     *
     *
     * @param array|null $data
     * @throws Exception
     */
    public function solveParameters(array $data = null) {
        if (empty($data)) {
            throw new Exception("parameter error", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['sourceHostIP']) || empty($data['destinationHostIP'])) {
            throw new Exception("lost ip critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['sourceDatabaseName']) || empty($data['destinationDatabaseName'])) {
            throw new Exception("lost database name critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['sourceUser']) || empty($data['destinationUser'])) {
            throw new Exception("lost  user critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['sourcePassword']) || empty($data['destinationPassword'])) {
            throw new Exception("lost database password critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['userIDColumnName'])) {
            throw new Exception("lost userIDColumnName critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['taskID'])) {
            throw new Exception("lost taskID critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if (empty($data['workData']) || !$this->checkWorkData(json_decode($data['workData'], true))) {
            throw new Exception("lost workData critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }
        if ((empty($data['sourceTB']) && $data['sourceTB'] !== "0") || (empty($data['destinationTB']) && $data['destinationTB'] !== "0")) {
            throw new Exception("lost tb critical parameters", SUB_TASK_STATUS_EXCEPTION);
        }

        $this->SHIP = $data['sourceHostIP'];
        $this->SDN = $data['sourceDatabaseName'];
        $this->SUN = $data['sourceUser'];
        $this->SPt = isset($data['sourcePort']) ? $data['sourcePort'] : DEFAULT_MYSQL_PORT;
        $this->SPd = $data['sourcePassword'];
        $this->DHIP = $data['destinationHostIP'];
        $this->DDN = $data['destinationDatabaseName'];
        $this->DUN = $data['destinationUser'];
        $this->DPt = isset($data['destinationPort']) ? $data['destinationPort'] : DEFAULT_MYSQL_PORT;
        $this->DPd = $data['destinationPassword'];
        $this->userID = isset($data['userID']) ? $data['userID'] : null;
        $this->userIDColumnName = isset($data['userIDColumnName']) ? $data['userIDColumnName'] : "visit_id";
        //judge whether or not cross server
        $this->deleteSource = isset($data['deleteSource']) ? 1 : 0;
        $this->taskID = $data['taskID'];
        $this->workData = json_decode($data['workData'], true);
        //have check in checkWorkData method
        $this->subTaskID = $this->workData['subTaskID'];
        $this->STB = $data['sourceTB'];
        $this->DTB = $data['destinationTB'];
    }

    /**
     * check work data whether or not meet conversion
     * @param array $data
     * @return bool
     */
    private function checkWorkData(array $data) {
        if (isset($data['sequence'], $data['map'], $data['subTaskID'])) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * all table structure is ready (table structure is stored in redis),else get table structure from database,
     * then stored in redis.if some exception happen,throw a exception
     *
     * The main purpose of this design is for speeding up migration
     *
     */
    private function prepareTableStructure() {
        $GTNS = $this->getTableNameFromSequence($this->workData['sequence']);
        $CRedis = new CRedis();
        foreach ($GTNS as $GTN) {
            $key = $CRedis->getKey($GTN);
            $ary = $CRedis->getValue($key);
            if ($ary == false) {
                if (FETCH_TABLE_STRUCTURE_FROM_SOURCE) {
                    $this->getSpecificTableStructure(CDatabase::getAccurateTableName($GTN, $this->STB), $this->SDN);
                } else {
                    $this->getSpecificTableStructure(CDatabase::getAccurateTableName($GTN, $this->DTB), $this->DDN);
                }
            }
        }
    }


    /**
     *  return array with table column name as it value
     *
     * @param string $ATN
     * @param string $databaseName
     * @return array
     * @throws Exception
     */
    public function getSpecificTableStructure(string $ATN, string $databaseName) {
        $CRedis = new CRedis();
        $key = $CRedis->getKey($ATN);
        $array = $CRedis->getValue($key);
        if (empty($array)) {
            //get from database,then store in redis
            if (empty($this->TABLE_SCHEMA_INFO_DBC)) {
                $this->TABLE_SCHEMA_INFO_DBC = new CDatabase(INFO_SCHEMA_DSN, INFO_SCHEMA_USERNAME, INFO_SCHEMA_PASSWORD,array(PDO::ATTR_PERSISTENT=>1));
            }
            $result = $this->TABLE_SCHEMA_INFO_DBC->getColumnNamesInArray($ATN, $databaseName);
            if ($result !== false) {
                $array = $result;
                $CRedis->setValue($key, $array);
            } else {
                throw new Exception("fetch a table is not existed in database,reason maybe found in here :\n table = " . print_r($ATN, true) . "\n database =" . print_r($databaseName, true), SUB_TASK_STATUS_EXCEPTION);
            }
        }
        return $array;
    }

    /**
     *  do real migration joy
     *
     * if source and destination are the same server and same database and table,then talk to client that is invalid task,
     * if database and server is the same,but table is different,then only need to migrate spilt table ([0-9]).
     * if server is the same,database is different,then migrate all table,single task can use fast migrate mode.
     * others use slow migrate method if server is different,all table need be migrate,single group task can use
     * batch mode,others use slow migration mode
     *
     * @return bool
     */
    public function migration() {
        //whether or not single group task
        if (count($this->workData['sequence']) == 1) {
            //single group task
            if ($this->SHIP == $this->DHIP) {
                //can use fast mode
                return $this->fastMigrate();
            } else {
                //can use batch mode
                return $this->batchMigrate();
            }
        } else {
            //complex group task can use slow mode
            return $this->slowMigrate();
        }
    }

    /**
     * only single group task can use this method
     *
     * @return bool
     * @throws Exception
     */
    public function fastMigrate() {
        $dsn = CDatabase::getDSN($this->SHIP, $this->SPt, $this->SDN);
        $CD = new CDatabase($dsn, $this->SUN, $this->SPd,array(PDO::ATTR_PERSISTENT=>1));
        $CD->DBC->beginTransaction();
        $tableNames = $this->getTableNameFromSequence($this->workData['sequence']);
        if (empty($tableNames[0])) {
            throw new Exception("not find sequence,reason maybe found in here " . print_r($this, true), SUB_TASK_STATUS_EXCEPTION);
        }
        $GTN = $tableNames[0];
        $ATN = CDatabase::getAccurateTableName($GTN, $this->STB);
        $insertFieldArray = $this->getSpecificTableStructure($ATN, $this->SDN);
        if (empty($insertFieldArray) || !is_array($insertFieldArray)) {
            throw new Exception("can't fetch {$ATN} table structure,maybe reason can be found in here " . print_r($this, true), SUB_TASK_STATUS_EXCEPTION);
        }
        $insertField = $CD->getSQLFields($insertFieldArray);
//        $selectField = $CD->getSQLFields($insertFieldArray,$this->SDN);

        $sourceTableName = CDatabase::getAccurateTableName($GTN, $this->STB);
        $destinationTableName = CDatabase::getAccurateTableName($GTN, $this->DTB);

        if ($CD->copy($this->userID, $insertField, $insertField, $sourceTableName, $this->SDN, $destinationTableName, $this->DDN, $this->userIDColumnName)) {
            $CD->DBC->commit();
            //record be migration success table
            $this->migratedTables[] = $ATN;
            return true;
        } else {
            $CD->DBC->rollBack();
            return false;
        }
    }

    /**
     * migrate batch mode
     *
     * @return bool
     */
    public function batchMigrate() {
        $SCD = new CDatabase(Database::getDSN($this->SHIP, $this->SPt, $this->SDN), $this->SUN, $this->SPd,array(PDO::ATTR_PERSISTENT=>1));
        $DCD = new CDatabase(Database::getDSN($this->DHIP, $this->DPt, $this->DDN), $this->DUN, $this->DPd,array(PDO::ATTR_PERSISTENT=>1));

        if (empty($this->workData['sequence'][0])) {
            return true;
        }
        $GTNs = $this->getTableNameFromSequence($this->workData['sequence']);
        if (empty($GTNs[0])) {
            return true;
        }
        $GTN = $GTNs[0];
        $sourceATN = CDatabase::getAccurateTableName($GTN, $this->STB);
        $totalRow = $SCD->getTotalRowAmount($this->userID, $sourceATN, $this->SDN, $this->userIDColumnName);
        if ($totalRow === false) {
            return false;
        }
        if ($totalRow == 0) {
            return true;
        }

        $times = intval($totalRow / MIGRATION_EACH_TIME_FAST) + 1;

        try {
            $DCD->DBC->beginTransaction();
            //table name is certain
            $destinationTN = CDatabase::getAccurateTableName($GTN, $this->DTB);
            $selectFieldsArray = $this->getSpecificTableStructure($sourceATN, $this->SDN);
            $selectFields = $SCD->getSQLFields($selectFieldsArray);

            for ($i = 0; $i < $times; $i++) {
                //get data
                $sourceData = $SCD->get((string)$this->userID, $selectFields, $sourceATN, $this->SDN, $this->userIDColumnName, ($i * MIGRATION_EACH_TIME_FAST), MIGRATION_EACH_TIME_FAST);
                //insert data
                if (!empty($sourceData)) {
                    $DCD->batchInsert($destinationTN, $sourceData);
                }
            }
            $DCD->DBC->commit();
            $this->migratedTables[] = $sourceATN;
            return true;
        } catch (Exception $exception) {
            $DCD->DBC->rollBack();
            return false;
        }
    }


    /**
     * slow migrate mode
     * get data from database module,insert data module,parse rule,rollback control module,
     *
     * if throw exception is without error_code = SUB_TASK_STATUS_EXCEPTION,then this will be re-execute,else not
     *
     * @return bool
     * @throws Exception
     */
    public function slowMigrate() {
        $SCD = new CDatabase(Database::getDSN($this->SHIP, $this->SPt, $this->SDN), $this->SUN, $this->SPd,array(PDO::ATTR_PERSISTENT=>1));
        $DCD = new CDatabase(Database::getDSN($this->DHIP, $this->DPt, $this->DDN), $this->DUN, $this->DPd,array(PDO::ATTR_PERSISTENT=>1));
        try {
            $DCD->DBC->beginTransaction();
            $maps = $this->parseMap($this->workData['map']);
            $sequences = $this->parseSequence($this->workData['sequence']);
            if (empty($maps) || empty($sequences)) {
                throw new Exception("map or sequence is empty", SUB_TASK_STATUS_EXCEPTION);
            }
            $tmps = array();
            
            foreach ($sequences as $sequence) {
                $SATableName = CDatabase::getAccurateTableName($sequence->table, $this->STB);
                $DATableName = CDatabase::getAccurateTableName($sequence->table, $this->DTB);
                $SGTableName = $sequence->table;
                $totalRows = $SCD->getTotalRowAmount($this->userID, $SATableName, $this->SDN, $this->userIDColumnName);
                if ($totalRows === false) {
                    throw new Exception("get total row amount error");
                }
                if ($totalRows == 0) {
                    continue;
                }
                $times = intval($totalRows / MIGRATION_EACH_TIME_SLOW) + 1;
                $SFsArray = $this->getSpecificTableStructure($SATableName, $this->SDN);
                $SFs = $SCD->getSQLFields($SFsArray, null, false);
                $this->iterateMigrate($SCD, $DCD, $times, $SFs, $SGTableName, $SATableName, $DATableName, $maps, $sequence->saveField, MIGRATION_EACH_TIME_SLOW);
                //record migrated table
                $tmps[] = $SATableName;
            }
            $DCD->DBC->commit();
            $this->migratedTables = array_merge($this->migratedTables,$tmps);
            return true;
        } catch (Exception $exception) {
            $DCD->DBC->rollBack();
            $errorCode = $exception->getCode();
            if ($errorCode == SUB_TASK_STATUS_EXCEPTION) {
                throw $exception;
            } else {
                return false;
            }

        }
    }

    /**
     *
     * if this table have saveField value,then it has export attribute,(table have export attribute that save field will be save to array),
     * if table name is not found in maps and have save field,then this table is export type table,only need select data from source db,then insert to destination db
     * if table name is found in maps and have save field,then this table is compound type table,both need keep save fields in array and replace field where insert to destination db
     * if table name is found in maps and haven't save field,then this table is import type table,only need replace field when insert to destination db
     * if table name is not found in maps and haven't save field,then this is empty type table,nothing will happen in this table(only you define table structure error)
     *
     * @param CDatabase $SCD
     * @param CDatabase $DCD
     * @param int $times
     * @param string $SFs
     * @param string $GTN
     * @param string $SATN
     * @param string $DATN
     * @param array $maps array("zds_stock_list_[0-9]"=>array(object(replace->"prov_id",materialTable->"zds_provide_[0-9]",materialField->"id')))
     * @param string $SSF sequence save field
     * @param int $row_count
     * @throws Exception
     */
    private function iterateMigrate(CDatabase $SCD, CDatabase $DCD, int $times, string $SFs, string $GTN, string $SATN, string $DATN, array $maps, string $SSF, int $row_count) {
        for ($i = 0; $i < $times; $i++) {
            //get data
            $sourceData = $SCD->get($this->userID, $SFs, $SATN, $this->SDN, $this->userIDColumnName, ($i * $row_count), $row_count);
            if($sourceData === false){
                throw new Exception("execute sql fail to fetch source data error");
            }
            if(empty($sourceData)){
                //fetch other's data from source database
                continue;
            }
            if (isset($maps[$GTN])) {
                //have fields that need be replace before insert to database
                if (!empty($SSF)) {
                    //id need to store in keeps

                    //do replace job
                    $sourceData = $this->replaceIDs($maps[$GTN], $sourceData, $this->keeps);
                    //do insert job
                    foreach ($sourceData as $value) {
                        $OID = array_key_exists("id", $value) ? $value['id'] : false;
                        //unset id field
                        if (array_key_exists("id", $value)) {
                            unset($value['id']);
                        }
                        //do insert on row
                        $NID = $DCD->lastInsertId($DATN, $value);
                        if ($NID === false) {
                            throw new Exception("insert error");
                        }

                        //do keep id job
                        if ($OID !== false) {
                            $this->keepID($OID, $NID, $GTN);
                        }
                    }
                } else {
                    //id don't need to store in keeps
                    //do replace job
                    $sourceData = $this->replaceIDs($maps[$GTN], $sourceData, $this->keeps);
                    //do insert job
                    foreach ($sourceData as &$value) {
//                        $OID = array_key_exists("id",$value) ? $value['id'] : false;
                        //unset id field
                        if (array_key_exists("id", $value)) {
                            unset($value['id']);
                        }
                    }
                    //do insert batch
                    $DCD->batchInsert($DATN, $sourceData);
                }
            } else {
                //don't need replace

                if (!empty($SSF)) {
                    //id need to store in keeps
                    foreach ($sourceData as $value) {
                        $OID = array_key_exists("id", $value) ? $value['id'] : false;
                        //unset id field
                        if (array_key_exists("id", $value)) {
                            unset($value['id']);
                        }
                        //do insert on row
                        $NID = $DCD->lastInsertId($DATN, $value);
                        if ($NID === false) {
                            throw new Exception("insert error");
                        }

                        //do keep id job
                        if ($OID !== false) {
                            $this->keepID($OID, $NID, $GTN);
                        }
                    }
                } else {
                    //id don't need to store in keeps
                    //maybe error format for define table's structure
                    throw new Exception("please re-edit", SUB_TASK_STATUS_EXCEPTION);
                }
            }
        }
    }

    /**
     * replace all need be replaced fields ,then return a replaced data array
     *
     *
     * @param array $replaceRules array(object(replace->"prov_id",materialTable->"zds_provide_[0-9]",materialField->"id'))
     * @param array $originalData array(array("key0"=>"value0","key1"=>"value1"),array("key0"=>"value0","key1"=>"value1"))
     * @param array $keeps $keep = array("zds_provider" => array("O_12"=>1,"O13"=>2);"zds_stock_list_[0-9]"=>array("O_12"=>5));
     * @return array
     */
    private function replaceIDs(array $replaceRules, array $originalData, array $keeps) {
        foreach ($replaceRules as $rule) {
            foreach ($originalData as &$original) {
                if (array_key_exists($rule->replace, $original) && array_key_exists($rule->materialTable, $keeps)) {
                    //assign new value
                    $original[$rule->replace] = isset($keeps[$rule->materialTable]["O_{$original[$rule->replace]}"]) ? $keeps[$rule->materialTable]["O_{$original[$rule->replace]}"] : "";
                }
            }
        }
        return $originalData;
    }


    /**
     *
     * array("zds_provider" => array("O_12"=>1,"O13"=>2);"zds_stock_list_[0-9]"=>array("O_12"=>5));
     *
     * @param string $OID
     * @param string $NID
     * @param string $GTN
     * @param string $prefix
     */
    private function keepID(string $OID, string $NID, string $GTN, string $prefix = "O_") {
        $this->keeps[$GTN]["{$prefix}{$OID}"] = $NID;
    }


    /**
     * get rules like below
     *
     * $rules = array(
     * "zds_stock_list_[0-9]"=>array(object(replace->"prov_id",materialTable->"zds_provide_[0-9]",materialField->"id'))
     * )
     *
     * @param $map
     * @return array|bool
     */
    private function parseMap($map) {
        $rules = array();
        if (!empty($map)) {
            foreach ($map as $rule) {
                $tmp = $this->parseStr($rule, "=", 2);
                $tmpExport = $this->parseStr($tmp[0], ":", 2);
                $tmpImport = $this->parseStr($tmp[1], ":", 2);
                $obj = new stdClass();
                $obj->replace = $tmpImport[1];
                $obj->materialTable = $tmpExport[0];
                $obj->materialField = $tmpExport[1];
                $rules["{$tmpImport[0]}"][] = $obj;
            }
        }
        return $rules;
    }

    /**
     * return a array like array(obj1(table->"zds_logistics_[0-9]",saveField->"id"))
     * @param array $sequence
     * @return array
     * @throws Exception
     */
    private function parseSequence(array $sequence) {
        $rules = array();
        if (!empty($sequence)) {
            foreach ($sequence as $rule) {
                $tmp = $this->parseStr($rule, ":");
                if (!isset($tmp[0])) {
                    throw new Exception("sequence format error", SUB_TASK_STATUS_EXCEPTION);
                }
                $obj = new stdClass();
                $obj->table = $tmp[0];
                $obj->saveField = isset($tmp[1]) ? $tmp[1] : "";
                $rules[] = $obj;
            }
        }
        return $rules;
    }

    /**
     * parse string
     *
     * @param $str
     * @param $delimiter
     * @param $amount
     * @return array
     * @throws Exception
     */
    private function parseStr($str, $delimiter, $amount = null) {
        $tmp = explode($delimiter, $str);

        if (!empty($amount) && $amount === 0) {
            if (count($tmp) != $amount) {
                throw new Exception("map parameter format error");
            }
        }
        return $tmp;
    }


    /**
     * set a row to dataTransferSubTask to reflect this task status
     *
     * @param string $UID
     * @param string $taskID
     * @param string $subTaskID
     * @param string $status
     * @param string|null $errorMessage
     * @return bool|\PDOStatement
     */
    public function setSubTask($UID, $taskID, $subTaskID, $status, $errorMessage = null) {

        $CD = new CDatabase(TASK_DSN, TASK_USERNAME, TASK_PASSWORD,array(PDO::ATTR_PERSISTENT=>1));
        //check whether or not already exist the same task and sub task
        $database = TASK_TABLE_NAME_SUB;
        $sql = "select * from `{$database}` where `UID` = '{$UID}' and `taskID` = '{$taskID}' and `subTaskID` = '{$subTaskID}';";
        $record = $CD->getSingleRecord($sql);
        $now = $this->getDate();

        $migratedTableStr = "";
        //remove duplicate tables in migrated tables
        $this->migratedTables = array_unique($this->migratedTables,SORT_STRING);
        if(!empty($this->migratedTables)){
            $migratedTableStr = json_encode($this->migratedTables);
        }

        if ($record === false) {
            //not existed,create one

            $data = array(
                "UID" => $UID,
                "taskID" => $taskID,
                "subTaskID" => $subTaskID,
                "status" => $status,
                "errorMessage" => $errorMessage,
                "times" => 1,
                "delete" => $this->deleteSource,
                "tables" =>$migratedTableStr,
                "created_at" => $now,
                "updated_at" => $now
            );

            return $CD->insert(TASK_TABLE_NAME_SUB, $data);

        } else {
            $times = $record['times'];
            $data = array(
                "status" => $status,
                "errorMessage" => $errorMessage,
                "times" => $times + 1,
                "delete" => $this->deleteSource,
                "tables" => $migratedTableStr,
                "updated_at" => $now
            );
            $where = array(
                "`UID`='{$UID}'",
                "and `taskID`='{$taskID}'",
                "and `subTaskID`='{$subTaskID}'"
            );
            return $CD->update(TASK_TABLE_NAME_SUB, $data, $where);
        }
    }


}









