<?php
namespace migration\Consumer;

use migration\Database;
use migration\Log;
use \PDO;
use \Exception;

/**
 * Class CDatabase
 *
 * @package migration\Consumer
 * @author williamchu--<154893323@qq.com>
 * @since 2016-12-23 15:00:55
 */
class CDatabase extends Database {

    /**
     * CDatabase constructor.
     * @param $dsn
     * @param $username
     * @param $password
     * @param array $option
     */
    public function __construct($dsn, $username, $password,$option=array()) {
        parent::__construct($dsn, $username, $password,$option);
    }




    /**
     * return a array with all table column name
     * eg array("id","tid","visit_id","print_type","log_from","trade_from")
     *
     * @param string $ATN
     * @param string $databaseName
     * @return array|bool
     */
    public function getColumnNamesInArray(string $ATN, string $databaseName) {
        $array = array();
        $sql = "select COLUMN_NAME as `name` from information_schema.columns where TABLE_SCHEMA='" . $databaseName . "' and  table_name='" . $ATN . "';";
        $query = $this->DBC->query($sql);
        if ($query === false) {
            return false;
        }
        $results = $query->fetchAll(PDO::FETCH_ASSOC);

        if (!empty($results)) {
            foreach ($results as $value) {
                if (isset($value['name'])) {
                    $array[] = $value['name'];
                }
            }
            if (DEBUG) {
                Log::logMessage("result = " . print_r($array, true));
            }
            return $array;
        } else {
            return false;
        }
    }


    /**
     * return true if success copy data from one database or table to another database or table else false
     * @param string $userID
     * @param string $insertField
     * @param string $selectField
     * @param string $sourceTableName
     * @param string $sourceDatabase
     * @param string $destinationTableName
     * @param string $destinationDatabase
     * @param string $columnName
     * @return bool|\PDOStatement
     */
    public function copy(string $userID, string $insertField, string $selectField, string $sourceTableName, string $sourceDatabase, string $destinationTableName, string $destinationDatabase, string $columnName) {
        if (!$insertField) {
            return false;
        }
        $sql = "insert IGNORE into `{$destinationDatabase}`.`{$destinationTableName}` ({$insertField}) select {$selectField} from `{$sourceDatabase}`.`{$sourceTableName}` where `{$columnName}`='{$userID}'";
        return $this->DBC->query($sql);
    }


    /**
     * get data from database
     *
     * @param string $userID
     * @param string $SFs
     * @param string $ATN
     * @param string $DN
     * @param string $CN
     * @param int $offset
     * @param int $row_count
     * @return array|bool
     */
    public function get(string $userID, string $SFs, string $ATN, string $DN, string $CN, int $offset, int $row_count) {
        $sql = "select {$SFs} from `{$DN}`.`{$ATN}` where `{$CN}` = '{$userID}' order by `id` asc limit {$offset},{$row_count}";
        $query = $this->DBC->query($sql);
        if ($query === false) {
            return false;
        }
        $results = $query->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }



    /**
     * insert batch mode
     *
     * @param string $table
     * @param array $sets array(0=>array(),1=>array(),3=>array())
     * @param bool $escape
     * @return bool
     * @throws Exception
     */
    public function batchInsert(string $table, array $sets, bool $escape = true) {
        if (empty($sets)) {
            return false;
        }
        $insertSets = array();
        $insertKeys = array();
        $keys = array_keys(current($sets));
        sort($keys);

        foreach ($sets as $row) {
            if (count(array_diff($keys, array_keys($row))) > 0 OR count(array_diff(array_keys($row), $keys)) > 0) {
                // batch function above returns an error on an empty array
                throw new Exception("key are diff");
            }

            ksort($row); // puts $row in the same order as our keys

            if ($escape === FALSE) {
                $insertSets[] = '(' . implode(',', $row) . ')';
            } else {
                $clean = array();

                foreach ($row as $value) {
                    $clean[] = $this->escape($value);
                }

                $insertSets[] = '(' . implode(',', $clean) . ')';
            }
        }

        foreach ($keys as $k) {
            $insertKeys[] = "`$k`";
        }

        try {
            for ($i = 0, $total = count($insertSets); $i < $total; $i = $i + 100) {

                $sql = $this->_insert_batch("`$table`", $insertKeys, array_slice($insertSets, $i, 100));
                //logMessage(print_r($sql,true));
                //echo $sql;
                $this->DBC->query($sql);

            }
        } catch (Exception $e) {
            throw new Exception($e->getMessage());
        }
    }

    /**
     * @param string $ATN
     * @param $data
     * @return bool|\PDOStatement
     */
    public function insert(string $ATN, $data) {
        if (empty($data)) {
            return false;
        }
        $set = $this->_set($data, true);
        $sql = $this->_insert($ATN, array_keys($set), array_values($set));
        //Log::logMessage(print_r($sql,true));
        return $this->DBC->query($sql);
    }

    /**
     * @param $ATN
     * @param $data
     * @return bool|string
     */
    /**
     * @param string $ATN
     * @param array $data
     * @return bool|string
     */
    public function lastInsertId(string $ATN, array $data) {
        $result = $this->insert($ATN, $data);
        if ($result !== false) {
            return $this->DBC->lastInsertId();
        } else {
            return false;
        }
    }


    /**
     * Insert statement
     *
     * Generates a platform-specific insert string from the supplied data
     *
     * @access    public
     * @param    string    the table name
     * @param    array    the insert keys
     * @param    array    the insert values
     * @return    string
     */
    private function _insert($table, $keys, $values) {
        return "INSERT IGNORE INTO  `{$table}` (" . implode(', ', $keys) . ") VALUES (" . implode(', ', $values) . ")";
    }


    /**
     * Insert_batch statement
     *
     * Generates a platform-specific insert string from the supplied data
     *
     * @access  public
     * @param   string  the table name
     * @param   array   the insert keys
     * @param   array   the insert values
     * @return  string
     */
    function _insert_batch($table, $keys, $values) {
        return "INSERT IGNORE INTO  " . $table . " (" . implode(', ', $keys) . ") VALUES " . implode(', ', $values);
    }

    /**
     * return a string separated by ``,
     *
     * @param array|object $array
     * @param string|null $prefix
     * @param bool $removeID
     * @return bool|string
     */
    public function getSQLFields($array, string $prefix = null, $removeID = true) {
        if (empty($array)) {
            return false;
        }
        $result = "";
        foreach ($array as $property) {
            if ($removeID) {
                if (strtolower($property) == 'id') {
                    continue;
                }
            }

            if (empty($prefix)) {
                $result .= "`{$property}`,";
            } else {
                $result .= "`{$prefix}`.`{$property}`,";
            }
        }
        return substr($result, 0, -1);
    }

    /**
     * return a table name with right tb filed (zds_order_3)
     *
     * @param string $generalTableForm
     * @param string $tb
     * @return string
     */
    public static function getAccurateTableName(string $generalTableForm, string $tb) {
        //remove id from xxxx_table:id
        if (strpos($generalTableForm, ":") !== false) {
            $tmp = explode(":", $generalTableForm);
            $specifyTableName = $tmp[0];
        } else {
            $specifyTableName = $generalTableForm;
        }
        if (preg_match('/\w*(_\[0-9\]){1}$/', $specifyTableName)) {
            $specifyTableName = substr($specifyTableName, 0, -5) . $tb;
        }
        return $specifyTableName;
    }


    /**
     * figure out how many row that will be migrate to other table
     *
     * @param string $userID
     * @param string $ATN must be exactly table name
     * @param string $database
     * @param string $columnName
     * @return bool|int
     */
    public function getTotalRowAmount(string $userID, string $ATN, string $database, string $columnName) {

        //$sourceTableName = $this->getRightTableName($data->workData['sequence'][0], $data->sourceTB);
        $sql = "select count(`id`) as `total` from `{$database}`.`{$ATN}` where `{$columnName}`='{$userID}'";
        $query = $this->DBC->query($sql);
        if ($query === false) {
            return false;
        }
        $result = $query->fetch(PDO::FETCH_ASSOC);

        if (isset($result['total']) || $result['total'] == 0) {
            return $result['total'];
        } else {
            return false;
        }
    }


}