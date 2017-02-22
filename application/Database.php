<?php
namespace migration;
use \PDO;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/12/2016
 * Time: 11:19 AM
 */
class Database {
    public $DBC = null;


    public function __construct($dsn,$username,$password,$option=array()) {
        $this->DBC = new \PDO($dsn,$username,$password,$option);
    }

    public static function getDSN($hostIP,$port,$databaseName){
        //for example mysql:host=127.0.0.1:3306;dbname=MyDatabaseName;charset=utf8
        return "mysql:host=" . $hostIP . ":" . $port . ";dbname=" . $databaseName . ";charset=utf8";
    }


    /**
     * use $sql get a record from database
     *
     * @param $sql
     * @return bool|mixed
     */
    public function getSingleRecord($sql) {
        $query = $this->DBC->query($sql);
        if ($query === false) {
            return false;
        }
        $result = $query->fetch(PDO::FETCH_ASSOC);
        if (empty($result)) {
            return false;
        } else {
            return $result;
        }
    }

    /**
     * use $sql get all record from database
     *
     * @param $sql
     * @return array|bool
     */
    public function getAllRecord($sql) {
        $query = $this->DBC->query($sql);
        if ($query === false) {
            return false;
        }
        $results = $query->fetchAll(PDO::FETCH_ASSOC);
        if (empty($results)) {
            return false;
        } else {
            return $results;
        }
    }


    /**
     * do update
     *
     * @param $table
     * @param $data
     * @param $where
     * @param array $orderBy
     * @param bool $limit
     * @return bool|\PDOStatement
     */
    public function update($table, $data, $where, $orderBy = array(), $limit = false) {
        if (empty($data)) {
            return false;
        }
        $set = $this->_set($data, true);
        $sql = $this->_update($table, $set, $where, $orderBy, $limit);
        return $this->DBC->query($sql);
    }


    /**
     * Update statement
     *
     * Generates a platform-specific update string from the supplied data
     *
     * @param $table
     * @param $values
     * @param $where
     * @param array $orderby
     * @param bool $limit
     * @return string
     */
    public function _update($table, $values, $where, $orderby = array(), $limit = FALSE) {
        foreach ($values as $key => $val) {
            $valstr[] = $key . " = " . $val;
        }

        $limit = (!$limit) ? '' : ' LIMIT ' . $limit;

        $orderby = (count($orderby) >= 1) ? ' ORDER BY ' . implode(", ", $orderby) : '';

        $sql = "UPDATE " . $table . " SET " . implode(', ', $valstr);

        $sql .= ($where != '' AND count($where) >= 1) ? " WHERE " . implode(" ", $where) : '';

        $sql .= $orderby . $limit;

        return $sql;
    }
    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param    object
     * @return    array
     */
    public function _object_to_array($object) {
        if (!is_object($object)) {
            return $object;
        }

        $array = array();
        foreach (get_object_vars($object) as $key => $val) {
            // There are some built in keys we need to ignore for this conversion
            if (!is_object($val) && !is_array($val) && $key != '_parent_name') {
                $array[$key] = $val;
            }
        }

        return $array;
    }

    public function escape($str) {
        if (is_string($str)) {
            $str = "'" . $str . "'";
        } elseif (is_bool($str)) {
            $str = ($str === FALSE) ? 0 : 1;
        } elseif (is_null($str)) {
            $str = 'NULL';
        }

        return $str;
    }

    public function _set($data, $escape = TRUE) {
        $result = array();
        $data = $this->_object_to_array($data);
        foreach ($data as $k => $v) {
            if ($escape === FALSE) {
                $result[$k] = $v;
            } else {
                $result["`{$k}`"] = $this->escape($v);
            }
        }
        return $result;
    }


}

