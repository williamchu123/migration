<?php
namespace migration\Consumer;

/**
 * Class BaseWorker
 *
 * @package migration\Consumer
 * @author williamchu--<154893323@qq.com>
 * @since 2016-12-23 15:16:19
 */
class BaseWorker {


    /**
     * get Table Name From Sequence array
     *
     * @param array $sequence array("xxx:id", "xxyyxx_[0-9]:id","yy_[0-9]")
     * @return array array("xxx", "xxyyxx_[0-9]","yy_[0-9]")
     */
    public function getTableNameFromSequence(array $sequence) {
        $originTableName = array();
        foreach ($sequence as $tableName) {
            $resultArray = explode(":", $tableName);
            if (!empty($resultArray[0])) {
                $originTableName[] = $resultArray[0];
            }
        }
        return $originTableName;
    }


    /**
     * get date like this 2016-04-05 15:12:59
     * @return false|string
     */
    public static function getDate() {
        return date("Y-m-d H:i:s");
    }


}