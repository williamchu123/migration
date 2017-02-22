<?php
namespace migration\Producer;
use migration\Database;
use migration\Log;


/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/20/2016
 * Time: 4:58 PM
 */
class PDatabase extends Database {


    public function __construct($dsn, $username, $password) {
        parent::__construct($dsn, $username, $password);
    }


    /**
     * for quickly,use write it statically
     * @param $userID
     * @return bool
     */
    public function getUserTb($userID){

        $database = "MyDatabaseName";
        $table = "zds_user";

        $sql = "select tb from `{$database}`.`{$table}` where `visit_id`='{$userID}';";
        $query = $this->DBC->query($sql);
        if($query === false){
            return false;
        }
        $result = $query->fetch(\PDO::FETCH_ASSOC);

        if(!isset($result['tb'])){
            return false;
        }

        if(DEBUG){
            Log::logMessage("result= " . print_r($result,true));
        }
        return $result['tb'];
    }
















}