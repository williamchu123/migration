<?php
/**
 * worker enter script
 *
 * @author williamchu--<154893323@qq.com>
 * @since 2016/12/19  16:42:53
 *
 */
namespace migration\Consumer;
use \migration\Log;
use \Exception;

require_once __DIR__  . "/../Config.php";
require_once __DIR__ . "/../Autoload.php";
require_once __DIR__ . "/../Log.php";
$consumer = new Consumer();
//run without time limit
set_time_limit(0);
while (true) {
    try{
        $data = $consumer->blockedReceive();
        //$data maybe false or valid data
        if(empty($data)){
            continue;
        }
        $worker = new Worker();
        $worker->run($data);
    }catch (Exception $exception){
        Log::logMessage("[EXCEPTION] maybe reason is " . $exception->getMessage());
        continue;
    }
}