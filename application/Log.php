<?php
namespace migration;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/14/2016
 * Time: 1:55 PM
 */
class Log {
    private $fileHandler = null;
    private $filePath = __DIR__ . DIRECTORY_SEPARATOR . "Log" . DIRECTORY_SEPARATOR;
    private $fileName = "work.log";

    public function __construct() {
        $dir = substr($this->filePath, 0, -1);

        if (!file_exists($dir)) {
            mkdir($dir, '0777', true);
        }

        $handler = fopen($this->filePath . $this->fileName,"a");
        if($handler === false){
            throw new \Exception("open log file failed!,please checkout whether or not you have write privilege");
        }
        $this->fileHandler = $handler;
    }

    public function write($message) {
        return fwrite($this->fileHandler,$message);
    }

    public function __destruct() {
        fclose($this->fileHandler);
    }


    public static function logMessage($message){
        $log = new self();
        return $log->write($message);
    }


}
