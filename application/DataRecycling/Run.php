<?php
namespace migration\DataRecycling;
/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/16/2017
 * Time: 7:01 PM
 */

$applicationPath = __DIR__ . DIRECTORY_SEPARATOR . ".." . DIRECTORY_SEPARATOR;
require_once $applicationPath . "DataRecycling" . DIRECTORY_SEPARATOR . "DRAutomator.php";
require_once $applicationPath . "TaskConfig.php";
require_once $applicationPath . "Database.php";
require_once $applicationPath . "Config.php";


$automator = new DRAutomator();
$automator->clean();








