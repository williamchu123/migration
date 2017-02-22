<?php
namespace migration;
use \PDO;
/**
 * compare before migration data and after migration data,if each table have the same rows,then this table is same.
 * same table we don't wanna to compare,put it in blacklist.
 *
 *
 *
 *
 * @author williamchu--<154893323@qq.com>
 * @since 2016-12-28 9:57:12
 *
 */

require __DIR__ . DIRECTORY_SEPARATOR . "../application/Autoload.php";
require __DIR__. DIRECTORY_SEPARATOR ."Blacklist.php";
require __DIR__. DIRECTORY_SEPARATOR ."Config.php";
require __DIR__. DIRECTORY_SEPARATOR ."TDatabase.php";



$userID = "68843276";


$TDatabaseA = new TDatabase($dsnA,$userNameA,$passwordA);
$TDatabaseB = new TDatabase($dsnB,$userNameB,$passwordB);

$allTables = $TDatabaseA->getAllTablesFromDB($compareDBA);
if(empty($allTables)){
    exit("fetch comparing tables failed");
}
$tmpTables = array();
foreach ($allTables as $allTable){
    $tmpTables[] = $allTable['table'];
}

$allTables = $tmpTables;
$targetTables = array_diff($allTables,BLACKLIST);

$temp = array();
foreach ($targetTables as $targetTable){
    if(preg_match('/\w*_[0-9]{1}/',$targetTable)){
        $temp[] = substr($targetTable,0,-1) . "1";
    }else{
        $temp[] = $targetTable;
    }
}
$targetTables = array_unique($temp);

foreach ($targetTables as $targetTable){

    if(preg_match('/\w*_[0-9]{1}/',$targetTable)){
        $tmp = substr($targetTable,0,-1);
        $amountA = getRows($TDatabaseA->DBC,(string)$userID,(string)$tmp . $compareTBA,(string)$compareDBA);
        $amountB = getRows($TDatabaseB->DBC,(string)$userID,(string)$tmp . $compareTBB,(string)$compareDBB);
    }else{
        $amountA = getRows($TDatabaseA->DBC,(string)$userID,(string)$targetTable,(string)$compareDBA);
        $amountB = getRows($TDatabaseB->DBC,(string)$userID,(string)$targetTable,(string)$compareDBB);
    }
    if($amountA == $amountB){
        if(preg_match('/\w*_[0-9]{1}/',$targetTable)) {
            $tmp = substr($targetTable, 0, -1);
            echo "[success] A [{$amountA}] {$compareDBA} {$tmp}{$compareTBA} == B [{$amountB}] {$compareDBB} {$tmp}{$compareTBB}\r\n";
        }else{
            echo "[success] A [{$amountA}]  {$compareDBA} {$targetTable} == B [{$amountB}]  {$compareDBB} {$targetTable}\r\n";
        }
    }else{
        if(preg_match('/\w*_[0-9]{1}/',$targetTable)) {
            $tmp = substr($targetTable, 0, -1);
            echo "[failed] A  [{$amountA}] {$compareDBA} {$tmp}{$compareTBA} != B [{$amountB}] {$compareDBB} {$tmp}{$compareTBB}\r\n";
        }else{
            echo "[failed] A [{$amountA}]  {$compareDBA} {$targetTable} is != B [{$amountB}] {$compareDBB} {$targetTable}\r\n";
        }
    }
}



/**
 * @param object $DBC
 * @param string $UID
 * @param string $TN
 * @param string $DB
 * @return bool
 */
function getRows($DBC,string $UID,string $TN,string $DB){
    $sql = "select count(`id`) as `amount` from `{$DB}`.`{$TN}` where `visit_id`='{$UID}';";
    $query = $DBC->query($sql);
    if($query === false){
        return false;
    }
    $result = $query->fetch(PDO::FETCH_ASSOC);
    if($result === false){
        return false;
    }

    return $result["amount"];
}






















