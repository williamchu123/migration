<?php
namespace migration;


use migration\Redis;
/**
 * Created by PhpStorm williamchu
 *
 * Date: 1/13/2017
 * Time: 4:16 PM
 */
class TRedis extends Redis {

    public function __construct() {
        parent::__construct();
    }


}
