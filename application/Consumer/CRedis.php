<?php
namespace migration\Consumer;

use migration\Redis;

class CRedis extends Redis {



    /**
     * CRedis constructor.
     */
    public function __construct() {
        parent::__construct();
    }

    /**
     * if table is like this zds_order_0 ,then will be converted to zds_order_[0-9]
     *
     * @param string $TN
     * @return string
     */
    public function getKey(string $TN){
        //xxx_1 | xxx_0 will be converted to xxx_[0-9] xxx_[0-9]
        if(preg_match('/_[0-9]{1}$/',$TN)){
            $TN = substr($TN,0,-1) . "[0-9]";
        }
        return "prefix_" . $TN . "_suffix";
    }












}