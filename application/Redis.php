<?php
namespace migration;

/**
 * Created by PhpStorm williamchu
 *
 * Date: 12/19/2016
 * Time: 6:27 PM
 */
class Redis {
    public $client = null;

    /**
     * Redis constructor.
     * @throws \Exception
     */
    public function __construct() {
        $client = new \Redis();
        if ($client->pconnect(REDIS_SERVER_IP, REDIS_SERVER_PORT)) {
            $this->client = $client;
        } else {
            throw new \Exception("connect redis server failed!");
        }
    }

    /**
     * @param $key
     * @return bool
     */
    public function exists($key) {
        return $this->client->exists($key);
    }

    /**
     * a json decode object or array by the key
     *
     * @param $key
     * @return bool|mixed
     */
    public function getValue($key){
        $value = $this->client->get($key);
        if($value !== false){
            return json_decode($value);
        }else{
            return false;
        }
    }

    /**
     * store a value in redis with $expire seconds to live
     *
     * @param string $key
     * @param mixed $value
     * @param int $expire unit = second
     * @return bool
     */
    public function setValue($key,$value,$expire=REDIS_EXPIRE_TIME){
        $encodeValue = json_encode($value);
        return $this->client->setEx($key,$expire,$encodeValue);
    }






}