<?php
namespace migration\Consumer;

use migration\MessageQueue;
use migration\Log;

/**
 * Class Consumer
 *
 * receive message from redis queue
 *
 * @package migration
 * @author williamchu--<154893323@qq.com>
 * @since 12/12/2016 1:15:46
 */
class Consumer extends MessageQueue {


    /**
     * Consumer constructor.
     */
    public function __construct() {
        parent::__construct();
    }

    /**
     * get message form queue,if queue is empty,then block here until have message coming
     *
     *
     * @return bool|mixed
     */
    public function blockedReceive() {

//        $message = $this->client->rPop(REDIS_MESSAGE_QUEUE_NAME);//return message
        $message = $this->client->brPop(REDIS_MESSAGE_QUEUE_NAME, 10);//return array

        if (is_array($message)) {
            $message = isset($message[1]) ? $message[1] : "";
        }
        if (DEBUG && $message !== false) {
            Log::logMessage("message = " . print_r($message, true));
        }

        if ($message !== false) {
            //have to use array
            $value = json_decode($message, true);
            return $value;
        } else {
            return false;
        }
    }
}
