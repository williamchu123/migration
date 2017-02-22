<?php
namespace migration;
/**
 * Class MessageQueue
 * @package migration
 * @author williamchu--<154893323@qq.com>
 * @since 12/12/2016 1:53:02
 *
 */
class MessageQueue extends Redis {



    /**
     * MessageQueue constructor.
     */
    public function __construct() {
       parent::__construct();
    }

}
