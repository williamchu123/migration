<?php
/**
 * Created by PhpStorm williamchu
 *
 * Date: 2/21/2017
 * Time: 2:33 PM
 */
namespace Migration\Test;

use migration\Producer\Producer;
use PHPUnit\Framework\TestCase;

class ProducerTest extends TestCase {
    private $producer = null;

    public function setUp() {
        $this->producer = new Producer();
    }


    /**
     * @test
     * @dataProvider dataProvider
     */
    public function checkParameterFormat($data) {
        $result = $this->producer->checkParameterFormat($data);
        $this->assertTrue($result);
    }


    /**
     *
     */
    public function dataProvider() {
        $data = array();

        $source = array(
            '{"source":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix","destination":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix","subTaskIDS":"*","userID":"61","taskID":"1","deleteOrigin":false}',
            '{"source":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix","destination":"root@127.0.0.1:3306;password=yourPassword;dbname=MyDatabaseName;tb=yourTableSuffix","subTaskIDS":["yourTask_0","yourTask_1"],"userID":"61","taskID":"1","deleteOrigin":false}'
        );

        foreach ($source as $value) {
            $data[] = array(json_decode($value));
        }


        return $data;

    }


}