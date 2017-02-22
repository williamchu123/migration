<?php
/**
 * for simplify programing ,this relative table are compiled by manual
 *
 * @author williamchu--<154893323@qq.com>
 * @since 2016/12/27 16:15:15
 *
 * note:巭孬嫑乱动
 */

$normalTasks = array(
    "yourTask_1" => array(
        "sequence" => array("zds_self_cate:id", "zds_self_bill_order_[0-9]:id", "zds_self_bill_[0-9]"),
        "map" => array("zds_self_cate:id=zds_self_bill_[0-9]:cate", "zds_self_bill_order_[0-9]:id=zds_self_bill_[0-9]:order_id")
    )

);
/*
 * if cross server only in production clusters db,it will be add when cross server out production clusters
 */
$crossServerInProductionClustersTasks = array(
    "yourTask_CSIPCT_1" => array(
        "sequence" => array("auto_comment"),
        "map" => array()
    ),
    "yourTask_CSIPCT_2" => array(
        "sequence" => array("sync_lock"),
        "map" => array()
    )
);

/*
 * if cross server out of production clusters,like import data into 182.2 server from production servers
 *
 */
$crossServerOutProductionClusterTasks = array(
    "yourTask_CSOPCT_1" => array(
        "sequence" => array("zds_tip"),
        "map" => array()
    ),
    "yourTask_CSOPCT_2" => array(
        "sequence" => array("user_login_info"),
        "map" => array()
    )
);


/*
 *when server and database is then same.but table is not the same.then use below
 *
 *
 */
$crossOnlyTableTasks = array(
    "yourTask_tb_1" => array(
        "sequence" => array("zds_item_[0-9]"),
        "map" => array()
    ),
    "yourTask_tb_2" => array(
        "sequence" => array("zds_sku_[0-9]"),
        "map" => array()
    )
);



