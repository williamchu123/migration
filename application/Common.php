<?php

/**
 * @param string $code
 * @param string $message
 * @param string $data
 * @return string
 */
function stdResponse($code='',$message='',$data=''){
    $result = array(
        "code"=>$code,
        "message"=>$message,
        "data"=>$data
    );
    return json_encode($result);
}



