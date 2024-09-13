<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 19:01
 */

namespace MaxSky\AMQP\Queue;

abstract class AbstractSendMessage {

    protected $connection;

    public function __construct($connection) {
        $this->connection = $connection;
    }
}
