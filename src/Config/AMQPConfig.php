<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 16:34
 */

namespace MaxSky\AMQP\Config;

class AMQPConfig {

    public $connection_name = 'default';

    public $queue_ttl = 5000;
    // public $message_ttl = 0;

    public $host = '127.0.0.1';
    public $port = 5672;
    public $user = 'guest';
    public $password = 'guest';
    public $vhost = '/';
    public $locale = 'zh_CN';

    public $connect_options = [];
}
