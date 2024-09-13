<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:40
 */

namespace MaxSky\AMQP;

use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Queue\SendMessage;

class Message {

    private static $instance;

    private $config;
    private $connection;

    /**
     * @param AMQPConfig $config
     *
     * @throws AMQPConnectionException
     */
    public function __construct(AMQPConfig $config) {
        $this->config = $config;

        $this->connection = (new AMQPBaseConnection($config))->getConnection();
    }

    /**
     * @param AMQPConfig $config
     *
     * @return Message
     * @throws AMQPConnectionException
     */
    public static function connect(AMQPConfig $config): Message {
        if (!self::$instance) {
            self::$instance = new self($config);
        }

        return self::$instance;
    }

    /**
     * @return bool
     * @throws AMQPQueueException
     */
    public function send() {
        $class = new SendMessage();

        if (extension_loaded('amqp')) {

        }

        return $class->send($this->connection);
    }
}
