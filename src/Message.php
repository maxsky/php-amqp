<?php

/**
 * Created by IntelliJ IDEA.
 * User: maxsky
 * Date: 2024 Sep 13
 * Time: 17:40
 */

namespace MaxSky\AMQP;

use AMQPConnection;
use AMQPException;
use MaxSky\AMQP\Config\AMQPBaseConnection;
use MaxSky\AMQP\Config\AMQPConfig;
use MaxSky\AMQP\Exception\AMQPConnectionException;
use MaxSky\AMQP\Exception\AMQPQueueException;
use MaxSky\AMQP\Queue\SendMessage;
use MaxSky\AMQP\Queue\SendMessageByExtension;
use PhpAmqpLib\Connection\AbstractConnection;

class Message {

    private static $instance;

    private $config;

    /** @var AMQPConnection|AbstractConnection */
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
     * @param string      $handler
     * @param mixed       $data
     * @param string|null $queue_name
     * @param null        $delay
     * @param bool        $transaction
     *
     * @return void
     * @throws AMQPQueueException
     */
    public function send(string  $handler, $data,
                         ?string $queue_name = 'default', $delay = null, bool $transaction = false) {
        if (extension_loaded('amqp')) {
            $sendService = new SendMessageByExtension($this->connection, $this->config->connection_name);
        } else {
            $sendService = new SendMessage($this->connection, $this->config->connection_name);
        }

        try {
            $sendService->send($handler, $data, $queue_name, $delay, $transaction);
        } catch (AMQPException $e) {
            throw new AMQPQueueException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }
    }
}
